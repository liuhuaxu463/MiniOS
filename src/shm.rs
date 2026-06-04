use crate::error::{MiniOsError, Result};
use log::{debug, info, warn};

// ============================================================================
// 常量
// ============================================================================

/// 用于共享内存头部标识的魔数（小端序的 "MOSH"）
const SHM_MAGIC: u32 = 0x4D4F5348; // 小端序的 "MOSH"
/// 当前共享内存布局版本号
const SHM_VERSION: u32 = 1;
/// 共享内存头部大小（字节）
const SHM_HEADER_SIZE: u64 = 64;

// ============================================================================
// 共享内存头部（共享内存区域的前 64 字节）
// ============================================================================

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct ShmHeader {
    magic: u32,         // 偏移量 0
    version: u32,       // 偏移量 4
    total_size: u64,    // 偏移量 8
    page_size: u32,     // 偏移量 16
    num_pages: u32,     // 偏移量 20
    bitmap_offset: u64, // 偏移量 24
    data_offset: u64,   // 偏移量 32
    free_pages: u32,    // 偏移量 40
    reserved: [u8; 20], // 偏移量 44（填充至 64 字节）
}

impl ShmHeader {
    fn new(total_size: u64, page_size: u32) -> Self {
        let num_pages = ((total_size - SHM_HEADER_SIZE) / page_size as u64) as u32;
        let bitmap_bytes = (num_pages as u64 + 7) / 8;
        let bitmap_offset = SHM_HEADER_SIZE;
        let data_offset = bitmap_offset + ((bitmap_bytes + 7) / 8) * 8; // 8 字节对齐

        Self {
            magic: SHM_MAGIC,
            version: SHM_VERSION,
            total_size,
            page_size,
            num_pages,
            bitmap_offset,
            data_offset,
            free_pages: num_pages,
            reserved: [0u8; 20],
        }
    }

    fn to_bytes(&self) -> [u8; SHM_HEADER_SIZE as usize] {
        let mut buf = [0u8; SHM_HEADER_SIZE as usize];
        let mut off = 0;

        buf[off..off + 4].copy_from_slice(&self.magic.to_le_bytes());
        off += 4;
        buf[off..off + 4].copy_from_slice(&self.version.to_le_bytes());
        off += 4;
        buf[off..off + 8].copy_from_slice(&self.total_size.to_le_bytes());
        off += 8;
        buf[off..off + 4].copy_from_slice(&self.page_size.to_le_bytes());
        off += 4;
        buf[off..off + 4].copy_from_slice(&self.num_pages.to_le_bytes());
        off += 4;
        buf[off..off + 8].copy_from_slice(&self.bitmap_offset.to_le_bytes());
        off += 8;
        buf[off..off + 8].copy_from_slice(&self.data_offset.to_le_bytes());
        off += 8;
        buf[off..off + 4].copy_from_slice(&self.free_pages.to_le_bytes());
        // 剩余部分为填充字节

        buf
    }

    fn from_bytes(buf: &[u8; SHM_HEADER_SIZE as usize]) -> Result<Self> {
        let mut off = 0;

        let magic = u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
        off += 4;

        if magic != SHM_MAGIC {
            return Err(MiniOsError::Shm(format!(
                "Invalid shared memory magic: 0x{:08X} (expected 0x{:08X})",
                magic, SHM_MAGIC
            )));
        }

        let version =
            u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
        off += 4;

        if version != SHM_VERSION {
            return Err(MiniOsError::Shm(format!(
                "Shared memory version mismatch: {} (expected {})",
                version, SHM_VERSION
            )));
        }

        let total_size = u64::from_le_bytes([
            buf[off], buf[off + 1], buf[off + 2], buf[off + 3],
            buf[off + 4], buf[off + 5], buf[off + 6], buf[off + 7],
        ]);
        off += 8;

        let page_size =
            u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
        off += 4;

        let num_pages =
            u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
        off += 4;

        let bitmap_offset = u64::from_le_bytes([
            buf[off], buf[off + 1], buf[off + 2], buf[off + 3],
            buf[off + 4], buf[off + 5], buf[off + 6], buf[off + 7],
        ]);
        off += 8;

        let data_offset = u64::from_le_bytes([
            buf[off], buf[off + 1], buf[off + 2], buf[off + 3],
            buf[off + 4], buf[off + 5], buf[off + 6], buf[off + 7],
        ]);
        off += 8;

        let free_pages =
            u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);

        Ok(Self {
            magic,
            version,
            total_size,
            page_size,
            num_pages,
            bitmap_offset,
            data_offset,
            free_pages,
            reserved: [0u8; 20],
        })
    }
}

// ============================================================================
// 共享内存管理器
// ============================================================================

/// 管理用于客户端-服务器数据传输的共享内存区域。
///
/// 服务器创建并拥有共享内存。客户端将其映射为可读写，
/// 以便在 Put/Get 操作期间进行数据传输。
///
/// ## 内存布局
/// ```text
/// +------------------+
/// | 头部 (64 B)      | 魔数、版本、大小、偏移量
/// +------------------+
/// | 页面位图         | 每页一位（1 = 已使用，0 = 空闲）
/// +------------------+
/// | 数据页           | num_pages * page_size 字节
/// +------------------+
/// ```
pub struct SharedMemory {
    /// 指向已映射共享内存的原始指针
    ptr: *mut u8,
    /// 映射区域的总大小（字节）
    size: u64,
    /// 共享内存对象的名称
    name: String,
    /// 共享内存对象的文件描述符
    shm_fd: i32,
    /// 此实例是否为共享内存的创建者（所有者）
    is_owner: bool,
}

// 安全性说明：SharedMemory 封装了来自 mmap 的原始指针，
// 在 Linux 上跨线程共享是安全的。
unsafe impl Send for SharedMemory {}
unsafe impl Sync for SharedMemory {}

impl SharedMemory {
    /// 从共享内存区域读取头部（始终从 mmap 读取权威副本，绝不缓存）。
    fn read_header(&self) -> ShmHeader {
        unsafe { std::ptr::read(self.ptr as *const ShmHeader) }
    }

    /// 将头部写回共享内存区域。
    #[allow(dead_code)]
    fn write_header(&self, header: &ShmHeader) {
        unsafe {
            (self.ptr as *mut ShmHeader).write(*header);
        }
    }

    /// 获取共享内存中头部的可变原始指针。
    fn header_mut_ptr(&self) -> *mut ShmHeader {
        self.ptr as *mut ShmHeader
    }

    /// 创建新的共享内存区域（服务器端调用）。
    ///
    /// 使用原始系统调用（`shm_open`、`ftruncate`、`mmap`）分配和初始化
    /// 共享内存。仅应从服务器进程调用此方法。如果共享内存对象已存在
    /// （例如，上次运行崩溃后残留），则会先取消链接再重试。
    pub fn create(name: &str, total_size: u64, page_size: u32) -> Result<Self> {
        let shm_name = if name.starts_with('/') {
            name.to_string()
        } else {
            format!("/{}", name)
        };

        info!(
            "Creating shared memory: name={}, size={}, page_size={}",
            shm_name, total_size, page_size
        );

        // 创建共享内存对象
        let shm_name_c = std::ffi::CString::new(shm_name.as_bytes())
            .map_err(|e| MiniOsError::Shm(format!("Invalid shm name: {}", e)))?;

        // 尝试创建共享内存对象。
        // 如果因之前崩溃运行而已经存在，则取消链接并重试。
        let fd = loop {
            let fd = unsafe {
                libc::shm_open(
                    shm_name_c.as_ptr(),
                    libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
                    0o666,
                )
            };

            if fd >= 0 {
                break fd;
            }

            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EEXIST) {
                warn!("Shared memory already exists, attempting to re-create...");
                unsafe { libc::shm_unlink(shm_name_c.as_ptr()) };
                // 循环回到 shm_open 重试
            } else {
                return Err(MiniOsError::Shm(format!(
                    "shm_open failed: {}",
                    err
                )));
            }
        };

        // 设置共享内存大小
        let ret = unsafe { libc::ftruncate(fd, total_size as libc::off_t) };
        if ret < 0 {
            let err = std::io::Error::last_os_error();
            unsafe { libc::close(fd) };
            unsafe { libc::shm_unlink(shm_name_c.as_ptr()) };
            return Err(MiniOsError::Shm(format!("ftruncate failed: {}", err)));
        }

        // 映射到进程地址空间
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                total_size as libc::size_t,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            let err = std::io::Error::last_os_error();
            unsafe { libc::close(fd) };
            unsafe { libc::shm_unlink(shm_name_c.as_ptr()) };
            return Err(MiniOsError::Shm(format!("mmap failed: {}", err)));
        }

        // 初始化头部 — 通过原始指针写入头部字节
        let header = ShmHeader::new(total_size, page_size);
        let header_bytes = header.to_bytes();
        unsafe {
            let dst = ptr as *mut u8;
            std::ptr::copy_nonoverlapping(
                header_bytes.as_ptr() as *const libc::c_void,
                dst as *mut libc::c_void,
                SHM_HEADER_SIZE as usize,
            );
        }

        // 初始化位图（全部清零 = 全部空闲）
        let bitmap_size = (header.num_pages as u64 + 7) / 8;
        let bitmap_ptr = unsafe { (ptr as *mut u8).add(header.bitmap_offset as usize) };
        unsafe {
            libc::memset(
                bitmap_ptr as *mut libc::c_void,
                0,
                bitmap_size as libc::size_t,
            );
        }

        info!(
            "Shared memory created: {} pages of {} bytes each ({} total free)",
            header.num_pages, header.page_size, header.free_pages
        );

        Ok(Self {
            ptr: ptr as *mut u8,
            size: total_size,
            name: shm_name,
            shm_fd: fd,
            is_owner: true,
        })
    }

    /// 打开已有的共享内存区域（客户端调用）。
    ///
    /// 客户端通过此方法连接到服务器已创建的共享内存。
    /// 首先只映射头部以获取实际的总大小，然后取消映射并以完整大小重新映射。
    pub fn open(name: &str) -> Result<Self> {
        let shm_name = if name.starts_with('/') {
            name.to_string()
        } else {
            format!("/{}", name)
        };

        debug!("Opening shared memory: {}", shm_name);

        let shm_name_c = std::ffi::CString::new(shm_name.as_bytes())
            .map_err(|e| MiniOsError::Shm(format!("Invalid shm name: {}", e)))?;

        let fd = unsafe { libc::shm_open(shm_name_c.as_ptr(), libc::O_RDWR, 0o666) };

        if fd < 0 {
            return Err(MiniOsError::Shm(format!(
                "shm_open failed: {}",
                std::io::Error::last_os_error()
            )));
        }

        // 首先只映射头部以获取总大小
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                SHM_HEADER_SIZE as libc::size_t,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            let err = std::io::Error::last_os_error();
            unsafe { libc::close(fd) };
            return Err(MiniOsError::Shm(format!("mmap header failed: {}", err)));
        }

        // 通过原始指针读取头部字节并解析
        let mut header_buf = [0u8; SHM_HEADER_SIZE as usize];
        unsafe {
            let src = ptr as *const u8;
            std::ptr::copy_nonoverlapping(
                src as *const libc::c_void,
                header_buf.as_mut_ptr() as *mut libc::c_void,
                SHM_HEADER_SIZE as usize,
            );
        }
        let header = ShmHeader::from_bytes(&header_buf)?;

        // 取消头部映射并以完整大小重新映射
        unsafe { libc::munmap(ptr, SHM_HEADER_SIZE as libc::size_t) };

        let full_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                header.total_size as libc::size_t,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };

        if full_ptr == libc::MAP_FAILED {
            let err = std::io::Error::last_os_error();
            unsafe { libc::close(fd) };
            return Err(MiniOsError::Shm(format!("mmap full failed: {}", err)));
        }

        debug!(
            "Shared memory opened: {} pages of {} bytes",
            header.num_pages, header.page_size
        );

        Ok(Self {
            ptr: full_ptr as *mut u8,
            size: header.total_size,
            name: shm_name,
            shm_fd: fd,
            is_owner: false,
        })
    }

    // --- 位图操作 ---

    /// 检查指定页面是否已被使用（已分配）。
    /// 如果页面索引超出范围，则视为"已使用"。
    fn is_page_used(&self, page: u32) -> bool {
        let header = self.read_header();
        if page >= header.num_pages {
            return true; // 超出范围视为"已使用"
        }
        let byte_idx = (page / 8) as usize;
        let bit_idx = (page % 8) as u8;
        let bitmap_ptr = unsafe { self.ptr.add(header.bitmap_offset as usize) };
        let byte = unsafe { *bitmap_ptr.add(byte_idx) };
        byte & (1 << bit_idx) != 0
    }

    /// 设置页面的分配位。
    ///
    /// 通过原始 mmap 指针实现内部可变性，因此 `&self` 共享引用即可满足需求。
    /// 同时会就地更新头部中的 `free_pages` 计数。
    fn set_page_bit(&self, page: u32, used: bool) {
        let header = self.read_header();
        if page >= header.num_pages {
            return;
        }
        let byte_idx = (page / 8) as usize;
        let bit_idx = (page % 8) as u8;
        let bitmap_ptr = unsafe { self.ptr.add(header.bitmap_offset as usize) };
        unsafe {
            let byte_ptr = bitmap_ptr.add(byte_idx);
            if used {
                *byte_ptr |= 1 << bit_idx;
            } else {
                *byte_ptr &= !(1 << bit_idx);
            }
        }

        // 就地更新头部中的 free_pages 计数（mmap 内存）
        let header_ptr = self.header_mut_ptr();
        unsafe {
            if used {
                (*header_ptr).free_pages -= 1;
            } else {
                (*header_ptr).free_pages += 1;
            }
        }
    }

    /// 获取指定页面数据区域的可变原始指针。
    ///
    /// 返回的指针指向页面数据区域的起始位置，调用者可直接读写。
    pub fn page_ptr(&self, page: u32) -> *mut u8 {
        let header = self.read_header();
        let offset = header.data_offset + page as u64 * header.page_size as u64;
        unsafe { self.ptr.add(offset as usize) }
    }

    /// 获取指定页面数据区域的不可变原始指针。
    #[allow(dead_code)]
    pub fn page_ptr_const(&self, page: u32) -> *const u8 {
        self.page_ptr(page) as *const u8
    }

    // --- 公共页面分配 API ---

    /// 分配 `count` 个连续的空闲页面。
    ///
    /// 返回起始页面编号。使用首次适应（first-fit）算法扫描位图。
    /// 如果没有足够的连续空闲页面，则会忙等待（每隔 10ms 重试）直到有足够页面为止。
    pub fn alloc_pages(&self, count: u32) -> Result<u32> {
        if count == 0 {
            return Err(MiniOsError::Shm(
                "Cannot allocate 0 pages".to_string(),
            ));
        }

        let num_pages = self.read_header().num_pages;
        if count > num_pages {
            return Err(MiniOsError::Shm(format!(
                "Requested {} pages but only {} exist",
                count, num_pages
            )));
        }

        // 简单的首次适应算法，忙等待连续空闲块
        loop {
            let header = self.read_header();
            let free = header.free_pages;
            if free < count {
                debug!(
                    "Waiting for free pages (need {}, have {})",
                    count, free
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
                continue;
            }

            // 首次适应扫描查找连续空闲页面
            let mut consecutive = 0u32;
            let mut start = 0u32;

            for page in 0..header.num_pages {
                if !self.is_page_used(page) {
                    if consecutive == 0 {
                        start = page;
                    }
                    consecutive += 1;
                    if consecutive == count {
                        // 找到连续空闲页面 — 标记为已使用
                        for p in start..start + count {
                            self.set_page_bit(p, true);
                        }
                        let hdr = self.read_header();
                        debug!(
                            "Allocated {} pages starting at page {} ({} free remaining)",
                            count, start, hdr.free_pages
                        );
                        return Ok(start);
                    }
                } else {
                    consecutive = 0;
                }
            }

            // 未找到连续块 — 等待并重试
            debug!(
                "No contiguous block of {} pages found ({} free), waiting...",
                count, header.free_pages
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// 释放从 `start_page` 开始的 `count` 个页面。
    ///
    /// 将对应页面的位图位清零，并更新空闲页面计数。
    /// 如果页面范围超出总数则返回错误。
    pub fn free_pages(&self, start_page: u32, count: u32) -> Result<()> {
        let header = self.read_header();
        if start_page + count > header.num_pages {
            return Err(MiniOsError::Shm(format!(
                "Cannot free pages {}-{} (max is {})",
                start_page,
                start_page + count - 1,
                header.num_pages
            )));
        }

        for page in start_page..start_page + count {
            self.set_page_bit(page, false);
        }

        let hdr = self.read_header();
        debug!(
            "Freed {} pages starting at page {} ({} free now)",
            count, start_page, hdr.free_pages
        );

        Ok(())
    }

    /// 将字节数据写入连续范围的页面中。
    ///
    /// 从 `start_page` 开始，将 `data` 中的字节复制到共享内存的页面区域。
    /// 如果数据长度超出共享内存可用空间则返回错误。
    pub fn write_pages(&self, start_page: u32, data: &[u8]) -> Result<()> {
        let header = self.read_header();
        let page_size = header.page_size as usize;
        let start_offset =
            header.data_offset as usize + start_page as usize * page_size;

        if start_offset + data.len() > self.size as usize {
            return Err(MiniOsError::Shm(format!(
                "Data size {} exceeds available space from page {}",
                data.len(),
                start_page
            )));
        }

        unsafe {
            let dst = self.ptr.add(start_offset);
            std::ptr::copy_nonoverlapping(
                data.as_ptr() as *const libc::c_void,
                dst as *mut libc::c_void,
                data.len(),
            );
        }

        debug!(
            "Wrote {} bytes to pages starting at {}",
            data.len(),
            start_page
        );
        Ok(())
    }

    /// 从连续范围的页面中读取数据。
    ///
    /// `count` 指定要读取的页面数量，`expected_size` 是期望的有效数据字节数。
    /// 返回的字节向量会被截断至 `expected_size` 长度。
    pub fn read_pages(&self, start_page: u32, count: u32, expected_size: u64) -> Result<Vec<u8>> {
        let header = self.read_header();
        let page_size = header.page_size as usize;
        let total_bytes = count as usize * page_size;
        let start_offset =
            header.data_offset as usize + start_page as usize * page_size;

        let mut data = vec![0u8; total_bytes];
        unsafe {
            let src = self.ptr.add(start_offset);
            std::ptr::copy_nonoverlapping(
                src as *const libc::c_void,
                data.as_mut_ptr() as *mut libc::c_void,
                total_bytes,
            );
        }

        data.truncate(expected_size as usize);
        Ok(data)
    }

    /// 获取共享内存的页面大小（字节）。
    pub fn page_size(&self) -> u32 {
        self.read_header().page_size
    }

    /// 获取共享内存中的页面总数。
    pub fn num_pages(&self) -> u32 {
        self.read_header().num_pages
    }

    /// 获取当前空闲（未分配）页面的数量。
    pub fn free_page_count(&self) -> u32 {
        self.read_header().free_pages
    }

    /// 获取共享内存对象的名称。
    #[allow(dead_code)]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 获取共享内存的完整状态信息，以调试字符串形式返回。
    ///
    /// 包含名称、页面总数、空闲页面数、页面大小和总大小等信息。
    #[allow(dead_code)]
    pub fn status_string(&self) -> String {
        let header = self.read_header();
        format!(
            "SHM '{}': {} pages ({} free), page_size={}, total={}",
            self.name,
            header.num_pages,
            header.free_pages,
            header.page_size,
            header.total_size,
        )
    }
}

impl Drop for SharedMemory {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                libc::munmap(self.ptr as *mut libc::c_void, self.size as libc::size_t);
            }
        }
        if self.shm_fd >= 0 {
            unsafe { libc::close(self.shm_fd) };
        }
        if self.is_owner {
            if let Ok(name_c) = std::ffi::CString::new(self.name.as_bytes()) {
                unsafe { libc::shm_unlink(name_c.as_ptr()) };
            }
            info!("Shared memory '{}' destroyed", self.name);
        }
    }
}

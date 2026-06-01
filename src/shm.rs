use crate::error::{MiniOsError, Result};
use log::{debug, info, warn};

// ============================================================================
// Constants
// ============================================================================

/// Magic number for shared memory header identification
const SHM_MAGIC: u32 = 0x4D4F5348; // "MOSH" in little-endian
/// Current shared memory layout version
const SHM_VERSION: u32 = 1;
/// Header size in bytes
const SHM_HEADER_SIZE: u64 = 64;

// ============================================================================
// Shared Memory Header (first 64 bytes of the shared memory region)
// ============================================================================

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct ShmHeader {
    magic: u32,         // offset 0
    version: u32,       // offset 4
    total_size: u64,    // offset 8
    page_size: u32,     // offset 16
    num_pages: u32,     // offset 20
    bitmap_offset: u64, // offset 24
    data_offset: u64,   // offset 32
    free_pages: u32,    // offset 40
    reserved: [u8; 20], // offset 44 (pad to 64 bytes)
}

impl ShmHeader {
    fn new(total_size: u64, page_size: u32) -> Self {
        let num_pages = ((total_size - SHM_HEADER_SIZE) / page_size as u64) as u32;
        let bitmap_bytes = (num_pages as u64 + 7) / 8;
        let bitmap_offset = SHM_HEADER_SIZE;
        let data_offset = bitmap_offset + ((bitmap_bytes + 7) / 8) * 8; // 8-byte aligned

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
        // rest is padding

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
// Shared Memory Manager
// ============================================================================

/// Manages the shared memory region used for client-server data transfer.
///
/// The server creates and owns the shared memory. Clients map it read/write
/// for data transfer during Put/Get operations.
///
/// ## Layout
/// ```text
/// +------------------+
/// | Header (64 B)    | magic, version, sizes, offsets
/// +------------------+
/// | Page Bitmap      | one bit per page (1 = used, 0 = free)
/// +------------------+
/// | Data Pages       | num_pages * page_size bytes
/// +------------------+
/// ```
pub struct SharedMemory {
    /// Raw pointer to the mapped shared memory
    ptr: *mut u8,
    /// Total size of the mapping
    size: u64,
    /// Name of the shared memory object
    name: String,
    /// File descriptor for the shared memory object
    shm_fd: i32,
    /// Whether this instance owns (created) the shared memory
    is_owner: bool,
}

// Safety: SharedMemory wraps a raw pointer from mmap, which is safe to
// share across threads on Linux.
unsafe impl Send for SharedMemory {}
unsafe impl Sync for SharedMemory {}

impl SharedMemory {
    /// Read the header from the shared memory region (always reads the
    /// authoritative copy from mmap, never caches).
    fn read_header(&self) -> ShmHeader {
        unsafe { std::ptr::read(self.ptr as *const ShmHeader) }
    }

    /// Write a header back to the shared memory region.
    fn write_header(&self, header: &ShmHeader) {
        unsafe {
            (self.ptr as *mut ShmHeader).write(*header);
        }
    }

    /// Get a mutable reference to the header in shared memory.
    fn header_mut_ptr(&self) -> *mut ShmHeader {
        self.ptr as *mut ShmHeader
    }

    /// Create a new shared memory region (server-side)
    ///
    /// Uses raw system calls (shm_open, ftruncate, mmap). Only call this
    /// from the server process.
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

        // Create shared memory object
        let shm_name_c = std::ffi::CString::new(shm_name.as_bytes())
            .map_err(|e| MiniOsError::Shm(format!("Invalid shm name: {}", e)))?;

        let fd = unsafe {
            libc::shm_open(
                shm_name_c.as_ptr(),
                libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
                0o666,
            )
        };

        if fd < 0 {
            let err = std::io::Error::last_os_error();
            // If already exists, try opening it
            if err.raw_os_error() == Some(libc::EEXIST) {
                warn!("Shared memory already exists, attempting to re-create...");
                // Unlink and try again
                unsafe { libc::shm_unlink(shm_name_c.as_ptr()) };
                let fd2 = unsafe {
                    libc::shm_open(
                        shm_name_c.as_ptr(),
                        libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
                        0o666,
                    )
                };
                if fd2 < 0 {
                    return Err(MiniOsError::Shm(format!(
                        "shm_open failed after cleanup: {}",
                        std::io::Error::last_os_error()
                    )));
                }
                fd2
            } else {
                return Err(MiniOsError::Shm(format!(
                    "shm_open failed: {}",
                    err
                )));
            }
        } else {
            fd
        };

        // Set size
        let ret = unsafe { libc::ftruncate(fd, total_size as libc::off_t) };
        if ret < 0 {
            let err = std::io::Error::last_os_error();
            unsafe { libc::close(fd) };
            unsafe { libc::shm_unlink(shm_name_c.as_ptr()) };
            return Err(MiniOsError::Shm(format!("ftruncate failed: {}", err)));
        }

        // Map into memory
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

        // Initialize header — write via raw pointer, then read back to verify
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

        // Initialize bitmap (all zeros = all free)
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

    /// Open an existing shared memory region (client-side)
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

        // First map just the header to get the total size
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

        // Read header via raw pointer
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

        // Unmap and remap at full size
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

    // --- Bitmap Operations ---

    /// Check if a page is used
    fn is_page_used(&self, page: u32) -> bool {
        let header = self.read_header();
        if page >= header.num_pages {
            return true; // out of range = "used"
        }
        let byte_idx = (page / 8) as usize;
        let bit_idx = (page % 8) as u8;
        let bitmap_ptr = unsafe { self.ptr.add(header.bitmap_offset as usize) };
        let byte = unsafe { *bitmap_ptr.add(byte_idx) };
        byte & (1 << bit_idx) != 0
    }

    /// Set a page's allocation bit. Uses interior mutability through the
    /// raw mmap pointer, so `&self` is sufficient.
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

        // Update header free_pages in-place (mmap'd memory)
        let header_ptr = self.header_mut_ptr();
        unsafe {
            if used {
                (*header_ptr).free_pages -= 1;
            } else {
                (*header_ptr).free_pages += 1;
            }
        }
    }

    /// Get a pointer to the data area of a specific page
    pub fn page_ptr(&self, page: u32) -> *mut u8 {
        let header = self.read_header();
        let offset = header.data_offset + page as u64 * header.page_size as u64;
        unsafe { self.ptr.add(offset as usize) }
    }

    /// Get a const pointer to the data area of a specific page
    #[allow(dead_code)]
    pub fn page_ptr_const(&self, page: u32) -> *const u8 {
        self.page_ptr(page) as *const u8
    }

    // --- Public Page Allocation API ---

    /// Allocate `count` contiguous free pages.
    ///
    /// Returns the starting page number. Blocks (waits) if insufficient
    /// contiguous free pages are available.
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

        // Simple first-fit with busy-wait for contiguity
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

            // First-fit scan for contiguous free pages
            let mut consecutive = 0u32;
            let mut start = 0u32;

            for page in 0..header.num_pages {
                if !self.is_page_used(page) {
                    if consecutive == 0 {
                        start = page;
                    }
                    consecutive += 1;
                    if consecutive == count {
                        // Found contiguous free pages — mark them as used
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

            // No contiguous block found — wait and retry
            debug!(
                "No contiguous block of {} pages found ({} free), waiting...",
                count, header.free_pages
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// Free `count` pages starting from `start_page`
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

    /// Write data to a contiguous range of pages
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

    /// Read data from a contiguous range of pages
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

    /// Get the page size
    pub fn page_size(&self) -> u32 {
        self.read_header().page_size
    }

    /// Get the total number of pages
    pub fn num_pages(&self) -> u32 {
        self.read_header().num_pages
    }

    /// Get the number of free pages
    pub fn free_page_count(&self) -> u32 {
        self.read_header().free_pages
    }

    /// Get the shared memory name
    #[allow(dead_code)]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get shared memory status as a debug string
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

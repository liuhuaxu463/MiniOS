use crate::error::{MiniOsError, Result};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::RwLock;

// ============================================================================
// 常量定义
// ============================================================================

/// store.odb 文件的魔数标识
const MAGIC: &[u8; 4] = b"MOS\0";
/// 当前文件格式版本号
const VERSION: u32 = 1;
/// 超级块始终位于第一个块（4096 字节）
const SUPER_BLOCK_SIZE: u64 = 4096;
/// 超级块占用的块数量
const SUPER_BLOCK_COUNT: u64 = 1;

// ============================================================================
// 数据结构
// ============================================================================

/// 存储对象的信息（返回给调用方）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectInfo {
    /// 对象的唯一标识符（UUID 字符串）
    pub uuid: String,
    /// 对象名称
    pub name: String,
    /// 对象数据大小（字节数）
    pub size: u64,
    /// 对象的 MIME 内容类型
    pub content_type: String,
    /// 对象创建时间（格式化的日期时间字符串）
    pub created_at: String,
    /// 对象的标签（用于分类和检索）
    pub tags: String,
    /// 对象占用的数据块数量
    pub block_count: u32,
}

/// 存储引擎的运行状态信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStatus {
    /// 文件系统中的总块数
    pub total_blocks: u64,
    /// 当前空闲的块数
    pub free_blocks: u64,
    /// 当前已使用的块数
    pub used_blocks: u64,
    /// 每个块的大小（字节数）
    pub block_size: u32,
    /// 当前存储的对象数量
    pub object_count: u64,
    /// 最大可存储的对象数量
    pub max_objects: u64,
    /// 元数据区当前已使用的大小（字节数）
    pub metadata_area_size: u64,
    /// 元数据区的最大容量（字节数）
    pub max_metadata_area_size: u64,
    /// 存储文件的路径
    pub store_path: String,
    /// 存储文件的总容量（字节数）
    pub total_capacity: u64,
    /// 已使用的容量（字节数）
    pub used_capacity: u64,
    /// 剩余可用的容量（字节数）
    pub free_capacity: u64,
}

// ============================================================================
// 超级块（位于文件偏移 0 处的 4096 字节）
// ============================================================================

/// 超级块的磁盘布局（共 4096 字节）
#[derive(Debug, Clone)]
struct SuperBlock {
    magic: [u8; 4],            // 偏移 0：魔数标识
    version: u32,              // 偏移 4：版本号
    block_size: u32,           // 偏移 8：块大小
    total_blocks: u64,         // 偏移 12：总块数
    free_blocks: u64,          // 偏移 20：空闲块数
    object_count: u64,         // 偏移 28：对象数量
    metadata_area_size: u64,   // 偏移 36：元数据区大小
    max_metadata_area_size: u64, // 偏移 44：元数据区最大容量
    bitmap_size: u64,          // 偏移 52：位图大小
    created_at: i64,           // 偏移 60：创建时间戳
    flags: u32,                // 偏移 68：标志位
    // 填充至 4096 字节
}

impl SuperBlock {
    /// 创建一个新的超级块实例
    fn new(block_size: u32, total_blocks: u64, max_metadata_area_size: u64) -> Self {
        let bitmap_size = (total_blocks + 7) / 8;
        // 将位图对齐到 8 字节边界
        let bitmap_size = ((bitmap_size + 7) / 8) * 8;
        Self {
            magic: *MAGIC,
            version: VERSION,
            block_size,
            total_blocks,
            free_blocks: total_blocks - SUPER_BLOCK_COUNT,
            object_count: 0,
            metadata_area_size: 0,
            max_metadata_area_size,
            bitmap_size,
            created_at: chrono::Utc::now().timestamp(),
            flags: 0,
        }
    }

    /// 将超级块序列化为原始字节数组（4096 字节）
    fn to_bytes(&self) -> [u8; SUPER_BLOCK_SIZE as usize] {
        let mut buf = [0u8; SUPER_BLOCK_SIZE as usize];
        let mut offset = 0;

        // 魔数（4 字节）
        buf[offset..offset + 4].copy_from_slice(&self.magic);
        offset += 4;

        // 版本号（4 字节，小端序）
        buf[offset..offset + 4].copy_from_slice(&self.version.to_le_bytes());
        offset += 4;

        // 块大小（4 字节）
        buf[offset..offset + 4].copy_from_slice(&self.block_size.to_le_bytes());
        offset += 4;

        // 总块数（8 字节）
        buf[offset..offset + 8].copy_from_slice(&self.total_blocks.to_le_bytes());
        offset += 8;

        // 空闲块数（8 字节）
        buf[offset..offset + 8].copy_from_slice(&self.free_blocks.to_le_bytes());
        offset += 8;

        // 对象数量（8 字节）
        buf[offset..offset + 8].copy_from_slice(&self.object_count.to_le_bytes());
        offset += 8;

        // 元数据区大小（8 字节）
        buf[offset..offset + 8].copy_from_slice(&self.metadata_area_size.to_le_bytes());
        offset += 8;

        // 元数据区最大容量（8 字节）
        buf[offset..offset + 8].copy_from_slice(&self.max_metadata_area_size.to_le_bytes());
        offset += 8;

        // 位图大小（8 字节）
        buf[offset..offset + 8].copy_from_slice(&self.bitmap_size.to_le_bytes());
        offset += 8;

        // 创建时间戳（8 字节）
        buf[offset..offset + 8].copy_from_slice(&self.created_at.to_le_bytes());
        offset += 8;

        // 标志位（4 字节）
        buf[offset..offset + 4].copy_from_slice(&self.flags.to_le_bytes());
        // offset += 4; // 不需要，剩余部分为填充

        buf
    }

    /// 从原始字节数组反序列化出超级块
    fn from_bytes(buf: &[u8; SUPER_BLOCK_SIZE as usize]) -> Result<Self> {
        let mut offset = 0;

        let mut magic = [0u8; 4];
        magic.copy_from_slice(&buf[offset..offset + 4]);
        offset += 4;

        if &magic != MAGIC {
            return Err(MiniOsError::Storage(
                "无效的魔数：不是 MiniOS 存储文件".to_string(),
            ));
        }

        let version = u32::from_le_bytes([
            buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3],
        ]);
        offset += 4;

        if version != VERSION {
            return Err(MiniOsError::Storage(format!(
                "不支持的版本：{}（期望版本 {}）",
                version, VERSION
            )));
        }

        let block_size = u32::from_le_bytes([
            buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3],
        ]);
        offset += 4;

        let total_blocks = u64::from_le_bytes([
            buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3],
            buf[offset + 4], buf[offset + 5], buf[offset + 6], buf[offset + 7],
        ]);
        offset += 8;

        let free_blocks = u64::from_le_bytes([
            buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3],
            buf[offset + 4], buf[offset + 5], buf[offset + 6], buf[offset + 7],
        ]);
        offset += 8;

        let object_count = u64::from_le_bytes([
            buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3],
            buf[offset + 4], buf[offset + 5], buf[offset + 6], buf[offset + 7],
        ]);
        offset += 8;

        let metadata_area_size = u64::from_le_bytes([
            buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3],
            buf[offset + 4], buf[offset + 5], buf[offset + 6], buf[offset + 7],
        ]);
        offset += 8;

        let max_metadata_area_size = u64::from_le_bytes([
            buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3],
            buf[offset + 4], buf[offset + 5], buf[offset + 6], buf[offset + 7],
        ]);
        offset += 8;

        let bitmap_size = u64::from_le_bytes([
            buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3],
            buf[offset + 4], buf[offset + 5], buf[offset + 6], buf[offset + 7],
        ]);
        offset += 8;

        let created_at = i64::from_le_bytes([
            buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3],
            buf[offset + 4], buf[offset + 5], buf[offset + 6], buf[offset + 7],
        ]);
        offset += 8;

        let flags = u32::from_le_bytes([
            buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3],
        ]);

        Ok(Self {
            magic,
            version,
            block_size,
            total_blocks,
            free_blocks,
            object_count,
            metadata_area_size,
            max_metadata_area_size,
            bitmap_size,
            created_at,
            flags,
        })
    }
}

// ============================================================================
// 元数据条目（变长，存储在元数据区中）
// ============================================================================

/// 磁盘上元数据条目的删除标记位
const META_FLAG_DELETED: u8 = 0x01;

/// 元数据区中的一个元数据条目
#[derive(Debug, Clone)]
struct MetadataEntry {
    /// 对象的唯一标识符（UUID）
    uuid: uuid::Uuid,
    /// 标志位（如删除标记等）
    flags: u8,
    /// 对象名称
    name: String,
    /// 对象数据大小（字节数）
    size: u64,
    /// 对象的 MIME 内容类型
    content_type: String,
    /// 对象创建时间（Unix 时间戳）
    created_at: i64,
    /// 对象标签
    tags: String,
    /// 数据块指针列表（指向各数据块的块号）
    block_pointers: Vec<u64>,
    /// 本条目的磁盘总大小（包含此字段自身）
    entry_size: u32,
}

impl MetadataEntry {
    /// 创建一个新的元数据条目
    fn new(
        name: &str,
        size: u64,
        content_type: &str,
        tags: &str,
        block_pointers: Vec<u64>,
    ) -> Self {
        Self {
            uuid: uuid::Uuid::new_v4(),
            flags: 0,
            name: name.to_string(),
            size,
            content_type: content_type.to_string(),
            created_at: chrono::Utc::now().timestamp(),
            tags: tags.to_string(),
            block_pointers,
            entry_size: 0, // 序列化时计算
        }
    }

    /// 计算本条目的磁盘大小
    fn calculate_entry_size(&self) -> u32 {
        // uuid(16) + flags(1) + name_len(2) + name + size(8) +
        // content_type_len(2) + content_type + created_at(8) +
        // tags_len(2) + tags + block_count(4) +
        // block_pointers(8*N) + entry_size(4)
        let size = 16
            + 1
            + 2
            + self.name.len() as u32
            + 8
            + 2
            + self.content_type.len() as u32
            + 8
            + 2
            + self.tags.len() as u32
            + 4
            + (8 * self.block_pointers.len()) as u32
            + 4;
        size
    }

    /// 将元数据条目序列化为字节数组
    fn to_bytes(&self) -> Vec<u8> {
        let entry_size = self.calculate_entry_size();
        let mut buf = Vec::with_capacity(entry_size as usize);

        // uuid（16 字节）
        buf.extend_from_slice(self.uuid.as_bytes());

        // 标志位（1 字节）
        buf.push(self.flags);

        // 名称长度（2 字节，小端序）
        buf.extend_from_slice(&(self.name.len() as u16).to_le_bytes());

        // 名称（变长）
        buf.extend_from_slice(self.name.as_bytes());

        // 数据大小（8 字节，小端序）
        buf.extend_from_slice(&self.size.to_le_bytes());

        // 内容类型长度（2 字节，小端序）
        buf.extend_from_slice(&(self.content_type.len() as u16).to_le_bytes());

        // 内容类型（变长）
        buf.extend_from_slice(self.content_type.as_bytes());

        // 创建时间（8 字节，小端序）
        buf.extend_from_slice(&self.created_at.to_le_bytes());

        // 标签长度（2 字节，小端序）
        buf.extend_from_slice(&(self.tags.len() as u16).to_le_bytes());

        // 标签（变长）
        buf.extend_from_slice(self.tags.as_bytes());

        // 块数量（4 字节，小端序）
        buf.extend_from_slice(&(self.block_pointers.len() as u32).to_le_bytes());

        // 块指针列表（每个 8 字节，小端序）
        for &ptr in &self.block_pointers {
            buf.extend_from_slice(&ptr.to_le_bytes());
        }

        // 条目大小（4 字节，小端序）
        buf.extend_from_slice(&entry_size.to_le_bytes());

        buf
    }

    /// 从字节数组反序列化元数据条目，返回 (条目, 消耗的字节数)
    fn from_bytes(data: &[u8]) -> Result<(Self, u32)> {
        if data.len() < 16 + 1 + 2 + 8 + 2 + 8 + 2 + 4 + 4 {
            return Err(MiniOsError::Storage(
                "元数据条目太短".to_string(),
            ));
        }

        let mut offset = 0;

        // uuid（16 字节）
        let uuid = uuid::Uuid::from_slice(&data[offset..offset + 16]).map_err(|e| {
            MiniOsError::Storage(format!("元数据中的 UUID 无效：{}", e))
        })?;
        offset += 16;

        // 标志位（1 字节）
        let flags = data[offset];
        offset += 1;

        // 名称长度（2 字节，小端序）
        let name_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;

        // 名称（变长）
        if offset + name_len > data.len() {
            return Err(MiniOsError::Storage("损坏的元数据：名称字段".to_string()));
        }
        let name = std::str::from_utf8(&data[offset..offset + name_len])?.to_string();
        offset += name_len;

        // 数据大小（8 字节，小端序）
        let size = u64::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
            data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
        ]);
        offset += 8;

        // 内容类型长度（2 字节，小端序）
        let ct_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;

        // 内容类型（变长）
        if offset + ct_len > data.len() {
            return Err(MiniOsError::Storage(
                "损坏的元数据：内容类型字段".to_string(),
            ));
        }
        let content_type = std::str::from_utf8(&data[offset..offset + ct_len])?.to_string();
        offset += ct_len;

        // 创建时间（8 字节，小端序）
        let created_at = i64::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
            data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
        ]);
        offset += 8;

        // 标签长度（2 字节，小端序）
        let tags_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;

        // 标签（变长）
        if offset + tags_len > data.len() {
            return Err(MiniOsError::Storage("损坏的元数据：标签字段".to_string()));
        }
        let tags = std::str::from_utf8(&data[offset..offset + tags_len])?.to_string();
        offset += tags_len;

        // 块数量（4 字节，小端序）
        let block_count = u32::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
        ]) as usize;
        offset += 4;

        // 块指针列表（每个 8 字节，小端序）
        let mut block_pointers = Vec::with_capacity(block_count);
        for _ in 0..block_count {
            if offset + 8 > data.len() {
                return Err(MiniOsError::Storage(
                    "损坏的元数据：块指针列表字段".to_string(),
                ));
            }
            let ptr = u64::from_le_bytes([
                data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
                data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
            ]);
            block_pointers.push(ptr);
            offset += 8;
        }

        // 条目大小（4 字节，小端序）
        let entry_size = u32::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
        ]);

        Ok((
            Self {
                uuid,
                flags,
                name,
                size,
                content_type,
                created_at,
                tags,
                block_pointers,
                entry_size,
            },
            entry_size,
        ))
    }

    /// 将元数据条目转换为 ObjectInfo，供外部 API 使用
    fn to_object_info(&self) -> ObjectInfo {
        ObjectInfo {
            uuid: self.uuid.to_string(),
            name: self.name.clone(),
            size: self.size,
            content_type: self.content_type.clone(),
            created_at: {
                if let Some(dt) = chrono::DateTime::from_timestamp(self.created_at, 0) {
                    dt.format("%Y-%m-%d %H:%M:%S").to_string()
                } else {
                    "unknown".to_string()
                }
            },
            tags: self.tags.clone(),
            block_count: self.block_pointers.len() as u32,
        }
    }
}

// ============================================================================
// 对象存储引擎
// ============================================================================

/// 对象存储引擎主结构体，负责管理 store.odb 文件
pub struct ObjectStorage {
    /// 底层存储文件句柄（仅用于写入操作）
    writer: File,
    /// 独立只读文件句柄，通过 RefCell 实现内部可变性，
    /// 允许 &self 方法在读取时修改文件偏移指针
    reader: RefCell<File>,
    /// 文件的超级块（包含文件系统的全局元信息）
    super_block: SuperBlock,
    /// 空闲块位图的内存副本（每个 bit 表示一个块是否已分配）
    bitmap: Vec<u8>,
    /// 脏标记：标记内存中的超级块和位图是否已修改但尚未写入磁盘
    dirty: bool,
}

impl ObjectStorage {
    // --- 偏移量计算 ---

    /// 计算位图在文件中的起始偏移量
    fn bitmap_offset() -> u64 {
        SUPER_BLOCK_SIZE
    }

    /// 计算元数据区在文件中的起始偏移量
    fn metadata_offset(&self) -> u64 {
        SUPER_BLOCK_SIZE + self.super_block.bitmap_size
    }

    /// 计算数据区在文件中的起始偏移量
    fn data_offset(&self) -> u64 {
        SUPER_BLOCK_SIZE
            + self.super_block.bitmap_size
            + self.super_block.max_metadata_area_size
    }

    /// 计算指定数据块在文件中的偏移量
    fn block_offset(&self, block_num: u64) -> u64 {
        self.data_offset() + block_num * self.super_block.block_size as u64
    }

    // --- 公共 API ---

    /// 打开一个已有的存储文件，若文件不存在则创建一个新的。
    ///
    /// # 参数
    /// * `path` - 存储文件的路径
    /// * `block_size` - 每个数据块的大小（字节数）
    /// * `total_blocks` - 文件系统中数据块的总数
    /// * `max_objects` - 最大可存储的对象数量
    ///
    /// # 返回值
    /// 成功时返回 `ObjectStorage` 实例，失败时返回错误。
    pub fn open(
        path: &str,
        block_size: u32,
        total_blocks: u64,
        max_objects: u64,
    ) -> Result<Self> {
        // 根据最大对象数估算元数据区的最大大小
        // 平均每个条目约 200 字节
        let max_metadata_area_size = max_objects * 256;

        let path = Path::new(path);
        let exists = path.exists();

        let writer = OpenOptions::new()
            .read(true).write(true).create(true).open(path)?;
        let reader = RefCell::new(OpenOptions::new()
            .read(true).open(path)?);  // 独立只读句柄，RefCell 允许 &self 方法中修改文件偏移

        if exists && writer.metadata()?.len() > 0 {
            // 读取已有的超级块
            let mut sb_buf = [0u8; SUPER_BLOCK_SIZE as usize];
            let mut f = &reader;
            f.seek(SeekFrom::Start(0))?;
            f.read_exact(&mut sb_buf)?;
            let super_block = SuperBlock::from_bytes(&sb_buf)?;

            if super_block.block_size != block_size {
                return Err(MiniOsError::Storage(format!(
                    "块大小不匹配：文件中为 {}，请求为 {}",
                    super_block.block_size, block_size
                )));
            }

            // 将位图读入内存
            let mut bitmap = vec![0u8; super_block.bitmap_size as usize];
            let mut f = &reader;
            f.seek(SeekFrom::Start(Self::bitmap_offset()))?;
            f.read_exact(&mut bitmap)?;

            Ok(Self {
                writer, reader, super_block, bitmap, dirty: false,
            })
        } else {
            let super_block = SuperBlock::new(block_size, total_blocks, max_metadata_area_size);
            let bitmap = vec![0u8; super_block.bitmap_size as usize];
            let mut storage = Self {
                writer, reader, super_block, bitmap, dirty: true,
            };

            // 初始化文件布局
            storage.init_file()?;
            storage.flush()?;

            Ok(storage)
        }
    }

    /// 初始化一个新的存储文件（写入超级块、位图和空的元数据区）
    fn init_file(&mut self) -> Result<()> {
        // 计算文件总大小
        let file_size = self.data_offset()
            + self.super_block.total_blocks * self.super_block.block_size as u64;

        // 截断（或预分配）文件到指定大小
        self.writer.set_len(file_size)?;

        // 写入超级块
        let sb_bytes = self.super_block.to_bytes();
        self.writer.seek(SeekFrom::Start(0))?;
        self.writer.write_all(&sb_bytes)?;

        // 写入初始位图（全零 = 全部空闲，但超级块区域除外）
        // 将超级块、位图和元数据区占用的块标记为已使用
        let reserved_blocks =
            (self.data_offset() + self.super_block.block_size as u64 - 1)
                / self.super_block.block_size as u64;
        for i in 0..reserved_blocks {
            self.set_bitmap_bit(i, true)?;
        }

        // 写入位图
        self.writer.seek(SeekFrom::Start(Self::bitmap_offset()))?;
        self.writer.write_all(&self.bitmap)?;

        self.writer.flush()?;
        self.dirty = true;

        Ok(())
    }

    /// 将一个对象存入存储系统。
    ///
    /// # 参数
    /// * `name` - 对象名称（不可重复）
    /// * `data` - 对象的原始字节数据
    /// * `content_type` - 对象的 MIME 内容类型
    /// * `tags` - 对象的标签字符串
    ///
    /// # 返回值
    /// 成功时返回包含生成的 UUID 等信息的 `ObjectInfo`，失败时返回错误。
    /// 如果同名对象已存在，则返回 `AlreadyExists` 错误。
    /// 如果存储空间不足，则返回 `NoSpace` 错误。
    pub fn put(
        &mut self,
        name: &str,
        data: &[u8],
        content_type: &str,
        tags: &str,
    ) -> Result<ObjectInfo> {
        // 检查是否有同名对象
        if self.find_by_name(name).is_some() {
            return Err(MiniOsError::AlreadyExists(format!(
                "名为 '{}' 的对象已存在",
                name
            )));
        }

        let data_len = data.len() as u64;
        let block_size = self.super_block.block_size as u64;
        let blocks_needed = if data_len == 0 {
            1 // 空对象也占用 1 个块
        } else {
            (data_len + block_size - 1) / block_size
        };

        // 检查空闲空间
        if self.super_block.free_blocks < blocks_needed {
            return Err(MiniOsError::NoSpace);
        }

        // 分配数据块
        let block_pointers = self.allocate_blocks(blocks_needed)?;

        // 将数据写入已分配的各数据块
        for (i, &block_num) in block_pointers.iter().enumerate() {
            let offset = self.block_offset(block_num);
            let start = i * block_size as usize;
            let end = std::cmp::min(start + block_size as usize, data.len());
            let chunk = &data[start..end];

            self.writer.seek(SeekFrom::Start(offset))?;

            // 先写数据块，然后将块中剩余部分填充零。
            // 这样做可以防止之前释放的块中的残留数据泄露到读取操作中。
            let padding_len = block_size as usize - chunk.len();
            self.writer.write_all(chunk)?;
            if padding_len > 0 {
                // 用一次写入高效地将剩余部分清零
                let zeros = vec![0u8; padding_len];
                self.writer.write_all(&zeros)?;
            }
        }

        // 创建元数据条目
        let entry = MetadataEntry::new(name, data_len, content_type, tags, block_pointers);
        let info = entry.to_object_info();
        let entry_bytes = entry.to_bytes();

        // 写入元数据条目（优先尝试复用已删除条目的空间）
        self.write_metadata_entry(&entry_bytes)?;

        // 更新超级块
        self.super_block.object_count += 1;
        self.dirty = true;

        // 刷新到磁盘，确保后续的读取操作能看到最新状态
        self.flush()?;

        Ok(info)
    }

    /// 根据 UUID 或名称查找对象的元信息，但不读取数据块。
    ///
    /// 返回 `ObjectInfo`；如需同时读取数据，请使用 `get()` 方法。
    ///
    /// # 参数
    /// * `key` - 对象的 UUID 字符串或名称
    ///
    /// # 返回值
    /// 成功时返回 `ObjectInfo`，未找到时返回 `NotFound` 错误。
    pub fn find_info(&self, key: &str) -> Result<ObjectInfo> {
        let entry = self
            .find_entry(key)?
            .ok_or_else(|| MiniOsError::NotFound(key.to_string()))?;

        if entry.flags & META_FLAG_DELETED != 0 {
            return Err(MiniOsError::NotFound(key.to_string()));
        }
        Ok(entry.to_object_info())
    }

    /// 根据 UUID 或名称从存储中读取对象。
    ///
    /// 同时返回对象的元信息 (`ObjectInfo`) 和原始数据字节。
    ///
    /// # 参数
    /// * `key` - 对象的 UUID 字符串或名称
    ///
    /// # 返回值
    /// 成功时返回 `(ObjectInfo, Vec<u8>)` 元组，
    /// 未找到时返回 `NotFound` 错误。
    pub fn get(&self, key: &str) -> Result<(ObjectInfo, Vec<u8>)> {
        // 已经使用了 &mut self —— 正确
        let entry = self
            .find_entry(key)?
            .ok_or_else(|| MiniOsError::NotFound(format!("未找到对象：{}", key)))?;

        if entry.flags & META_FLAG_DELETED != 0 {
            return Err(MiniOsError::NotFound(format!("未找到对象：{}", key)));
        }

        // 从数据块中读取数据
        let block_size = self.super_block.block_size as usize;
        let mut data = Vec::with_capacity(entry.size as usize);

        for &block_num in &entry.block_pointers {
            let offset = self.block_offset(block_num);
            self.reader.borrow_mut().seek(SeekFrom::Start(offset))?;

            let mut chunk = vec![0u8; block_size];
            self.reader.borrow_mut().read_exact(&mut chunk)?;
            data.extend_from_slice(&chunk);
        }

        // 将数据截断到实际大小
        data.truncate(entry.size as usize);

        Ok((entry.to_object_info(), data))
    }

    /// 根据 UUID 或名称删除一个对象。
    ///
    /// 删除操作会将对象的数据块标记为空闲，并将元数据条目标记为已删除。
    ///
    /// # 参数
    /// * `key` - 要删除的对象的 UUID 字符串或名称
    ///
    /// # 返回值
    /// 成功时返回 `Ok(())`，未找到时返回 `NotFound` 错误。
    pub fn delete(&mut self, key: &str) -> Result<()> {
        // 在元数据区中查找条目的偏移量
        let (entry, entry_offset) = self
            .find_entry_with_offset(key)?
            .ok_or_else(|| MiniOsError::NotFound(format!("未找到对象：{}", key)))?;

        if entry.flags & META_FLAG_DELETED != 0 {
            return Err(MiniOsError::NotFound(format!("未找到对象：{}", key)));
        }

        // 释放数据块
        for &block_num in &entry.block_pointers {
            self.set_bitmap_bit(block_num, false)?;
        }
        self.super_block.free_blocks += entry.block_pointers.len() as u64;

        // 将元数据条目标记为已删除
        let flags_offset = entry_offset + 16; // uuid 占 16 字节，flags 紧跟其后
        self.writer.seek(SeekFrom::Start(flags_offset))?;
        self.writer.write_all(&[META_FLAG_DELETED])?;

        // 更新超级块
        self.super_block.object_count -= 1;
        self.dirty = true;

        // 刷新到磁盘，确保后续的读取操作能看到最新状态
        self.flush()?;

        Ok(())
    }

    /// 列出存储中的所有对象。
    ///
    /// # 返回值
    /// 成功时返回所有未删除对象的 `ObjectInfo` 列表。
    pub fn list(&self) -> Result<Vec<ObjectInfo>> {
        let mut objects = Vec::new();
        let entries = self.scan_all_entries()?;
        for entry in entries {
            if entry.flags & META_FLAG_DELETED == 0 {
                objects.push(entry.to_object_info());
            }
        }
        Ok(objects)
    }

    /// 获取存储系统的当前运行状态。
    ///
    /// # 返回值
    /// 返回一个 `StorageStatus` 结构体，包含块使用情况、
    /// 容量信息和对象数量等统计信息。
    pub fn status(&self) -> StorageStatus {
        let block_size = self.super_block.block_size as u64;
        let total_capacity = self.super_block.total_blocks * block_size;
        let used_capacity = (self.super_block.total_blocks - self.super_block.free_blocks)
            * block_size;
        let free_capacity = self.super_block.free_blocks * block_size;

        StorageStatus {
            total_blocks: self.super_block.total_blocks,
            free_blocks: self.super_block.free_blocks,
            used_blocks: self.super_block.total_blocks - self.super_block.free_blocks,
            block_size: self.super_block.block_size,
            object_count: self.super_block.object_count,
            max_objects: self.super_block.max_metadata_area_size / 256,
            metadata_area_size: self.super_block.metadata_area_size,
            max_metadata_area_size: self.super_block.max_metadata_area_size,
            store_path: String::new(), // 由调用方填充
            total_capacity,
            used_capacity,
            free_capacity,
        }
    }

    /// 将所有已修改的数据刷新到磁盘。
    ///
    /// 包括超级块和空闲块位图的持久化写入。
    ///
    /// # 返回值
    /// 成功时返回 `Ok(())`，发生 I/O 错误时返回错误。
    pub fn flush(&mut self) -> Result<()> {
        if self.dirty {
            // 写入超级块
            let sb_bytes = self.super_block.to_bytes();
            self.writer.seek(SeekFrom::Start(0))?;
            self.writer.write_all(&sb_bytes)?;

            // 写入位图
            self.writer.seek(SeekFrom::Start(Self::bitmap_offset()))?;
            self.writer.write_all(&self.bitmap)?;

            self.writer.flush()?;
            self.dirty = false;
        }
        Ok(())
    }

    // --- 内部辅助方法 ---

    /// 设置空闲块位图中的某一位。
    /// `used = true` 表示该块已被分配。
    fn set_bitmap_bit(&mut self, block_num: u64, used: bool) -> Result<()> {
        if block_num >= self.super_block.total_blocks {
            return Err(MiniOsError::Storage(format!(
                "块号 {} 超出范围（最大值为 {}）",
                block_num, self.super_block.total_blocks
            )));
        }

        let byte_idx = (block_num / 8) as usize;
        let bit_idx = (block_num % 8) as u8;

        if used {
            self.bitmap[byte_idx] |= 1 << bit_idx;
        } else {
            self.bitmap[byte_idx] &= !(1 << bit_idx);
        }

        // 同时立即写入文件以保持一致性
        let offset = Self::bitmap_offset() + byte_idx as u64;
        self.writer.seek(SeekFrom::Start(offset))?;
        self.writer.write_all(&[self.bitmap[byte_idx]])?;

        Ok(())
    }

    /// 检查指定块是否已被使用
    fn is_block_used(&self, block_num: u64) -> bool {
        if block_num >= self.super_block.total_blocks {
            return true; // 超出范围视为"已使用"
        }
        let byte_idx = (block_num / 8) as usize;
        let bit_idx = (block_num % 8) as u8;
        self.bitmap[byte_idx] & (1 << bit_idx) != 0
    }

    /// 分配 `count` 个连续的空闲块，并返回块号列表
    fn allocate_blocks(&mut self, count: u64) -> Result<Vec<u64>> {
        let mut allocated = Vec::with_capacity(count as usize);
        let total = self.super_block.total_blocks;

        let mut consecutive = 0;
        let mut start = 0u64;

        for block_num in 0..total {
            if !self.is_block_used(block_num) {
                if consecutive == 0 {
                    start = block_num;
                }
                consecutive += 1;
                if consecutive == count {
                    // 找到了足够的连续块
                    for b in start..start + count {
                        self.set_bitmap_bit(b, true)?;
                        allocated.push(b);
                    }
                    self.super_block.free_blocks -= count;
                    self.dirty = true;
                    return Ok(allocated);
                }
            } else {
                consecutive = 0;
            }
        }

        // 没有足够的连续块，尝试非连续分配
        // （回退策略：任意找空闲块即可）
        allocated.clear();
        for block_num in 0..total {
            if !self.is_block_used(block_num) {
                self.set_bitmap_bit(block_num, true)?;
                allocated.push(block_num);
                if allocated.len() as u64 == count {
                    self.super_block.free_blocks -= count;
                    self.dirty = true;
                    return Ok(allocated);
                }
            }
        }

        // 如果已检查过空闲块数，理论上不会到达这里
        Err(MiniOsError::NoSpace)
    }

    /// 根据 UUID 或名称查找对象条目（先尝试 UUID 匹配，再尝试名称匹配）
    fn find_entry(&self, key: &str) -> Result<Option<MetadataEntry>> {
        self.find_entry_with_offset(key)
            .map(|opt| opt.map(|(entry, _)| entry))
    }

    /// 根据 key 查找对象条目，返回 (条目, 文件中的偏移量)
    fn find_entry_with_offset(&self, key: &str) -> Result<Option<(MetadataEntry, u64)>> {
        // 优先尝试按 UUID 解析
        let uuid_key = uuid::Uuid::parse_str(key).ok();

        let entries = self.scan_all_entries_with_offsets()?;
        for (entry, offset) in entries {
            if entry.flags & META_FLAG_DELETED != 0 {
                continue;
            }
            if let Some(ref uk) = uuid_key {
                if entry.uuid == *uk {
                    return Ok(Some((entry, offset)));
                }
            }
            if entry.name == key {
                return Ok(Some((entry, offset)));
            }
        }
        Ok(None)
    }

    /// 仅按名称查找对象（若条目存在且未被删除则返回）
    fn find_by_name(&self, name: &str) -> Option<MetadataEntry> {
        let entries = self.scan_all_entries().ok()?;
        entries.into_iter().find(|e| {
            e.flags & META_FLAG_DELETED == 0 && e.name == name
        })
    }

    /// 扫描所有元数据条目（不含偏移量）
    fn scan_all_entries(&self) -> Result<Vec<MetadataEntry>> {
        self.scan_all_entries_with_offsets()
            .map(|v| v.into_iter().map(|(e, _)| e).collect())
    }

    /// 扫描所有元数据条目及其在文件中的偏移量
    fn scan_all_entries_with_offsets(&self) -> Result<Vec<(MetadataEntry, u64)>> {
        let mut entries = Vec::new();
        let metadata_offset = self.metadata_offset();
        let metadata_size = self.super_block.metadata_area_size;

        if metadata_size == 0 {
            return Ok(entries);
        }

        // 读取整个元数据区
        let mut buf = vec![0u8; metadata_size as usize];
        self.reader.borrow_mut().seek(SeekFrom::Start(metadata_offset))?;
        self.reader.borrow_mut().read_exact(&mut buf)?;

        let mut offset = 0u64;
        while offset < metadata_size {
            let slice = &buf[offset as usize..];
            // 尝试解析一个条目；若失败，说明已到达末尾或数据已损坏
            match MetadataEntry::from_bytes(slice) {
                Ok((entry, entry_size)) => {
                    let file_offset = metadata_offset + offset;
                    entries.push((entry, file_offset));
                    offset += entry_size as u64;
                }
                Err(_) => {
                    // 可能是填充数据或是元数据末尾；停止扫描
                    break;
                }
            }
        }

        Ok(entries)
    }

    /// 将一个元数据条目写入元数据区。
    ///
    /// 始终追加到元数据区的末尾。已删除的条目保留在原位，
    /// 扫描时通过其标志位跳过。故意不复用已删除条目的空间，
    /// 因为当新条目更小时，旧条目的残留垃圾字节可能会破坏顺序扫描的正确性。
    fn write_metadata_entry(&mut self, entry_bytes: &[u8]) -> Result<()> {
        let metadata_offset = self.metadata_offset();
        let entry_size = entry_bytes.len() as u64;

        // 始终追加到元数据区末尾
        let write_offset = metadata_offset + self.super_block.metadata_area_size;

        // 检查是否有足够空间
        if self.super_block.metadata_area_size + entry_size
            > self.super_block.max_metadata_area_size
        {
            return Err(MiniOsError::Storage(
                "元数据区已满：无法存储更多对象".to_string(),
            ));
        }
        self.super_block.metadata_area_size += entry_size;

        // 写入条目
        self.writer.seek(SeekFrom::Start(write_offset))?;
        self.writer.write_all(entry_bytes)?;
        self.dirty = true;

        Ok(())
    }
}

impl Drop for ObjectStorage {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

// ============================================================================
// 线程安全的包装器
// ============================================================================

/// 线程安全的共享存储句柄类型别名
///
/// 通过 `Arc<RwLock<>>` 包装 `ObjectStorage`，
/// 允许多个线程安全地共享和并发访问同一个存储实例。
/// 多个读线程可以同时持有锁，写操作需要独占访问。
pub type SharedStorage = Arc<RwLock<ObjectStorage>>;

/// 创建一个新的线程安全的存储实例。
///
/// 该函数会打开（或创建）指定路径的存储文件，
/// 并将其包装在 `Arc<RwLock<ObjectStorage>>` 中以支持多线程并发访问。
///
/// # 参数
/// * `path` - 存储文件的路径
/// * `block_size` - 每个数据块的大小（字节数）
/// * `total_blocks` - 文件系统中数据块的总数
/// * `max_objects` - 最大可存储的对象数量
///
/// # 返回值
/// 成功时返回一个线程安全的 `SharedStorage` 句柄，
/// 失败时返回错误。
pub fn create_storage(
    path: &str,
    block_size: u32,
    total_blocks: u64,
    max_objects: u64,
) -> Result<SharedStorage> {
    let storage = ObjectStorage::open(path, block_size, total_blocks, max_objects)?;
    Ok(Arc::new(RwLock::new(storage)))
}

use crate::error::{MiniOsError, Result};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

// ============================================================================
// Constants
// ============================================================================

/// Magic number for store.odb file identification
const MAGIC: &[u8; 4] = b"MOS\0";
/// Current file format version
const VERSION: u32 = 1;
/// Super block is always the first block (4096 bytes)
const SUPER_BLOCK_SIZE: u64 = 4096;
/// How many blocks the super block occupies
const SUPER_BLOCK_COUNT: u64 = 1;

// ============================================================================
// Data Structures
// ============================================================================

/// Information about a stored object (returned to clients)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectInfo {
    pub uuid: String,
    pub name: String,
    pub size: u64,
    pub content_type: String,
    pub created_at: String,
    pub tags: String,
    pub block_count: u32,
}

/// Storage engine status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStatus {
    pub total_blocks: u64,
    pub free_blocks: u64,
    pub used_blocks: u64,
    pub block_size: u32,
    pub object_count: u64,
    pub max_objects: u64,
    pub metadata_area_size: u64,
    pub max_metadata_area_size: u64,
    pub store_path: String,
    pub total_capacity: u64,
    pub used_capacity: u64,
    pub free_capacity: u64,
}

// ============================================================================
// Super Block (4096 bytes at file offset 0)
// ============================================================================

/// On-disk layout of the super block (4096 bytes)
#[derive(Debug, Clone)]
struct SuperBlock {
    magic: [u8; 4],            // offset 0
    version: u32,              // offset 4
    block_size: u32,           // offset 8
    total_blocks: u64,         // offset 12
    free_blocks: u64,          // offset 20
    object_count: u64,         // offset 28
    metadata_area_size: u64,   // offset 36
    max_metadata_area_size: u64, // offset 44
    bitmap_size: u64,          // offset 52
    created_at: i64,           // offset 60
    flags: u32,                // offset 68
    // padding to 4096
}

impl SuperBlock {
    fn new(block_size: u32, total_blocks: u64, max_metadata_area_size: u64) -> Self {
        let bitmap_size = (total_blocks + 7) / 8;
        // Align bitmap to 8-byte boundary
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

    /// Serialize super block to raw bytes (4096 bytes)
    fn to_bytes(&self) -> [u8; SUPER_BLOCK_SIZE as usize] {
        let mut buf = [0u8; SUPER_BLOCK_SIZE as usize];
        let mut offset = 0;

        // magic (4 bytes)
        buf[offset..offset + 4].copy_from_slice(&self.magic);
        offset += 4;

        // version (4 bytes, little-endian)
        buf[offset..offset + 4].copy_from_slice(&self.version.to_le_bytes());
        offset += 4;

        // block_size (4 bytes)
        buf[offset..offset + 4].copy_from_slice(&self.block_size.to_le_bytes());
        offset += 4;

        // total_blocks (8 bytes)
        buf[offset..offset + 8].copy_from_slice(&self.total_blocks.to_le_bytes());
        offset += 8;

        // free_blocks (8 bytes)
        buf[offset..offset + 8].copy_from_slice(&self.free_blocks.to_le_bytes());
        offset += 8;

        // object_count (8 bytes)
        buf[offset..offset + 8].copy_from_slice(&self.object_count.to_le_bytes());
        offset += 8;

        // metadata_area_size (8 bytes)
        buf[offset..offset + 8].copy_from_slice(&self.metadata_area_size.to_le_bytes());
        offset += 8;

        // max_metadata_area_size (8 bytes)
        buf[offset..offset + 8].copy_from_slice(&self.max_metadata_area_size.to_le_bytes());
        offset += 8;

        // bitmap_size (8 bytes)
        buf[offset..offset + 8].copy_from_slice(&self.bitmap_size.to_le_bytes());
        offset += 8;

        // created_at (8 bytes)
        buf[offset..offset + 8].copy_from_slice(&self.created_at.to_le_bytes());
        offset += 8;

        // flags (4 bytes)
        buf[offset..offset + 4].copy_from_slice(&self.flags.to_le_bytes());
        // offset += 4; // not needed, rest is padding

        buf
    }

    /// Deserialize super block from raw bytes
    fn from_bytes(buf: &[u8; SUPER_BLOCK_SIZE as usize]) -> Result<Self> {
        let mut offset = 0;

        let mut magic = [0u8; 4];
        magic.copy_from_slice(&buf[offset..offset + 4]);
        offset += 4;

        if &magic != MAGIC {
            return Err(MiniOsError::Storage(
                "Invalid magic number: not a MiniOS store file".to_string(),
            ));
        }

        let version = u32::from_le_bytes([
            buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3],
        ]);
        offset += 4;

        if version != VERSION {
            return Err(MiniOsError::Storage(format!(
                "Unsupported version: {} (expected {})",
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
// Metadata Entry (variable-length, stored in metadata area)
// ============================================================================

/// On-disk metadata entry flags
const META_FLAG_DELETED: u8 = 0x01;

/// A metadata entry in the metadata area
#[derive(Debug, Clone)]
struct MetadataEntry {
    uuid: uuid::Uuid,
    flags: u8,
    name: String,
    size: u64,
    content_type: String,
    created_at: i64,
    tags: String,
    block_pointers: Vec<u64>,
    /// Total on-disk size of this entry (including this field)
    entry_size: u32,
}

impl MetadataEntry {
    /// Create a new metadata entry
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
            entry_size: 0, // calculated on serialization
        }
    }

    /// Calculate the on-disk size of this entry
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

    /// Serialize to bytes
    fn to_bytes(&self) -> Vec<u8> {
        let entry_size = self.calculate_entry_size();
        let mut buf = Vec::with_capacity(entry_size as usize);

        // uuid (16 bytes)
        buf.extend_from_slice(self.uuid.as_bytes());

        // flags (1 byte)
        buf.push(self.flags);

        // name_len (2 bytes, LE)
        buf.extend_from_slice(&(self.name.len() as u16).to_le_bytes());

        // name (variable)
        buf.extend_from_slice(self.name.as_bytes());

        // size (8 bytes, LE)
        buf.extend_from_slice(&self.size.to_le_bytes());

        // content_type_len (2 bytes, LE)
        buf.extend_from_slice(&(self.content_type.len() as u16).to_le_bytes());

        // content_type (variable)
        buf.extend_from_slice(self.content_type.as_bytes());

        // created_at (8 bytes, LE)
        buf.extend_from_slice(&self.created_at.to_le_bytes());

        // tags_len (2 bytes, LE)
        buf.extend_from_slice(&(self.tags.len() as u16).to_le_bytes());

        // tags (variable)
        buf.extend_from_slice(self.tags.as_bytes());

        // block_count (4 bytes, LE)
        buf.extend_from_slice(&(self.block_pointers.len() as u32).to_le_bytes());

        // block_pointers (8 bytes each, LE)
        for &ptr in &self.block_pointers {
            buf.extend_from_slice(&ptr.to_le_bytes());
        }

        // entry_size (4 bytes, LE)
        buf.extend_from_slice(&entry_size.to_le_bytes());

        buf
    }

    /// Deserialize from bytes, returns (entry, bytes_consumed)
    fn from_bytes(data: &[u8]) -> Result<(Self, u32)> {
        if data.len() < 16 + 1 + 2 + 8 + 2 + 8 + 2 + 4 + 4 {
            return Err(MiniOsError::Storage(
                "Metadata entry too short".to_string(),
            ));
        }

        let mut offset = 0;

        // uuid (16 bytes)
        let uuid = uuid::Uuid::from_slice(&data[offset..offset + 16]).map_err(|e| {
            MiniOsError::Storage(format!("Invalid UUID in metadata: {}", e))
        })?;
        offset += 16;

        // flags (1 byte)
        let flags = data[offset];
        offset += 1;

        // name_len (2 bytes, LE)
        let name_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;

        // name (variable)
        if offset + name_len > data.len() {
            return Err(MiniOsError::Storage("Corrupt metadata: name".to_string()));
        }
        let name = std::str::from_utf8(&data[offset..offset + name_len])?.to_string();
        offset += name_len;

        // size (8 bytes, LE)
        let size = u64::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
            data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
        ]);
        offset += 8;

        // content_type_len (2 bytes, LE)
        let ct_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;

        // content_type (variable)
        if offset + ct_len > data.len() {
            return Err(MiniOsError::Storage(
                "Corrupt metadata: content_type".to_string(),
            ));
        }
        let content_type = std::str::from_utf8(&data[offset..offset + ct_len])?.to_string();
        offset += ct_len;

        // created_at (8 bytes, LE)
        let created_at = i64::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
            data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
        ]);
        offset += 8;

        // tags_len (2 bytes, LE)
        let tags_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;

        // tags (variable)
        if offset + tags_len > data.len() {
            return Err(MiniOsError::Storage("Corrupt metadata: tags".to_string()));
        }
        let tags = std::str::from_utf8(&data[offset..offset + tags_len])?.to_string();
        offset += tags_len;

        // block_count (4 bytes, LE)
        let block_count = u32::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
        ]) as usize;
        offset += 4;

        // block_pointers (8 bytes each, LE)
        let mut block_pointers = Vec::with_capacity(block_count);
        for _ in 0..block_count {
            if offset + 8 > data.len() {
                return Err(MiniOsError::Storage(
                    "Corrupt metadata: block_pointers".to_string(),
                ));
            }
            let ptr = u64::from_le_bytes([
                data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
                data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
            ]);
            block_pointers.push(ptr);
            offset += 8;
        }

        // entry_size (4 bytes, LE)
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

    /// Convert to ObjectInfo for external API
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
// Object Storage Engine
// ============================================================================

/// The main object storage engine managing the store.odb file
pub struct ObjectStorage {
    file: File,
    super_block: SuperBlock,
    /// In-memory copy of the free block bitmap
    bitmap: Vec<u8>,
    dirty: bool,
}

impl ObjectStorage {
    // --- Offsets ---

    /// Offset where the bitmap starts
    fn bitmap_offset() -> u64 {
        SUPER_BLOCK_SIZE
    }

    /// Offset where the metadata area starts
    fn metadata_offset(&self) -> u64 {
        SUPER_BLOCK_SIZE + self.super_block.bitmap_size
    }

    /// Offset where the data area starts
    fn data_offset(&self) -> u64 {
        SUPER_BLOCK_SIZE
            + self.super_block.bitmap_size
            + self.super_block.max_metadata_area_size
    }

    /// Offset of a specific data block
    fn block_offset(&self, block_num: u64) -> u64 {
        self.data_offset() + block_num * self.super_block.block_size as u64
    }

    // --- Public API ---

    /// Open an existing store file or create a new one
    pub fn open(
        path: &str,
        block_size: u32,
        total_blocks: u64,
        max_objects: u64,
    ) -> Result<Self> {
        // Estimate max metadata area size based on max objects
        // Average entry size ~200 bytes
        let max_metadata_area_size = max_objects * 256;

        let path = Path::new(path);
        let exists = path.exists();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        if exists && file.metadata()?.len() > 0 {
            // Read existing super block
            let mut sb_buf = [0u8; SUPER_BLOCK_SIZE as usize];
            let mut f = &file;
            f.seek(SeekFrom::Start(0))?;
            f.read_exact(&mut sb_buf)?;
            let super_block = SuperBlock::from_bytes(&sb_buf)?;

            // Validate parameters match
            if super_block.block_size != block_size {
                return Err(MiniOsError::Storage(format!(
                    "Block size mismatch: file has {}, requested {}",
                    super_block.block_size, block_size
                )));
            }

            // Read bitmap into memory
            let mut bitmap = vec![0u8; super_block.bitmap_size as usize];
            let mut f = &file;
            f.seek(SeekFrom::Start(Self::bitmap_offset()))?;
            f.read_exact(&mut bitmap)?;

            Ok(Self {
                file,
                super_block,
                bitmap,
                dirty: false,
            })
        } else {
            // Create new store file
            let super_block = SuperBlock::new(block_size, total_blocks, max_metadata_area_size);
            let bitmap = vec![0u8; super_block.bitmap_size as usize];

            let mut storage = Self {
                file,
                super_block,
                bitmap,
                dirty: true,
            };

            // Initialize the file layout
            storage.init_file()?;
            storage.flush()?;

            Ok(storage)
        }
    }

    /// Initialize a new store file (write super block, bitmap, empty metadata area)
    fn init_file(&mut self) -> Result<()> {
        // Calculate total file size
        let file_size = self.data_offset()
            + self.super_block.total_blocks * self.super_block.block_size as u64;

        // Truncate (or pre-allocate) the file
        self.file.set_len(file_size)?;

        // Write super block
        let sb_bytes = self.super_block.to_bytes();
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&sb_bytes)?;

        // Write initial bitmap (all zeros = all free, except super block area)
        // Mark blocks used by super block, bitmap, and metadata area as used
        let reserved_blocks =
            (self.data_offset() + self.super_block.block_size as u64 - 1)
                / self.super_block.block_size as u64;
        for i in 0..reserved_blocks {
            self.set_bitmap_bit(i, true)?;
        }

        // Write bitmap
        self.file.seek(SeekFrom::Start(Self::bitmap_offset()))?;
        self.file.write_all(&self.bitmap)?;

        self.file.flush()?;
        self.dirty = true;

        Ok(())
    }

    /// Put an object into the store
    /// Returns the object info (including generated UUID)
    pub fn put(
        &mut self,
        name: &str,
        data: &[u8],
        content_type: &str,
        tags: &str,
    ) -> Result<ObjectInfo> {
        // Check for duplicate name
        if self.find_by_name(name).is_some() {
            return Err(MiniOsError::AlreadyExists(format!(
                "Object with name '{}' already exists",
                name
            )));
        }

        let data_len = data.len() as u64;
        let block_size = self.super_block.block_size as u64;
        let blocks_needed = if data_len == 0 {
            1 // Store empty objects in 1 block
        } else {
            (data_len + block_size - 1) / block_size
        };

        // Check free space
        if self.super_block.free_blocks < blocks_needed {
            return Err(MiniOsError::NoSpace);
        }

        // Allocate blocks
        let block_pointers = self.allocate_blocks(blocks_needed)?;

        // Write data to allocated blocks
        for (i, &block_num) in block_pointers.iter().enumerate() {
            let offset = self.block_offset(block_num);
            let start = i * block_size as usize;
            let end = std::cmp::min(start + block_size as usize, data.len());
            let chunk = &data[start..end];

            self.file.seek(SeekFrom::Start(offset))?;
            self.file.write_all(chunk)?;

            // Zero-fill the rest of the block if data doesn't fill it
            if chunk.len() < block_size as usize {
                let padding = vec![0u8; block_size as usize - chunk.len()];
                self.file.write_all(&padding)?;
            }
        }

        // Create metadata entry
        let entry = MetadataEntry::new(name, data_len, content_type, tags, block_pointers);
        let info = entry.to_object_info();
        let entry_bytes = entry.to_bytes();

        // Write metadata entry (try to reuse deleted entry space first)
        self.write_metadata_entry(&entry_bytes)?;

        // Update super block
        self.super_block.object_count += 1;
        self.dirty = true;

        // Flush to ensure subsequent reads see the latest state
        self.flush()?;

        Ok(info)
    }

    /// Find object metadata by UUID or name WITHOUT reading data blocks.
    /// Returns ObjectInfo only; use `get()` to also read the data.
    pub fn find_info(&mut self, key: &str) -> Result<ObjectInfo> {
        let entry = self
            .find_entry(key)?
            .ok_or_else(|| MiniOsError::NotFound(format!("Object not found: {}", key)))?;

        if entry.flags & META_FLAG_DELETED != 0 {
            return Err(MiniOsError::NotFound(format!("Object not found: {}", key)));
        }
        Ok(entry.to_object_info())
    }

    /// Get an object from the store by UUID or name
    /// Returns (ObjectInfo, data_bytes)
    pub fn get(&mut self, key: &str) -> Result<(ObjectInfo, Vec<u8>)> {
        // already &mut self — correct
        let entry = self
            .find_entry(key)?
            .ok_or_else(|| MiniOsError::NotFound(format!("Object not found: {}", key)))?;

        if entry.flags & META_FLAG_DELETED != 0 {
            return Err(MiniOsError::NotFound(format!("Object not found: {}", key)));
        }

        // Read data from blocks
        let block_size = self.super_block.block_size as usize;
        let mut data = Vec::with_capacity(entry.size as usize);

        for &block_num in &entry.block_pointers {
            let offset = self.block_offset(block_num);
            self.file.seek(SeekFrom::Start(offset))?;

            let mut chunk = vec![0u8; block_size];
            self.file.read_exact(&mut chunk)?;
            data.extend_from_slice(&chunk);
        }

        // Trim to actual size
        data.truncate(entry.size as usize);

        Ok((entry.to_object_info(), data))
    }

    /// Delete an object by UUID or name
    pub fn delete(&mut self, key: &str) -> Result<()> {
        // Find the entry offset in the metadata area
        let (entry, entry_offset) = self
            .find_entry_with_offset(key)?
            .ok_or_else(|| MiniOsError::NotFound(format!("Object not found: {}", key)))?;

        if entry.flags & META_FLAG_DELETED != 0 {
            return Err(MiniOsError::NotFound(format!("Object not found: {}", key)));
        }

        // Free data blocks
        for &block_num in &entry.block_pointers {
            self.set_bitmap_bit(block_num, false)?;
        }
        self.super_block.free_blocks += entry.block_pointers.len() as u64;

        // Mark metadata entry as deleted
        let flags_offset = entry_offset + 16; // uuid is 16 bytes, flags follows
        self.file.seek(SeekFrom::Start(flags_offset))?;
        self.file.write_all(&[META_FLAG_DELETED])?;

        // Update super block
        self.super_block.object_count -= 1;
        self.dirty = true;

        // Flush to ensure subsequent reads see the latest state
        self.flush()?;

        Ok(())
    }

    /// List all objects
    pub fn list(&mut self) -> Result<Vec<ObjectInfo>> {
        let mut objects = Vec::new();
        let entries = self.scan_all_entries()?;
        for entry in entries {
            if entry.flags & META_FLAG_DELETED == 0 {
                objects.push(entry.to_object_info());
            }
        }
        Ok(objects)
    }

    /// Get storage status
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
            store_path: String::new(), // filled in by caller
            total_capacity,
            used_capacity,
            free_capacity,
        }
    }

    /// Flush changes to disk
    pub fn flush(&mut self) -> Result<()> {
        if self.dirty {
            // Write super block
            let sb_bytes = self.super_block.to_bytes();
            self.file.seek(SeekFrom::Start(0))?;
            self.file.write_all(&sb_bytes)?;

            // Write bitmap
            self.file.seek(SeekFrom::Start(Self::bitmap_offset()))?;
            self.file.write_all(&self.bitmap)?;

            self.file.flush()?;
            self.dirty = false;
        }
        Ok(())
    }

    // --- Internal Helpers ---

    /// Set a bit in the free block bitmap
    /// `used = true` means the block is allocated
    fn set_bitmap_bit(&mut self, block_num: u64, used: bool) -> Result<()> {
        if block_num >= self.super_block.total_blocks {
            return Err(MiniOsError::Storage(format!(
                "Block number {} out of range (max {})",
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

        // Also write to file immediately for consistency
        let offset = Self::bitmap_offset() + byte_idx as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&[self.bitmap[byte_idx]])?;

        Ok(())
    }

    /// Check if a block is used
    fn is_block_used(&self, block_num: u64) -> bool {
        if block_num >= self.super_block.total_blocks {
            return true; // out of range = "used"
        }
        let byte_idx = (block_num / 8) as usize;
        let bit_idx = (block_num % 8) as u8;
        self.bitmap[byte_idx] & (1 << bit_idx) != 0
    }

    /// Allocate `count` contiguous free blocks, returns block numbers
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
                    // Found enough contiguous blocks
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

        // Not enough contiguous blocks, try non-contiguous allocation
        // (fallback: just find any free blocks)
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

        // Shouldn't reach here if we checked free_blocks
        Err(MiniOsError::NoSpace)
    }

    /// Find an object entry by UUID or name (tries UUID first, then name)
    fn find_entry(&mut self, key: &str) -> Result<Option<MetadataEntry>> {
        self.find_entry_with_offset(key)
            .map(|opt| opt.map(|(entry, _)| entry))
    }

    /// Find an object entry by key, returning (entry, file_offset_of_entry)
    fn find_entry_with_offset(&mut self, key: &str) -> Result<Option<(MetadataEntry, u64)>> {
        // Try parsing as UUID first
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

    /// Find an object by name only (returns entry if exists and not deleted)
    fn find_by_name(&mut self, name: &str) -> Option<MetadataEntry> {
        let entries = self.scan_all_entries().ok()?;
        entries.into_iter().find(|e| {
            e.flags & META_FLAG_DELETED == 0 && e.name == name
        })
    }

    /// Scan all metadata entries (without offsets)
    fn scan_all_entries(&mut self) -> Result<Vec<MetadataEntry>> {
        self.scan_all_entries_with_offsets()
            .map(|v| v.into_iter().map(|(e, _)| e).collect())
    }

    /// Scan all metadata entries with their file offsets
    fn scan_all_entries_with_offsets(&mut self) -> Result<Vec<(MetadataEntry, u64)>> {
        let mut entries = Vec::new();
        let metadata_offset = self.metadata_offset();
        let metadata_size = self.super_block.metadata_area_size;

        if metadata_size == 0 {
            return Ok(entries);
        }

        // Read the entire metadata area
        let mut buf = vec![0u8; metadata_size as usize];
        self.file.seek(SeekFrom::Start(metadata_offset))?;
        self.file.read_exact(&mut buf)?;

        let mut offset = 0u64;
        while offset < metadata_size {
            let slice = &buf[offset as usize..];
            // Try to parse an entry; if it fails, we're at the end or data is corrupt
            match MetadataEntry::from_bytes(slice) {
                Ok((entry, entry_size)) => {
                    let file_offset = metadata_offset + offset;
                    entries.push((entry, file_offset));
                    offset += entry_size as u64;
                }
                Err(_) => {
                    // Could be padding/end of metadata; stop scanning
                    break;
                }
            }
        }

        Ok(entries)
    }

    /// Write a metadata entry to the metadata area
    /// Tries to reuse space from a deleted entry first
    fn write_metadata_entry(&mut self, entry_bytes: &[u8]) -> Result<()> {
        let metadata_offset = self.metadata_offset();
        let entry_size = entry_bytes.len() as u64;

        // Try to find a deleted entry with enough space
        let mut reuse_offset: Option<u64> = None;
        let entries = self.scan_all_entries_with_offsets()?;
        for (entry, offset) in entries {
            if entry.flags & META_FLAG_DELETED != 0
                && entry.entry_size as u64 >= entry_size
            {
                reuse_offset = Some(offset);
                break;
            }
        }

        let write_offset = if let Some(off) = reuse_offset {
            off
        } else {
            // Append at end of metadata area
            let off = metadata_offset + self.super_block.metadata_area_size;
            // Check if we have space
            if self.super_block.metadata_area_size + entry_size
                > self.super_block.max_metadata_area_size
            {
                return Err(MiniOsError::Storage(
                    "Metadata area full: cannot store more objects".to_string(),
                ));
            }
            self.super_block.metadata_area_size += entry_size;
            off
        };

        // Write the entry
        self.file.seek(SeekFrom::Start(write_offset))?;
        self.file.write_all(entry_bytes)?;
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
// Thread-safe wrapper
// ============================================================================

pub type SharedStorage = Arc<Mutex<ObjectStorage>>;

/// Create a new thread-safe storage instance
pub fn create_storage(
    path: &str,
    block_size: u32,
    total_blocks: u64,
    max_objects: u64,
) -> Result<SharedStorage> {
    let storage = ObjectStorage::open(path, block_size, total_blocks, max_objects)?;
    Ok(Arc::new(Mutex::new(storage)))
}

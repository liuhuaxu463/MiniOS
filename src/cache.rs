use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Cached object data along with its metadata
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CachedObject {
    pub uuid: String,
    pub data: Vec<u8>,
    pub name: String,
    pub content_type: String,
    pub size: u64,
    pub tags: String,
}

/// LRU cache statistics
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    #[allow(dead_code)]
    pub evictions: u64,
    pub size: usize,
    pub capacity: usize,
}

impl CacheStats {
    /// Hit rate as a percentage (0.0 - 100.0)
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            (self.hits as f64 / total as f64) * 100.0
        }
    }
}

/// Thread-safe LRU cache for frequently accessed objects
///
/// Caches objects by their UUID. Uses the `lru` crate internally.
pub struct ObjectCache {
    cache: Mutex<LruCache<String, CachedObject>>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    capacity: usize,
}

impl ObjectCache {
    /// Create a new cache with the given capacity
    pub fn new(capacity: usize) -> Self {
        let cap = if capacity == 0 {
            NonZeroUsize::new(1).unwrap()
        } else {
            NonZeroUsize::new(capacity).unwrap()
        };
        Self {
            cache: Mutex::new(LruCache::new(cap)),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            capacity,
        }
    }

    /// Get an object from the cache by UUID
    /// Returns None on cache miss
    pub fn get(&self, uuid: &str) -> Option<CachedObject> {
        let mut cache = self.cache.lock().unwrap();
        match cache.get(uuid) {
            Some(obj) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(obj.clone())
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Put an object into the cache
    /// Returns the evicted object's UUID if eviction occurred
    pub fn put(&self, uuid: &str, obj: CachedObject) -> Option<(String, CachedObject)> {
        let mut cache = self.cache.lock().unwrap();
        let evicted = cache.push(uuid.to_string(), obj);
        if evicted.is_some() {
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
        evicted
    }

    /// Remove an object from the cache
    pub fn remove(&self, uuid: &str) -> Option<CachedObject> {
        let mut cache = self.cache.lock().unwrap();
        cache.pop(uuid)
    }

    /// Check if an object is in the cache
    pub fn contains(&self, uuid: &str) -> bool {
        let cache = self.cache.lock().unwrap();
        cache.contains(uuid)
    }

    /// Get the current cache size (number of cached items)
    pub fn len(&self) -> usize {
        let cache = self.cache.lock().unwrap();
        cache.len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get cache capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            size: self.len(),
            capacity: self.capacity,
        }
    }

    /// Clear all entries from the cache
    pub fn clear(&self) {
        let mut cache = self.cache.lock().unwrap();
        cache.clear();
    }

    /// Warm up the cache by loading objects from a list of (uuid, data, metadata) tuples
    pub fn warm_up(&self, objects: Vec<(String, CachedObject)>) {
        let mut cache = self.cache.lock().unwrap();
        for (uuid, obj) in objects {
            cache.push(uuid, obj);
        }
    }

    /// Reset statistics counters
    pub fn reset_stats(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
    }
}

/// Helper to convert bytes to a human-readable size string
pub fn human_readable_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;
    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }
    if unit_idx == 0 {
        format!("{} {}", bytes, UNITS[unit_idx])
    } else {
        format!("{:.2} {}", size, UNITS[unit_idx])
    }
}

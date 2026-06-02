use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::collections::{VecDeque, HashMap};
use std::num::NonZeroUsize;

/// Cached object data along with its metadata
#[derive(Debug, Clone)]
pub struct CachedObject {
    pub uuid: String,
    pub data: Vec<u8>,
    pub name: String,
    pub content_type: String,
    pub size: u64,
    pub tags: String,
}

/// Cache replacement algorithms
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheAlgorithmType {
    /// Least Recently Used — evicts the item accessed least recently
    Lru,
    /// First-In First-Out — evicts the oldest inserted item
    Fifo,
    /// Least Frequently Used — evicts the item with the lowest access count
    Lfu,
}

impl CacheAlgorithmType {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "fifo" => Self::Fifo,
            "lfu" => Self::Lfu,
            _ => Self::Lru,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Lru => "LRU",
            Self::Fifo => "FIFO",
            Self::Lfu => "LFU",
        }
    }

    pub fn all() -> &'static [Self] {
        &[Self::Lru, Self::Fifo, Self::Lfu]
    }
}

// ============================================================================
// Cache Statistics
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub size: usize,
    pub capacity: usize,
    pub algorithm: String,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 { 0.0 } else { (self.hits as f64 / total as f64) * 100.0 }
    }
}

/// Per-algorithm benchmark snapshot for comparison
#[derive(Debug, Clone)]
pub struct AlgorithmBenchmark {
    pub algorithm: String,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub hit_rate: f64,
    pub final_size: usize,
}

// ============================================================================
// Internal cache implementations
// ============================================================================

/// FIFO cache: VecDeque + HashMap for O(1) lookup and O(1) eviction order.
struct FifoCache {
    capacity: usize,
    order: VecDeque<String>,
    map: HashMap<String, CachedObject>,
}

impl FifoCache {
    fn new(capacity: usize) -> Self {
        Self { capacity: capacity.max(1), order: VecDeque::with_capacity(capacity), map: HashMap::new() }
    }

    fn get(&mut self, key: &str) -> Option<&CachedObject> {
        self.map.get(key)
    }

    fn put(&mut self, key: &str, value: CachedObject) -> Option<(String, CachedObject)> {
        let mut evicted = None;
        if self.map.contains_key(key) {
            // Update existing — remove from order list and re-insert at back
            self.order.retain(|k| k != key);
            self.order.push_back(key.to_string());
            self.map.insert(key.to_string(), value);
        } else {
            if self.map.len() >= self.capacity {
                if let Some(old_key) = self.order.pop_front() {
                    if let Some(old_val) = self.map.remove(&old_key) {
                        evicted = Some((old_key, old_val));
                    }
                }
            }
            self.order.push_back(key.to_string());
            self.map.insert(key.to_string(), value);
        }
        evicted
    }

    fn remove(&mut self, key: &str) -> Option<CachedObject> {
        self.order.retain(|k| k != key);
        self.map.remove(key)
    }

    fn contains(&self, key: &str) -> bool { self.map.contains_key(key) }
    fn len(&self) -> usize { self.map.len() }
    fn capacity(&self) -> usize { self.capacity }
    fn resize(&mut self, new_cap: usize) {
        self.capacity = new_cap.max(1);
        while self.map.len() > self.capacity {
            if let Some(old_key) = self.order.pop_front() {
                self.map.remove(&old_key);
            } else { break; }
        }
    }
    fn clear(&mut self) { self.order.clear(); self.map.clear(); }
}

/// LFU cache: tracks access frequency, evicts lowest-frequency item.
struct LfuCache {
    capacity: usize,
    /// frequency[0] = access count, frequency[1] = insertion order (tie-breaker)
    freq: HashMap<String, (u64, u64)>,
    map: HashMap<String, CachedObject>,
    seq: u64,
}

impl LfuCache {
    fn new(capacity: usize) -> Self {
        Self { capacity: capacity.max(1), freq: HashMap::new(), map: HashMap::new(), seq: 0 }
    }

    fn get(&mut self, key: &str) -> Option<&CachedObject> {
        if self.map.contains_key(key) {
            if let Some(f) = self.freq.get_mut(key) {
                f.0 += 1;
                self.seq += 1;
                f.1 = self.seq;
            }
        }
        self.map.get(key)
    }

    fn put(&mut self, key: &str, value: CachedObject) -> Option<(String, CachedObject)> {
        let mut evicted = None;
        if self.map.contains_key(key) {
            if let Some(f) = self.freq.get_mut(key) {
                f.0 += 1;
                self.seq += 1;
                f.1 = self.seq;
            }
            self.map.insert(key.to_string(), value);
        } else {
            if self.map.len() >= self.capacity {
                // Find entry with lowest frequency (tie-break by oldest seq)
                if let Some(victim) = self.freq.iter()
                    .min_by(|(_, a), (_, b)| a.0.cmp(&b.0).then(a.1.cmp(&b.1)))
                    .map(|(k, _)| k.clone())
                {
                    self.freq.remove(&victim);
                    if let Some(v) = self.map.remove(&victim) {
                        evicted = Some((victim, v));
                    }
                }
            }
            self.seq += 1;
            self.freq.insert(key.to_string(), (1, self.seq));
            self.map.insert(key.to_string(), value);
        }
        evicted
    }

    fn remove(&mut self, key: &str) -> Option<CachedObject> {
        self.freq.remove(key);
        self.map.remove(key)
    }

    fn contains(&self, key: &str) -> bool { self.map.contains_key(key) }
    fn len(&self) -> usize { self.map.len() }
    fn capacity(&self) -> usize { self.capacity }
    fn resize(&mut self, new_cap: usize) {
        self.capacity = new_cap.max(1);
        while self.map.len() > self.capacity {
            if let Some(victim) = self.freq.iter()
                .min_by(|(_, a), (_, b)| a.0.cmp(&b.0).then(a.1.cmp(&b.1)))
                .map(|(k, _)| k.clone())
            {
                self.freq.remove(&victim);
                self.map.remove(&victim);
            } else { break; }
        }
    }
    fn clear(&mut self) { self.freq.clear(); self.map.clear(); self.seq = 0; }
}

/// LRU cache wrapper around the `lru` crate.
struct LruCacheImpl {
    inner: lru::LruCache<String, CachedObject>,
}

impl LruCacheImpl {
    fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).unwrap();
        Self { inner: lru::LruCache::new(cap) }
    }

    fn get(&mut self, key: &str) -> Option<&CachedObject> {
        self.inner.get(key)
    }

    fn put(&mut self, key: &str, value: CachedObject) -> Option<(String, CachedObject)> {
        self.inner.push(key.to_string(), value)
    }

    fn remove(&mut self, key: &str) -> Option<CachedObject> {
        self.inner.pop(key)
    }

    fn contains(&self, key: &str) -> bool { self.inner.contains(key) }
    fn len(&self) -> usize { self.inner.len() }
    fn capacity(&self) -> usize { self.inner.cap().get() }
    fn resize(&mut self, new_cap: usize) {
        let cap = NonZeroUsize::new(new_cap.max(1)).unwrap();
        self.inner.resize(cap);
    }
    fn clear(&mut self) { self.inner.clear(); }
}

// ============================================================================
// Unified ObjectCache (thread-safe, algorithm-swappable)
// ============================================================================

enum InnerCache {
    Lru(LruCacheImpl),
    Fifo(FifoCache),
    Lfu(LfuCache),
}

/// Thread-safe object cache. Supports LRU / FIFO / LFU algorithms
/// and dynamic capacity resizing at runtime.
pub struct ObjectCache {
    inner: Mutex<InnerCache>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    algorithm: CacheAlgorithmType,
}

impl ObjectCache {
    pub fn new(algorithm: CacheAlgorithmType, capacity: usize) -> Self {
        let inner = match algorithm {
            CacheAlgorithmType::Lru => InnerCache::Lru(LruCacheImpl::new(capacity)),
            CacheAlgorithmType::Fifo => InnerCache::Fifo(FifoCache::new(capacity)),
            CacheAlgorithmType::Lfu => InnerCache::Lfu(LfuCache::new(capacity)),
        };
        Self {
            inner: Mutex::new(inner),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            algorithm,
        }
    }

    pub fn get(&self, uuid: &str) -> Option<CachedObject> {
        let mut inner = self.inner.lock().unwrap();
        let found = match &mut *inner {
            InnerCache::Lru(c) => c.get(uuid).cloned(),
            InnerCache::Fifo(c) => c.get(uuid).cloned(),
            InnerCache::Lfu(c) => c.get(uuid).cloned(),
        };
        if found.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        found
    }

    pub fn put(&self, uuid: &str, obj: CachedObject) -> Option<(String, CachedObject)> {
        let mut inner = self.inner.lock().unwrap();
        let evicted = match &mut *inner {
            InnerCache::Lru(c) => c.put(uuid, obj),
            InnerCache::Fifo(c) => c.put(uuid, obj),
            InnerCache::Lfu(c) => c.put(uuid, obj),
        };
        if evicted.is_some() {
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
        evicted
    }

    pub fn remove(&self, uuid: &str) -> Option<CachedObject> {
        let mut inner = self.inner.lock().unwrap();
        match &mut *inner {
            InnerCache::Lru(c) => c.remove(uuid),
            InnerCache::Fifo(c) => c.remove(uuid),
            InnerCache::Lfu(c) => c.remove(uuid),
        }
    }

    pub fn len(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        match &*inner {
            InnerCache::Lru(c) => c.len(),
            InnerCache::Fifo(c) => c.len(),
            InnerCache::Lfu(c) => c.len(),
        }
    }

    pub fn capacity(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        match &*inner {
            InnerCache::Lru(c) => c.capacity(),
            InnerCache::Fifo(c) => c.capacity(),
            InnerCache::Lfu(c) => c.capacity(),
        }
    }

    /// Dynamically resize the cache at runtime.
    /// If shrinking, excess entries are evicted according to the current algorithm.
    pub fn resize(&self, new_capacity: usize) {
        let mut inner = self.inner.lock().unwrap();
        match &mut *inner {
            InnerCache::Lru(c) => c.resize(new_capacity),
            InnerCache::Fifo(c) => c.resize(new_capacity),
            InnerCache::Lfu(c) => c.resize(new_capacity),
        }
    }

    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        match &mut *inner {
            InnerCache::Lru(c) => c.clear(),
            InnerCache::Fifo(c) => c.clear(),
            InnerCache::Lfu(c) => c.clear(),
        }
    }

    pub fn warm_up(&self, objects: Vec<(String, CachedObject)>) {
        let mut inner = self.inner.lock().unwrap();
        for (uuid, obj) in objects {
            let evicted = match &mut *inner {
                InnerCache::Lru(c) => c.put(&uuid, obj),
                InnerCache::Fifo(c) => c.put(&uuid, obj),
                InnerCache::Lfu(c) => c.put(&uuid, obj),
            };
            if evicted.is_some() {
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            size: self.len(),
            capacity: self.capacity(),
            algorithm: self.algorithm.as_str().to_string(),
        }
    }

    pub fn algorithm(&self) -> CacheAlgorithmType {
        self.algorithm
    }

    pub fn reset_stats(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
    }

    // ---- Benchmark support ----

    /// Run a benchmark: simulate GET workload over a list of keys,
    /// return stats collected during this benchmark run.
    pub fn benchmark_run(&self, keys: &[String], preloaded: &[(String, CachedObject)]) -> AlgorithmBenchmark {
        self.clear();
        self.reset_stats();
        for (uuid, obj) in preloaded {
            self.put(uuid, obj.clone());
        }

        for key in keys {
            self.get(key);
        }

        let stats = self.stats();
        AlgorithmBenchmark {
            algorithm: stats.algorithm,
            hits: stats.hits,
            misses: stats.misses,
            evictions: stats.evictions,
            hit_rate: stats.hit_rate(),
            final_size: stats.size,
        }
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

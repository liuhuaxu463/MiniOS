use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::collections::{VecDeque, HashMap};
use std::num::NonZeroUsize;

/// 缓存对象数据及其元数据
#[derive(Debug, Clone)]
pub struct CachedObject {
    /// 对象的唯一标识符
    pub uuid: String,
    /// 对象的二进制数据内容
    pub data: Vec<u8>,
    /// 对象的名称
    pub name: String,
    /// 对象的 MIME 内容类型
    pub content_type: String,
    /// 对象数据的大小（字节）
    pub size: u64,
    /// 对象的标签信息
    pub tags: String,
}

/// 缓存淘汰算法
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheAlgorithmType {
    /// 最近最少使用 — 淘汰被访问时间距离现在最久的条目
    Lru,
    /// 先进先出 — 淘汰最先插入的条目
    Fifo,
    /// 最不经常使用 — 淘汰访问次数最少的条目
    Lfu,
}

impl CacheAlgorithmType {
    /// 从字符串解析缓存算法类型。
    /// 支持 "fifo"、"lfu"，其他任何字符串默认返回 LRU。
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "fifo" => Self::Fifo,
            "lfu" => Self::Lfu,
            _ => Self::Lru,
        }
    }

    /// 返回该算法类型的字符串表示。
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Lru => "LRU",
            Self::Fifo => "FIFO",
            Self::Lfu => "LFU",
        }
    }

    /// 返回所有支持的缓存算法类型切片。
    pub fn all() -> &'static [Self] {
        &[Self::Lru, Self::Fifo, Self::Lfu]
    }
}

// ============================================================================
// 缓存统计
// ============================================================================

/// 缓存运行时的统计信息
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// 缓存命中次数
    pub hits: u64,
    /// 缓存未命中次数
    pub misses: u64,
    /// 缓存淘汰次数
    pub evictions: u64,
    /// 当前缓存中的条目数量
    pub size: usize,
    /// 缓存的最大容量
    pub capacity: usize,
    /// 当前使用的缓存算法名称
    pub algorithm: String,
}

impl CacheStats {
    /// 计算缓存的命中率（百分比）。
    /// 如果命中次数和未命中次数之和为零，返回 0.0。
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 { 0.0 } else { (self.hits as f64 / total as f64) * 100.0 }
    }
}

/// 每种缓存算法的基准测试快照，用于对比不同算法的性能
#[derive(Debug, Clone)]
pub struct AlgorithmBenchmark {
    /// 算法名称
    pub algorithm: String,
    /// 基准测试期间的缓存命中次数
    pub hits: u64,
    /// 基准测试期间的缓存未命中次数
    pub misses: u64,
    /// 基准测试期间的缓存淘汰次数
    pub evictions: u64,
    /// 命中率（百分比）
    pub hit_rate: f64,
    /// 基准测试结束时的缓存大小
    pub final_size: usize,
}

// ============================================================================
// 内部缓存实现
// ============================================================================

/// FIFO 缓存：使用 VecDeque + HashMap，实现 O(1) 查找和 O(1) 淘汰顺序。
struct FifoCache {
    /// 缓存的最大容量
    capacity: usize,
    /// 按插入顺序维护的键序列，前端为最早的条目
    order: VecDeque<String>,
    /// 键到缓存对象的映射
    map: HashMap<String, CachedObject>,
}

impl FifoCache {
    /// 创建具有指定容量的新 FIFO 缓存。
    /// 容量至少为 1。
    fn new(capacity: usize) -> Self {
        Self { capacity: capacity.max(1), order: VecDeque::with_capacity(capacity), map: HashMap::new() }
    }

    /// 根据键获取缓存对象。不会改变插入顺序。
    fn get(&mut self, key: &str) -> Option<&CachedObject> {
        self.map.get(key)
    }

    /// 插入或更新一个对象。
    /// 如果键已存在，则更新并将该键移动到队尾；
    /// 如果键不存在且缓存已满，则淘汰队首的条目。
    /// 返回被淘汰的条目（键和值），无淘汰时返回 None。
    fn put(&mut self, key: &str, value: CachedObject) -> Option<(String, CachedObject)> {
        let mut evicted = None;
        if self.map.contains_key(key) {
            // 更新已存在的条目 — 从顺序列表中移除并重新插入到队尾
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

    /// 从缓存中显式删除指定键的对象。
    fn remove(&mut self, key: &str) -> Option<CachedObject> {
        self.order.retain(|k| k != key);
        self.map.remove(key)
    }

    /// 检查键是否存在于缓存中。
    fn contains(&self, key: &str) -> bool { self.map.contains_key(key) }
    /// 返回当前缓存的条目数。
    fn len(&self) -> usize { self.map.len() }
    /// 返回缓存的最大容量。
    fn capacity(&self) -> usize { self.capacity }
    /// 动态调整缓存容量。如果缩小，则从队首开始逐个淘汰多余条目。
    fn resize(&mut self, new_cap: usize) {
        self.capacity = new_cap.max(1);
        while self.map.len() > self.capacity {
            if let Some(old_key) = self.order.pop_front() {
                self.map.remove(&old_key);
            } else { break; }
        }
    }
    /// 清空缓存中的所有数据。
    fn clear(&mut self) { self.order.clear(); self.map.clear(); }
}

/// LFU 缓存：跟踪每个对象的访问频率，淘汰访问频率最低的条目。
/// 当频率相同时，以插入顺序（序列号）作为平局决胜依据。
struct LfuCache {
    /// 缓存的最大容量
    capacity: usize,
    /// 频率映射：键 -> (访问次数, 插入顺序序号)。
    /// frequency[0] = 访问次数，frequency[1] = 插入顺序（平局决胜）
    freq: HashMap<String, (u64, u64)>,
    /// 键到缓存对象的映射
    map: HashMap<String, CachedObject>,
    /// 全局递增的序列号，用于跟踪插入顺序
    seq: u64,
}

impl LfuCache {
    /// 创建具有指定容量的新 LFU 缓存。
    /// 容量至少为 1。
    fn new(capacity: usize) -> Self {
        Self { capacity: capacity.max(1), freq: HashMap::new(), map: HashMap::new(), seq: 0 }
    }

    /// 根据键获取缓存对象，同时递增该键的访问频率。
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

    /// 插入或更新一个对象。
    /// 如果键已存在，则更新值并递增访问频率；
    /// 如果键不存在且缓存已满，则淘汰访问频率最低的条目。
    /// 返回被淘汰的条目（键和值），无淘汰时返回 None。
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
                // 找到访问频率最低的条目（频率相同时以最早的序列号决胜）
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

    /// 从缓存中显式删除指定键的对象及其频率记录。
    fn remove(&mut self, key: &str) -> Option<CachedObject> {
        self.freq.remove(key);
        self.map.remove(key)
    }

    /// 检查键是否存在于缓存中。
    fn contains(&self, key: &str) -> bool { self.map.contains_key(key) }
    /// 返回当前缓存的条目数。
    fn len(&self) -> usize { self.map.len() }
    /// 返回缓存的最大容量。
    fn capacity(&self) -> usize { self.capacity }
    /// 动态调整缓存容量。如果缩小，则按 LFU 规则淘汰多余条目。
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
    /// 清空缓存中的所有数据和频率信息。
    fn clear(&mut self) { self.freq.clear(); self.map.clear(); self.seq = 0; }
}

/// LRU 缓存实现，封装了 `lru` crate 的功能。
struct LruCacheImpl {
    /// `lru` crate 的 LruCache 实例
    inner: lru::LruCache<String, CachedObject>,
}

impl LruCacheImpl {
    /// 创建具有指定容量的新 LRU 缓存。
    /// 容量至少为 1。
    fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).unwrap();
        Self { inner: lru::LruCache::new(cap) }
    }

    /// 根据键获取缓存对象。此操作会将该条目标记为最近使用。
    fn get(&mut self, key: &str) -> Option<&CachedObject> {
        self.inner.get(key)
    }

    /// 插入或更新一个对象。
    /// 返回被淘汰的条目（键和值），无淘汰时返回 None。
    fn put(&mut self, key: &str, value: CachedObject) -> Option<(String, CachedObject)> {
        self.inner.push(key.to_string(), value)
    }

    /// 从缓存中显式删除指定键的对象。
    fn remove(&mut self, key: &str) -> Option<CachedObject> {
        self.inner.pop(key)
    }

    /// 检查键是否存在于缓存中。
    fn contains(&self, key: &str) -> bool { self.inner.contains(key) }
    /// 返回当前缓存的条目数。
    fn len(&self) -> usize { self.inner.len() }
    /// 返回缓存的最大容量。
    fn capacity(&self) -> usize { self.inner.cap().get() }
    /// 动态调整缓存容量。
    fn resize(&mut self, new_cap: usize) {
        let cap = NonZeroUsize::new(new_cap.max(1)).unwrap();
        self.inner.resize(cap);
    }
    /// 清空缓存中的所有数据。
    fn clear(&mut self) { self.inner.clear(); }
}

// ============================================================================
// 统一的 ObjectCache（线程安全，算法可切换）
// ============================================================================

/// 内部缓存枚举，统一封装三种不同的缓存实现。
enum InnerCache {
    Lru(LruCacheImpl),
    Fifo(FifoCache),
    Lfu(LfuCache),
}

/// 线程安全的对象缓存。
/// 支持 LRU / FIFO / LFU 三种淘汰算法，并支持运行时动态调整容量。
pub struct ObjectCache {
    /// 内部缓存实例，由互斥锁保护以确保线程安全
    inner: Mutex<InnerCache>,
    /// 缓存命中次数（原子计数器）
    hits: AtomicU64,
    /// 缓存未命中次数（原子计数器）
    misses: AtomicU64,
    /// 缓存淘汰次数（原子计数器）
    evictions: AtomicU64,
    /// 当前使用的缓存算法
    algorithm: CacheAlgorithmType,
    /// 记录每个 UUID 被访问（GET 调用）的次数。
    /// 用于基准测试生成符合真实访问模式的测试负载。
    access_counts: Mutex<HashMap<String, u64>>,
}

impl ObjectCache {
    /// 创建一个新的对象缓存实例。
    ///
    /// # 参数
    /// - `algorithm`: 要使用的缓存淘汰算法
    /// - `capacity`: 缓存的最大容量
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
            access_counts: Mutex::new(HashMap::new()),
        }
    }

    /// 根据 UUID 获取缓存对象。
    ///
    /// 每次调用都会记录到访问频率统计中，用于后续基准测试。
    /// 如果找到对象则增加命中计数，否则增加未命中计数。
    /// 返回对象的克隆副本（如果存在）。
    pub fn get(&self, uuid: &str) -> Option<CachedObject> {
        // 记录每次访问，用于生成测试负载
        {
            let mut counts = self.access_counts.lock().unwrap();
            *counts.entry(uuid.to_string()).or_insert(0) += 1;
        }
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

    /// 将一个对象插入缓存。
    ///
    /// 如果缓存已满，会按当前算法淘汰一个条目。
    /// 返回被淘汰的条目（键和值）；如果没有发生淘汰则返回 None。
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

    /// 从缓存中显式删除指定 UUID 的对象。
    /// 返回被删除的对象（如果存在）。
    pub fn remove(&self, uuid: &str) -> Option<CachedObject> {
        let mut inner = self.inner.lock().unwrap();
        match &mut *inner {
            InnerCache::Lru(c) => c.remove(uuid),
            InnerCache::Fifo(c) => c.remove(uuid),
            InnerCache::Lfu(c) => c.remove(uuid),
        }
    }

    /// 返回当前缓存中的条目数量。
    pub fn len(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        match &*inner {
            InnerCache::Lru(c) => c.len(),
            InnerCache::Fifo(c) => c.len(),
            InnerCache::Lfu(c) => c.len(),
        }
    }

    /// 返回缓存的最大容量。
    pub fn capacity(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        match &*inner {
            InnerCache::Lru(c) => c.capacity(),
            InnerCache::Fifo(c) => c.capacity(),
            InnerCache::Lfu(c) => c.capacity(),
        }
    }

    /// 在运行时动态调整缓存容量。
    /// 如果缩小容量，多余的条目将按照当前算法被淘汰。
    pub fn resize(&self, new_capacity: usize) {
        let mut inner = self.inner.lock().unwrap();
        match &mut *inner {
            InnerCache::Lru(c) => c.resize(new_capacity),
            InnerCache::Fifo(c) => c.resize(new_capacity),
            InnerCache::Lfu(c) => c.resize(new_capacity),
        }
    }

    /// 清空缓存中的所有数据。
    /// 注意：此操作不会重置统计计数器。
    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        match &mut *inner {
            InnerCache::Lru(c) => c.clear(),
            InnerCache::Fifo(c) => c.clear(),
            InnerCache::Lfu(c) => c.clear(),
        }
    }

    /// 预热缓存：批量插入一组对象。
    ///
    /// 对给定的 (UUID, CachedObject) 列表逐一执行 `put` 操作，
    /// 将对象预加载到缓存中。如果发生淘汰，也会被计入统计。
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

    /// 获取缓存当前的整体统计信息。
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

    /// 返回当前缓存使用的算法类型。
    pub fn algorithm(&self) -> CacheAlgorithmType {
        self.algorithm
    }

    /// 返回每个 UUID 的访问频率映射的副本。
    /// 键是 UUID，值是该 UUID 被 `get()` 调用的次数。
    pub fn get_access_frequencies(&self) -> HashMap<String, u64> {
        self.access_counts.lock().unwrap().clone()
    }

    /// 重置所有统计计数器（命中、未命中、淘汰）为零。
    pub fn reset_stats(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
    }

    // ---- 基准测试支持 ----

    /// 运行一次基准测试：对给定的键列表模拟 GET 访问负载，
    /// 返回此次基准测试期间收集的统计数据。
    ///
    /// # 参数
    /// - `keys`: 要模拟访问的 UUID 序列
    /// - `preloaded`: 基准测试前需要预加载到缓存中的对象列表
    ///
    /// # 注意
    /// 缓存命中时返回真实数据，未命中时会插入一个轻量占位对象，
    /// 以确保该对象参与缓存空间的竞争。否则缓存将永远为空，
    /// 每次访问的命中率都是 0%。这里不需要真实数据，只测量命中/未命中模式。
    pub fn benchmark_run(&self, keys: &[String], preloaded: &[(String, CachedObject)]) -> AlgorithmBenchmark {
        self.clear();
        self.reset_stats();
        for (uuid, obj) in preloaded {
            self.put(uuid, obj.clone());
        }

        for key in keys {
            if self.get(key).is_none() {
                // 缓存未命中 — 插入一个轻量占位对象，使该对象参与缓存空间的竞争。
                // 如果不这样做，缓存将永远为空，每次访问的命中率都会是 0%。
                // 这里不需要真实数据，只测量命中/未命中模式。
                self.put(key, CachedObject {
                    uuid: key.clone(),
                    data: vec![],
                    name: String::new(),
                    content_type: String::new(),
                    size: 0,
                    tags: String::new(),
                });
            }
        }

        let stats = self.stats();
        let hit_rate = stats.hit_rate();
        AlgorithmBenchmark {
            algorithm: stats.algorithm,
            hits: stats.hits,
            misses: stats.misses,
            evictions: stats.evictions,
            hit_rate,
            final_size: stats.size,
        }
    }
}

/// 根据真实的每 UUID 访问频率生成加权基准测试负载。
///
/// 每个对象的权重 = (下载次数 + 1)。用户实际下载次数越多的对象，
/// 在测试负载中出现的频率相应越高。
/// 如果没有可用的频率数据，则退回到均匀分布。
///
/// # 参数
/// - `objects`: 所有可能的对象 UUID 列表
/// - `iterations`: 生成的测试负载中的总访问次数
/// - `frequencies`: UUID 到下载次数的映射
///
/// # 返回
/// 一个长度为 `iterations` 的 UUID 字符串向量，按加权概率分布生成。
pub fn generate_weighted_workload(
    objects: &[String],
    iterations: usize,
    frequencies: &HashMap<String, u64>,
) -> Vec<String> {
    let n = objects.len();
    if n == 0 { return vec![]; }

    // 根据真实的下载次数构建权重（未被访问过的对象最小权重为 1）
    let weights: Vec<f64> = objects.iter()
        .map(|uuid| (frequencies.get(uuid).copied().unwrap_or(0) + 1) as f64)
        .collect();
    let total: f64 = weights.iter().sum();
    if total == 0.0 {
        // 没有任何数据 — 使用均匀分布
        let mut seed = iterations as u64 * n as u64 + 12345;
        let mut wl = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let idx = ((seed >> 32) as usize) % n;
            wl.push(objects[idx].clone());
        }
        return wl;
    }

    let cum: Vec<f64> = weights.iter()
        .scan(0.0, |acc, w| { *acc += w; Some(*acc / total) })
        .collect();
    let mut seed = iterations as u64 * n as u64 + 12345;
    let mut workload = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let r = (seed >> 32) as f64 / (u32::MAX as f64);
        for (j, &c) in cum.iter().enumerate() {
            if r <= c { workload.push(objects[j].clone()); break; }
        }
    }
    workload
}

/// 将字节数转换为人类可读的大小字符串。
///
/// # 示例
/// - `0` -> `"0 B"`
/// - `1024` -> `"1.00 KB"`
/// - `1048576` -> `"1.00 MB"`
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

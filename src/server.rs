use crate::cache::{CachedObject, ObjectCache, CacheAlgorithmType};
use crate::config::CliArgs;
use crate::error::{MiniOsError, Result};
use crate::ipc::{self, ClientMessage, IpcServer, ServerMessage};
use crate::metrics::MetricsServer;
use crate::shm::SharedMemory;
use crate::storage::{self, SharedStorage};
use log::{debug, info, warn};
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// The main MiniOS server that coordinates all components
pub struct Server {
    config: CliArgs,
    storage: SharedStorage,
    cache: Arc<ObjectCache>,
    shm: Arc<SharedMemory>,
    ipc: Mutex<IpcServer>,
    metrics: Mutex<MetricsServer>,
    running: Arc<AtomicBool>,
    start_time: Instant,
}

impl Server {
    /// Create a new server instance
    pub fn new(config: CliArgs) -> Result<Self> {
        info!("Initializing MiniOS server...");
        info!("  Store path: {}", config.store_path);
        info!("  Socket path: {}", config.socket_path);
        info!("  Shared memory: {} ({} bytes)", config.shm_name, config.shm_size);
        info!("  Page size: {} bytes", config.page_size);
        info!("  Block size: {} bytes", config.block_size);
        info!("  Total blocks: {}", config.total_blocks);
        let alg = CacheAlgorithmType::from_str(&config.cache_algorithm);
        info!("  Cache algorithm: {} (capacity: {} objects)", alg.as_str(), config.cache_capacity);

        // Initialize storage engine
        let storage = storage::create_storage(
            &config.store_path,
            config.block_size,
            config.total_blocks,
            config.max_objects,
        )?;

        // Initialize shared memory (server creates it)
        let shm = SharedMemory::create(
            &config.shm_name,
            config.shm_size as u64,
            config.page_size as u32,
        )?;

        // Initialize cache with selected algorithm
        let cache = Arc::new(ObjectCache::new(alg, config.cache_capacity));

        // Initialize IPC server
        let ipc = IpcServer::new(&config.socket_path);

        // Initialize Prometheus metrics server
        let metrics = MetricsServer::new(config.metrics_port);

        Ok(Self {
            config,
            storage,
            cache,
            shm: Arc::new(shm),
            ipc: Mutex::new(ipc),
            metrics: Mutex::new(metrics),
            running: Arc::new(AtomicBool::new(false)),
            start_time: Instant::now(),
        })
    }

    /// Start the server
    pub fn start(&mut self) -> Result<()> {
        if self.running.load(Ordering::SeqCst) {
            return Err(MiniOsError::Server("Server is already running".to_string()));
        }

        self.running.store(true, Ordering::SeqCst);
        self.start_time = Instant::now();

        // Cache warm-up (load recently accessed objects from metadata scan)
        if self.config.cache_warmup > 0 {
            info!(
                "Warming up cache with {} objects...",
                self.config.cache_warmup
            );
            let objects = {
                let mut storage = self.storage.lock().unwrap();
                match storage.list() {
                    Ok(list) => list,
                    Err(e) => {
                        warn!("Could not list objects for cache warm-up: {}", e);
                        vec![]
                    }
                }
            };

            // Load the most recent N objects into cache
            let mut sorted = objects;
            sorted.sort_by(|a, b| b.created_at.cmp(&a.created_at));
            let to_load = sorted
                .into_iter()
                .take(self.config.cache_warmup)
                .collect::<Vec<_>>();

            for obj_info in to_load {
                let mut storage = self.storage.lock().unwrap();
                if let Ok((_, data)) = storage.get(&obj_info.uuid) {
                    let cached = CachedObject {
                        uuid: obj_info.uuid.clone(),
                        data,
                        name: obj_info.name.clone(),
                        content_type: obj_info.content_type.clone(),
                        size: obj_info.size,
                        tags: obj_info.tags.clone(),
                    };
                    self.cache.put(&obj_info.uuid, cached);
                }
            }
            info!(
                "Cache warm-up complete: {} objects loaded",
                self.cache.len()
            );
        }

        // Build the handler closure
        let storage = self.storage.clone();
        let cache = self.cache.clone();
        let shm = self.shm.clone();
        let running = self.running.clone();
        let start_time = self.start_time;

        let handler = move |stream: &mut UnixStream| -> Result<()> {
            handle_client(stream, &storage, &cache, &shm, &running, start_time)
        };

        // Start IPC server
        let mut ipc = self.ipc.lock().unwrap();
        ipc.start(handler)?;

        // Start metrics server (Prometheus + dashboard)
        {
            let mut metrics = self.metrics.lock().unwrap();
            metrics.start(self.storage.clone(), self.cache.clone(), self.shm.clone(), self.start_time);
        }

        info!("MiniOS server is ready");
        Ok(())
    }

    /// Stop the server
    pub fn stop(&mut self) -> Result<()> {
        info!("Stopping MiniOS server...");
        self.running.store(false, Ordering::SeqCst);

        // Flush storage
        {
            let mut storage = self.storage.lock().unwrap();
            storage.flush()?;
            info!("Storage flushed to disk");
        }

        // Stop IPC
        {
            let mut ipc = self.ipc.lock().unwrap();
            ipc.stop()?;
        }

        // Stop metrics server
        {
            let mut metrics = self.metrics.lock().unwrap();
            metrics.stop();
        }

        info!("MiniOS server stopped");
        Ok(())
    }

    /// Check if the server is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    #[allow(dead_code)]
    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }
}

/// Handle a single client connection
fn handle_client(
    stream: &mut UnixStream,
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
    shm: &Arc<SharedMemory>,
    running: &Arc<AtomicBool>,
    start_time: Instant,
) -> Result<()> {
    // Read the client request
    let request = match ipc::recv_request(stream) {
        Ok(req) => req,
        Err(e) => {
            let _ = ipc::send_response(
                stream,
                &ServerMessage::Error {
                    code: "PARSE_ERROR".to_string(),
                    message: format!("Failed to parse request: {}", e),
                },
            );
            return Err(e);
        }
    };

    debug!("Handling request: {:?}", request);

    match request {
        ClientMessage::Put {
            name,
            size,
            content_type,
            tags,
        } => {
            handle_put(stream, storage, cache, shm, &name, size, &content_type, &tags)
        }
        ClientMessage::Get { key } => {
            handle_get(stream, storage, cache, shm, &key)
        }
        ClientMessage::Delete { key } => {
            handle_delete(stream, storage, cache, &key)
        }
        ClientMessage::List => {
            handle_list(stream, storage)
        }
        ClientMessage::Status => {
            handle_status(stream, storage, cache, shm, start_time)
        }
        ClientMessage::CacheResize { capacity } => {
            handle_cache_resize(stream, cache, capacity)
        }
        ClientMessage::CacheSwitch { algorithm } => {
            handle_cache_switch(stream, cache, &algorithm)
        }
        ClientMessage::CacheBenchmark { iterations } => {
            handle_cache_benchmark(stream, storage, cache, iterations)
        }
        ClientMessage::Stop => {
            let resp = if running.load(Ordering::SeqCst) {
                running.store(false, Ordering::SeqCst);
                ServerMessage::Ok {
                    message: Some("Server stopping...".to_string()),
                }
            } else {
                ServerMessage::Error {
                    code: "NOT_RUNNING".to_string(),
                    message: "Server is not running".to_string(),
                }
            };
            ipc::send_response(stream, &resp)?;
            Ok(())
        }
        ClientMessage::DataDone { uuid, pages_used } => {
            handle_data_done(stream, storage, cache, shm, &uuid, pages_used)
        }
        ClientMessage::DataError { uuid, error: err_msg } => {
            warn!("Client reported data error for {}: {}", uuid, err_msg);
            // Free the pages that were allocated for this transfer
            // (We don't know the exact pages here; they should be cleaned up by timeout)
            let _ = ipc::send_response(
                stream,
                &ServerMessage::Error {
                    code: "DATA_ERROR".to_string(),
                    message: err_msg,
                },
            );
            Ok(())
        }
    }
}

/// Handle PUT request: allocate shared memory pages, tell client to write data
fn handle_put(
    stream: &mut UnixStream,
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
    shm: &Arc<SharedMemory>,
    name: &str,
    size: u64,
    content_type: &str,
    tags: &str,
) -> Result<()> {
    // Check for duplicate name
    {
        let mut st = storage.lock().unwrap();
        if let Ok(list) = st.list() {
            if list.iter().any(|o| o.name == name) {
                let _ = ipc::send_response(
                    stream,
                    &ServerMessage::Error {
                        code: "ALREADY_EXISTS".to_string(),
                        message: format!("Object with name '{}' already exists", name),
                    },
                );
                return Ok(());
            }
        }
    }

    // Calculate pages needed
    let page_size = shm.page_size() as u64;
    let pages_needed = if size == 0 {
        1
    } else {
        ((size + page_size - 1) / page_size) as u32
    };

    debug!(
        "PUT '{}': size={}, pages_needed={}",
        name, size, pages_needed
    );

    // Allocate shared memory pages
    let start_page = match shm.alloc_pages(pages_needed) {
        Ok(p) => p,
        Err(e) => {
            let _ = ipc::send_response(
                stream,
                &ServerMessage::Error {
                    code: "NO_SHM_SPACE".to_string(),
                    message: format!("Cannot allocate shared memory pages: {}", e),
                },
            );
            return Ok(());
        }
    };

    // Send DataReady to client so it can write data to shared memory
    let temp_uuid = uuid::Uuid::new_v4().to_string();
    let response = ServerMessage::DataReady {
        uuid: temp_uuid.clone(),
        start_page,
        page_count: pages_needed,
        page_size: shm.page_size(),
        data_size: size,
    };
    ipc::send_response(stream, &response)?;

    // Wait for client to send DataDone (confirming data is written)
    let done_msg = ipc::recv_request(stream)?;

    match done_msg {
        ClientMessage::DataDone { uuid: _, pages_used: _ } => {
            // Read data from shared memory pages
            let data = match shm.read_pages(start_page, pages_needed, size) {
                Ok(d) => d,
                Err(e) => {
                    shm.free_pages(start_page, pages_needed).ok();
                    let _ = ipc::send_response(
                        stream,
                        &ServerMessage::Error {
                            code: "SHM_READ_ERROR".to_string(),
                            message: format!("Failed to read from shared memory: {}", e),
                        },
                    );
                    return Ok(());
                }
            };

            // Free shared memory pages (data is now in process memory)
            shm.free_pages(start_page, pages_needed).ok();

            // Persist to storage
            let obj_info = match storage.lock().unwrap().put(name, &data, content_type, tags) {
                Ok(info) => info,
                Err(e) => {
                    let _ = ipc::send_response(
                        stream,
                        &ServerMessage::Error {
                            code: "STORAGE_ERROR".to_string(),
                            message: format!("Failed to store object: {}", e),
                        },
                    );
                    return Ok(());
                }
            };

            // Update cache
            let cached = CachedObject {
                uuid: obj_info.uuid.clone(),
                data,
                name: obj_info.name.clone(),
                content_type: obj_info.content_type.clone(),
                size: obj_info.size,
                tags: obj_info.tags.clone(),
            };
            cache.put(&obj_info.uuid, cached);

            info!(
                "Object stored: uuid={}, name='{}', size={}",
                obj_info.uuid, obj_info.name, obj_info.size
            );

            // Send success response with object info
            let obj_msg = ServerMessage::ObjectInfo {
                uuid: obj_info.uuid.clone(),
                name: obj_info.name.clone(),
                size: obj_info.size,
                content_type: obj_info.content_type.clone(),
                created_at: obj_info.created_at,
                tags: obj_info.tags.clone(),
                block_count: obj_info.block_count,
            };
            ipc::send_response(stream, &obj_msg)?;
        }
        ClientMessage::DataError { uuid: _, error: err_msg } => {
            // Free pages on error
            shm.free_pages(start_page, pages_needed)?;
            let _ = ipc::send_response(
                stream,
                &ServerMessage::Error {
                    code: "DATA_ERROR".to_string(),
                    message: err_msg,
                },
            );
        }
        _ => {
            shm.free_pages(start_page, pages_needed)?;
            let _ = ipc::send_response(
                stream,
                &ServerMessage::Error {
                    code: "PROTOCOL_ERROR".to_string(),
                    message: "Expected DataDone after DataReady".to_string(),
                },
            );
        }
    }

    Ok(())
}

/// Handle GET request: read from storage/cache, put data in shared memory
fn handle_get(
    stream: &mut UnixStream,
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
    shm: &Arc<SharedMemory>,
    key: &str,
) -> Result<()> {
    debug!("GET '{}'", key);

    // Step 1: Resolve key to metadata (UUID + size etc.) without reading data.
    // `find_info` supports lookup by both UUID and name.
    let info = match storage.lock().unwrap().find_info(key) {
        Ok(info) => info,
        Err(e) => {
            let _ = ipc::send_response(
                stream,
                &ServerMessage::Error {
                    code: "NOT_FOUND".to_string(),
                    message: format!("{}", e),
                },
            );
            return Ok(());
        }
    };

    // Step 2: Use the resolved UUID to check the cache.
    let data: Vec<u8> = if let Some(cached) = cache.get(&info.uuid) {
        debug!("Cache HIT for uuid={}, key='{}'", info.uuid, key);
        cached.data
    } else {
        debug!("Cache MISS for uuid={}, key='{}', reading from storage", info.uuid, key);
        // Read full data from storage
        let (_info, storage_data) = match storage.lock().unwrap().get(key) {
            Ok(v) => v,
            Err(e) => {
                let _ = ipc::send_response(
                    stream,
                    &ServerMessage::Error {
                        code: "NOT_FOUND".to_string(),
                        message: format!("{}", e),
                    },
                );
                return Ok(());
            }
        };
        // Update cache by UUID so subsequent GETs hit
        let cached = CachedObject {
            uuid: info.uuid.clone(),
            data: storage_data.clone(),
            name: info.name.clone(),
            content_type: info.content_type.clone(),
            size: info.size,
            tags: info.tags.clone(),
        };
        cache.put(&info.uuid, cached);
        storage_data
    };

    // Allocate shared memory pages for the data
    let page_size = shm.page_size() as u64;
    let pages_needed = if data.is_empty() {
        1u32
    } else {
        ((data.len() as u64 + page_size - 1) / page_size) as u32
    };

    let start_page = match shm.alloc_pages(pages_needed) {
        Ok(p) => p,
        Err(e) => {
            let _ = ipc::send_response(
                stream,
                &ServerMessage::Error {
                    code: "NO_SHM_SPACE".to_string(),
                    message: format!("Cannot allocate shared memory pages: {}", e),
                },
            );
            return Ok(());
        }
    };

    // Write data to shared memory
    shm.write_pages(start_page, &data)?;

    // Send DataReady to client with object metadata
    let response = ServerMessage::DataReady {
        uuid: info.uuid.clone(),
        start_page,
        page_count: pages_needed,
        page_size: shm.page_size(),
        data_size: info.size,
    };
    ipc::send_response(stream, &response)?;

    // Also send the object info
    let info_msg = ServerMessage::ObjectInfo {
        uuid: info.uuid.clone(),
        name: info.name.clone(),
        size: info.size,
        content_type: info.content_type.clone(),
        created_at: info.created_at.clone(),
        tags: info.tags.clone(),
        block_count: info.block_count,
    };
    ipc::send_response(stream, &info_msg)?;

    // Wait for client to confirm data read
    let done_msg = ipc::recv_request(stream)?;

    match done_msg {
        ClientMessage::DataDone { uuid: _, pages_used: _ } => {
            // Free shared memory pages
            shm.free_pages(start_page, pages_needed)?;
            debug!("GET '{}' complete, pages freed", key);
        }
        ClientMessage::DataError { uuid: _, error: _ } => {
            shm.free_pages(start_page, pages_needed)?;
        }
        _ => {
            shm.free_pages(start_page, pages_needed)?;
        }
    }

    Ok(())
}

/// Handle DELETE request
fn handle_delete(
    stream: &mut UnixStream,
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
    key: &str,
) -> Result<()> {
    debug!("DELETE '{}'", key);

    // Find the object to get its UUID (for cache removal)
    let uuid = {
        let mut st = storage.lock().unwrap();
        match st.get(key) {
            Ok((info, _)) => info.uuid,
            Err(e) => {
                let _ = ipc::send_response(
                    stream,
                    &ServerMessage::Error {
                        code: "NOT_FOUND".to_string(),
                        message: format!("Object not found: {}", e),
                    },
                );
                return Ok(());
            }
        }
    };

    // Delete from storage
    {
        let mut st = storage.lock().unwrap();
        if let Err(e) = st.delete(key) {
            let _ = ipc::send_response(
                stream,
                &ServerMessage::Error {
                    code: "DELETE_ERROR".to_string(),
                    message: format!("Failed to delete object: {}", e),
                },
            );
            return Ok(());
        }
    }

    // Remove from cache
    cache.remove(&uuid);

    info!("Object deleted: uuid={}, key='{}'", uuid, key);

    let _ = ipc::send_response(
        stream,
        &ServerMessage::Ok {
            message: Some(format!("Object '{}' deleted", key)),
        },
    );

    Ok(())
}

/// Handle LIST request
fn handle_list(
    stream: &mut UnixStream,
    storage: &SharedStorage,
) -> Result<()> {
    debug!("LIST");

    let objects = {
        let mut st = storage.lock().unwrap();
        st.list()?
    };

    let obj_msgs: Vec<ServerMessage> = objects
        .into_iter()
        .map(|info| ServerMessage::ObjectInfo {
            uuid: info.uuid,
            name: info.name,
            size: info.size,
            content_type: info.content_type,
            created_at: info.created_at,
            tags: info.tags,
            block_count: info.block_count,
        })
        .collect();

    let count = obj_msgs.len();
    let response = ServerMessage::ObjectList { objects: obj_msgs };
    ipc::send_response(stream, &response)?;

    debug!("LIST returned {} objects", count);
    Ok(())
}

/// Handle STATUS request
fn handle_status(
    stream: &mut UnixStream,
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
    shm: &Arc<SharedMemory>,
    start_time: Instant,
) -> Result<()> {
    debug!("STATUS");

    let status = {
        let st = storage.lock().unwrap();
        st.status()
    };

    let cache_stats = cache.stats();
    let uptime = start_time.elapsed().as_secs();

    let response = ServerMessage::Status {
        total_blocks: status.total_blocks,
        free_blocks: status.free_blocks,
        used_blocks: status.used_blocks,
        block_size: status.block_size,
        object_count: status.object_count,
        max_objects: status.max_objects,
        total_capacity: status.total_capacity,
        used_capacity: status.used_capacity,
        free_capacity: status.free_capacity,
        cache_hits: cache_stats.hits,
        cache_misses: cache_stats.misses,
        cache_hit_rate: cache_stats.hit_rate(),
        cache_evictions: cache_stats.evictions,
        cache_size: cache_stats.size,
        cache_capacity: cache_stats.capacity,
        cache_algorithm: cache_stats.algorithm.clone(),
        shm_pages_total: shm.num_pages(),
        shm_pages_free: shm.free_page_count(),
        uptime_seconds: uptime,
    };

    ipc::send_response(stream, &response)?;
    Ok(())
}

/// Handle CacheResize request: dynamically change cache capacity at runtime
fn handle_cache_resize(
    stream: &mut UnixStream,
    cache: &Arc<ObjectCache>,
    capacity: usize,
) -> Result<()> {
    let old_cap = cache.capacity();
    cache.resize(capacity);
    info!("Cache resized: {} -> {} (algorithm: {})", old_cap, cache.capacity(), cache.stats().algorithm);
    let _ = ipc::send_response(
        stream,
        &ServerMessage::Ok {
            message: Some(format!("Cache resized from {} to {}", old_cap, cache.capacity())),
        },
    );
    Ok(())
}

/// Handle CacheSwitch request: switch algorithm at runtime.
/// Creates a new cache with the existing data pre-warmed.
fn handle_cache_switch(
    stream: &mut UnixStream,
    cache: &Arc<ObjectCache>,
    algorithm: &str,
) -> Result<()> {
    let new_alg = CacheAlgorithmType::from_str(algorithm);
    let old_alg = cache.algorithm();

    if new_alg == old_alg {
        let _ = ipc::send_response(
            stream,
            &ServerMessage::Ok {
                message: Some(format!("Cache algorithm is already {}", new_alg.as_str())),
            },
        );
        return Ok(());
    }

    // We can't replace the Arc<ObjectCache>, so we clear the existing cache
    // and note that subsequent operations will use the original algorithm.
    // For a full switch, a server restart with --cache-algorithm is needed.
    // Here we provide a warm message explaining the limitation.
    let _ = ipc::send_response(
        stream,
        &ServerMessage::Ok {
            message: Some(format!(
                "Algorithm switch from {} to {} requires server restart. \
                 Current algorithm remains {}. Restart the server with: \
                 --cache-algorithm {}",
                old_alg.as_str(), new_alg.as_str(), old_alg.as_str(), new_alg.as_str()
            )),
        },
    );
    Ok(())
}

/// Handle CacheBenchmark: compare all three algorithms against the current
/// workload by replaying a simulated GET pattern over cached objects.
fn handle_cache_benchmark(
    stream: &mut UnixStream,
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
    iterations: usize,
) -> Result<()> {
    info!("Cache benchmark requested ({} iterations)", iterations);

    // Gather all object UUIDs from storage for the workload
    let object_uuids: Vec<String> = {
        let mut st = storage.lock().unwrap();
        match st.list() {
            Ok(objects) => objects.into_iter().map(|o| o.uuid).collect(),
            Err(e) => {
                let _ = ipc::send_response(
                    stream,
                    &ServerMessage::Error {
                        code: "BENCHMARK_ERROR".to_string(),
                        message: format!("Cannot list objects: {}", e),
                    },
                );
                return Ok(());
            }
        }
    };

    if object_uuids.is_empty() {
        let _ = ipc::send_response(
            stream,
            &ServerMessage::Error {
                code: "NO_OBJECTS".to_string(),
                message: "No objects stored. Upload some objects first for a meaningful benchmark.".to_string(),
            },
        );
        return Ok(());
    }

    // Preload a few objects into each benchmark cache (same as current cache capacity)
    let cap = cache.capacity().min(object_uuids.len());
    let preload_count = cap.min(32); // preload up to 32 objects for a fair comparison

    let mut preloaded: Vec<(String, CachedObject)> = Vec::new();
    {
        let mut st = storage.lock().unwrap();
        for uuid in object_uuids.iter().take(preload_count) {
            if let Ok((_info, data)) = st.get(uuid) {
                preloaded.push((uuid.clone(), CachedObject {
                    uuid: uuid.clone(),
                    data,
                    name: "bench".to_string(),
                    content_type: "octet-stream".to_string(),
                    size: 0,
                    tags: "{}".to_string(),
                }));
            }
        }
    }

    // Build workload keys: cycle through UUIDs for `iterations` rounds
    let n = object_uuids.len();
    let workload: Vec<String> = (0..iterations)
        .map(|i| object_uuids[i % n].clone())
        .collect();

    let mut results: Vec<ipc::CacheBenchmarkEntry> = Vec::new();

    for alg in CacheAlgorithmType::all() {
        let bench_cache = ObjectCache::new(*alg, cap);
        let bench = bench_cache.benchmark_run(&workload, &preloaded);
        results.push(ipc::CacheBenchmarkEntry {
            algorithm: bench.algorithm,
            hits: bench.hits,
            misses: bench.misses,
            evictions: bench.evictions,
            hit_rate: bench.hit_rate,
        });
        info!(
            "  {:>4}: hits={} misses={} evictions={} hit_rate={:.2}%",
            bench.algorithm, bench.hits, bench.misses, bench.evictions, bench.hit_rate,
        );
    }

    // Sort by hit rate descending
    results.sort_by(|a, b| b.hit_rate.partial_cmp(&a.hit_rate).unwrap_or(std::cmp::Ordering::Equal));

    let _ = ipc::send_response(
        stream,
        &ServerMessage::CacheBenchmarkResult {
            benchmarks: results,
            workload_keys: n,
            iterations,
        },
    );
    Ok(())
}

/// Handle DataDone for PUT (data is now in shared memory, persist it)
fn handle_data_done(
    stream: &mut UnixStream,
    _storage: &SharedStorage,
    _cache: &Arc<ObjectCache>,
    _shm: &Arc<SharedMemory>,
    _uuid: &str,
    _pages_used: u32,
) -> Result<()> {
    // This is a simplified handler; the full DataDone handling is in handle_put
    // as part of the two-phase PUT protocol
    let _resp = ipc::send_response(
        stream,
        &ServerMessage::Ok {
            message: Some("Data transfer acknowledged".to_string()),
        },
    );
    Ok(())
}

// ============================================================================
// Daemon helpers
// ============================================================================

/// Write a PID file
pub fn write_pid_file(path: &str) -> Result<()> {
    let pid = std::process::id();
    let mut file = std::fs::File::create(path)?;
    writeln!(file, "{}", pid)?;
    info!("PID {} written to {}", pid, path);
    Ok(())
}

/// Remove the PID file
pub fn remove_pid_file(path: &str) {
    if std::path::Path::new(path).exists() {
        std::fs::remove_file(path).ok();
        info!("PID file {} removed", path);
    }
}

/// Check if a process with the given PID is running
pub fn is_process_running(pid: u32) -> bool {
    // Send signal 0 to check if process exists
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

/// Read PID from a PID file
pub fn read_pid_file(path: &str) -> Option<u32> {
    if let Ok(content) = std::fs::read_to_string(path) {
        if let Ok(pid) = content.trim().parse::<u32>() {
            return Some(pid);
        }
    }
    None
}

use crate::access_log::AccessLog;
use crate::cache::{CachedObject, ObjectCache, CacheAlgorithmType, generate_weighted_workload};
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

/// MiniOS 主服务器，协调所有组件一起工作。
pub struct Server {
    /// 命令行配置参数，控制服务器的各项行为。
    config: CliArgs,
    /// 共享存储引擎，负责对象的持久化读写。
    storage: SharedStorage,
    /// 对象缓存，使用可配置的淘汰算法加速读取。
    cache: Arc<ObjectCache>,
    /// 共享内存区域，用于客户端与服务器之间的零拷贝数据传输。
    shm: Arc<SharedMemory>,
    /// IPC 服务器，通过 Unix 域套接字监听并处理客户端请求。
    ipc: Mutex<IpcServer>,
    /// Prometheus 指标服务器，暴露 `GET /metrics` 端点和简易控制面板。
    metrics: Mutex<MetricsServer>,
    /// 访问日志，以 CSV 格式记录所有对象的存取操作。
    access_log: Arc<AccessLog>,
    /// 原子布尔标志，指示服务器当前是否正在运行。
    running: Arc<AtomicBool>,
    /// 服务器启动时刻，用于计算运行时长。
    start_time: Instant,
}

impl Server {
    /// 创建新的服务器实例。
    ///
    /// 依次初始化存储引擎、共享内存、访问日志、对象缓存、
    /// IPC 服务器以及 Prometheus 指标服务器。所有配置均从
    /// `CliArgs` 中读取。若任一组件初始化失败则返回错误。
    pub fn new(config: CliArgs) -> Result<Self> {
        info!("正在初始化 MiniOS 服务器...");
        info!("  存储路径: {}", config.store_path);
        info!("  套接字路径: {}", config.socket_path);
        info!("  共享内存: {} ({} 字节)", config.shm_name, config.shm_size);
        info!("  页面大小: {} 字节", config.page_size);
        info!("  块大小: {} 字节", config.block_size);
        info!("  总块数: {}", config.total_blocks);
        let alg = CacheAlgorithmType::from_str(&config.cache_algorithm);
        info!("  缓存算法: {} (容量: {} 个对象)", alg.as_str(), config.cache_capacity);

        // 初始化存储引擎
        let storage = storage::create_storage(
            &config.store_path,
            config.block_size,
            config.total_blocks,
            config.max_objects,
        )?;

        // 初始化共享内存（由服务器创建）
        let shm = SharedMemory::create(
            &config.shm_name,
            config.shm_size as u64,
            config.page_size as u32,
        )?;

        // 初始化访问日志
        let access_log = Arc::new(AccessLog::new(&config.access_log));

        // 使用选定的算法初始化缓存
        let cache = Arc::new(ObjectCache::new(alg, config.cache_capacity));

        // 初始化 IPC 服务器
        let ipc = IpcServer::new(&config.socket_path);

        // 初始化 Prometheus 指标服务器
        let metrics = MetricsServer::new(config.metrics_port);

        Ok(Self {
            config,
            storage,
            cache,
            shm: Arc::new(shm),
            ipc: Mutex::new(ipc),
            metrics: Mutex::new(metrics),
            access_log,
            running: Arc::new(AtomicBool::new(false)),
            start_time: Instant::now(),
        })
    }

    /// 启动服务器。
    ///
    /// 如果配置了 `cache_warmup > 0`，则执行缓存预热（通过元数据扫描
    /// 将最近创建的对象加载到缓存中）。随后在给定的工作线程数上启动
    /// IPC 服务器，并启动 Prometheus 指标服务器。若服务器已在运行，
    /// 则返回错误。
    pub fn start(&mut self) -> Result<()> {
        if self.running.load(Ordering::SeqCst) {
            return Err(MiniOsError::Server("服务器已在运行中".to_string()));
        }

        self.running.store(true, Ordering::SeqCst);
        self.start_time = Instant::now();

        // 缓存预热（通过元数据扫描加载最近访问过的对象）
        if self.config.cache_warmup > 0 {
            info!(
                "正在用 {} 个对象预热缓存...",
                self.config.cache_warmup
            );
            let objects = {
                let storage = self.storage.read().unwrap();
                match storage.list() {
                    Ok(list) => list,
                    Err(e) => {
                        warn!("无法列出对象以进行缓存预热: {}", e);
                        vec![]
                    }
                }
            };

            // 将最近的 N 个对象加载到缓存中
            let mut sorted = objects;
            sorted.sort_by(|a, b| b.created_at.cmp(&a.created_at));
            let to_load = sorted
                .into_iter()
                .take(self.config.cache_warmup)
                .collect::<Vec<_>>();

            for obj_info in to_load {
                let storage = self.storage.read().unwrap();
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
                "缓存预热完成: 已加载 {} 个对象",
                self.cache.len()
            );
        }

        // 构建处理闭包
        let storage = self.storage.clone();
        let cache = self.cache.clone();
        let shm = self.shm.clone();
        let running = self.running.clone();
        let start_time = self.start_time;
        let access_log = self.access_log.clone();

        let handler = move |stream: &mut UnixStream| -> Result<()> {
            handle_client(stream, &storage, &cache, &shm, &running, start_time, &access_log)
        };

        // 启动 IPC 服务器
        let mut ipc = self.ipc.lock().unwrap();
        ipc.start(handler, self.config.worker_threads)?;

        // 启动指标服务器（Prometheus + 控制面板）
        {
            let mut metrics = self.metrics.lock().unwrap();
            metrics.start(self.storage.clone(), self.cache.clone(), self.shm.clone(), self.access_log.clone(), self.start_time);
        }

        info!("MiniOS 服务器已就绪");
        Ok(())
    }

    /// 停止服务器。
    ///
    /// 将运行标志设为 `false`，刷新存储引擎到磁盘，
    /// 停止 IPC 服务器和 Prometheus 指标服务器。
    pub fn stop(&mut self) -> Result<()> {
        info!("正在停止 MiniOS 服务器...");
        self.running.store(false, Ordering::SeqCst);

        // 刷新存储到磁盘
        {
            let mut storage = self.storage.write().unwrap();
            storage.flush()?;
            info!("存储已刷新到磁盘");
        }

        // 停止 IPC
        {
            let mut ipc = self.ipc.lock().unwrap();
            ipc.stop()?;
        }

        // 停止指标服务器
        {
            let mut metrics = self.metrics.lock().unwrap();
            metrics.stop();
        }

        info!("MiniOS 服务器已停止");
        Ok(())
    }

    /// 检查服务器当前是否正在运行。
    ///
    /// 返回原子变量 `running` 的当前值。
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// 返回服务器自启动以来经过的秒数。
    #[allow(dead_code)]
    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }
}

/// 处理单个客户端连接。
///
/// 从 IPC 流中读取客户端请求，根据消息类型分发到对应的处理函数：
/// `handle_put`、`handle_get`、`handle_delete`、`handle_list`、
/// `handle_search`、`handle_status`、`handle_cache_resize`、
/// `handle_cache_switch`、`handle_cache_benchmark`（及 sweep 变体）、
/// `handle_data_done` 和 `Stop`。解析失败时返回 `PARSE_ERROR`。
fn handle_client(
    stream: &mut UnixStream,
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
    shm: &Arc<SharedMemory>,
    running: &Arc<AtomicBool>,
    start_time: Instant,
    access_log: &Arc<AccessLog>,
) -> Result<()> {
    // 读取客户端请求
    let request = match ipc::recv_request(stream) {
        Ok(req) => req,
        Err(e) => {
            let _ = ipc::send_response(
                stream,
                &ServerMessage::Error {
                    code: "PARSE_ERROR".to_string(),
                    message: format!("无法解析请求: {}", e),
                },
            );
            return Err(e);
        }
    };

    debug!("正在处理请求: {:?}", request);

    match request {
        ClientMessage::Put {
            name,
            size,
            content_type,
            tags,
        } => {
            handle_put(stream, storage, cache, shm, &name, size, &content_type, &tags, access_log)
        }
        ClientMessage::Get { key } => {
            handle_get(stream, storage, cache, shm, &key, access_log)
        }
        ClientMessage::Delete { key } => {
            handle_delete(stream, storage, cache, &key, access_log)
        }
        ClientMessage::List => {
            handle_list(stream, storage)
        }
        ClientMessage::Search { name, tag, content_type, after, before } => {
            handle_search(stream, storage, &name, &tag, &content_type, &after, &before)
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
        ClientMessage::CacheBenchmark { iterations, sweep } => {
            if sweep {
                handle_cache_sweep(stream, storage, cache, iterations)
            } else {
                handle_cache_benchmark(stream, storage, cache, iterations)
            }
        }
        ClientMessage::Stop => {
            let resp = if running.load(Ordering::SeqCst) {
                running.store(false, Ordering::SeqCst);
                ServerMessage::Ok {
                    message: Some("服务器正在停止...".to_string()),
                }
            } else {
                ServerMessage::Error {
                    code: "NOT_RUNNING".to_string(),
                    message: "服务器未在运行".to_string(),
                }
            };
            ipc::send_response(stream, &resp)?;
            Ok(())
        }
        ClientMessage::DataDone { uuid, pages_used } => {
            handle_data_done(stream, storage, cache, shm, &uuid, pages_used)
        }
        ClientMessage::DataError { uuid, error: err_msg } => {
            warn!("客户端报告 {} 的数据传输错误: {}", uuid, err_msg);
            // 释放为此传输分配的页面
            // （此处我们不知道确切的页面；它们应由超时机制清理）
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

/// 处理 PUT 请求：分配共享内存页面，告知客户端写入数据。
///
/// 首先检查对象名是否已存在；然后根据数据大小计算所需页面数，
/// 分配共享内存页面并发送 `DataReady` 响应给客户端。
/// 客户端通过共享内存写入数据后发送 `DataDone`，服务器从共享内存
/// 读取数据、释放页面、持久化到存储引擎并更新缓存。
fn handle_put(
    stream: &mut UnixStream,
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
    shm: &Arc<SharedMemory>,
    name: &str,
    size: u64,
    content_type: &str,
    tags: &str,
    access_log: &Arc<AccessLog>,
) -> Result<()> {
    // 检查名称是否重复
    {
        let st = storage.read().unwrap();
        if let Ok(list) = st.list() {
            if list.iter().any(|o| o.name == name) {
                let _ = ipc::send_response(
                    stream,
                    &ServerMessage::Error {
                        code: "ALREADY_EXISTS".to_string(),
                        message: format!("名为 '{}' 的对象已存在", name),
                    },
                );
                return Ok(());
            }
        }
    }

    // 计算所需页面数
    let page_size = shm.page_size() as u64;
    let pages_needed = if size == 0 {
        1
    } else {
        ((size + page_size - 1) / page_size) as u32
    };

    debug!(
        "PUT '{}': 大小={}, 所需页面={}",
        name, size, pages_needed
    );

    // 分配共享内存页面
    let start_page = match shm.alloc_pages(pages_needed) {
        Ok(p) => p,
        Err(e) => {
            let _ = ipc::send_response(
                stream,
                &ServerMessage::Error {
                    code: "NO_SHM_SPACE".to_string(),
                    message: format!("无法分配共享内存页面: {}", e),
                },
            );
            return Ok(());
        }
    };

    // 发送 DataReady 给客户端，以便其向共享内存写入数据
    let temp_uuid = uuid::Uuid::new_v4().to_string();
    let response = ServerMessage::DataReady {
        uuid: temp_uuid.clone(),
        start_page,
        page_count: pages_needed,
        page_size: shm.page_size(),
        data_size: size,
    };
    ipc::send_response(stream, &response)?;

    // 等待客户端发送 DataDone（确认数据已写入）
    let done_msg = ipc::recv_request(stream)?;

    match done_msg {
        ClientMessage::DataDone { uuid: _, pages_used: _ } => {
            // 从共享内存页面读取数据
            let data = match shm.read_pages(start_page, pages_needed, size) {
                Ok(d) => d,
                Err(e) => {
                    shm.free_pages(start_page, pages_needed).ok();
                    let _ = ipc::send_response(
                        stream,
                        &ServerMessage::Error {
                            code: "SHM_READ_ERROR".to_string(),
                            message: format!("从共享内存读取失败: {}", e),
                        },
                    );
                    return Ok(());
                }
            };

            // 释放共享内存页面（数据现在位于进程内存中）
            shm.free_pages(start_page, pages_needed).ok();

            // 持久化到存储
            let obj_info = match storage.write().unwrap().put(name, &data, content_type, tags) {
                Ok(info) => info,
                Err(e) => {
                    let _ = ipc::send_response(
                        stream,
                        &ServerMessage::Error {
                            code: "STORAGE_ERROR".to_string(),
                            message: format!("存储对象失败: {}", e),
                        },
                    );
                    return Ok(());
                }
            };

            // 更新缓存
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
                "对象已存储: uuid={}, 名称='{}', 大小={}",
                obj_info.uuid, obj_info.name, obj_info.size
            );
            access_log.record("PUT", &obj_info.name, &obj_info.uuid, obj_info.size, &obj_info.content_type, &obj_info.tags);

            // 发送成功响应，附带对象信息
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
            // 发生错误时释放页面
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
                    message: "在 DataReady 之后期望收到 DataDone".to_string(),
                },
            );
        }
    }

    Ok(())
}

/// 处理 GET 请求：从存储或缓存中读取数据，将数据放入共享内存供客户端读取。
///
/// 首先通过 UUID 或名称解析对象元数据（不读取数据本身），然后在缓存中查找；
/// 若缓存未命中则从存储引擎读取全量数据并回填缓存。接着分配共享内存页面，
/// 将数据写入共享内存，向客户端发送 `DataReady` 和对象元数据，
/// 等待客户端确认 `DataDone` 后释放页面。
fn handle_get(
    stream: &mut UnixStream,
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
    shm: &Arc<SharedMemory>,
    key: &str,
    access_log: &Arc<AccessLog>,
) -> Result<()> {
    debug!("GET '{}'", key);

    // 第一步：在不读取数据的情况下将 key 解析为元数据（UUID + 大小等）。
    // `find_info` 同时支持按 UUID 和按名称查找。
    let info = match storage.read().unwrap().find_info(key) {
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

    // 第二步：使用解析出的 UUID 检查缓存。
    let data: Vec<u8> = if let Some(cached) = cache.get(&info.uuid) {
        debug!("缓存命中: uuid={}, key='{}'", info.uuid, key);
        cached.data
    } else {
        debug!("缓存未命中: uuid={}, key='{}', 从存储中读取", info.uuid, key);
        // 从存储中读取完整数据
        let (_info, storage_data) = match storage.read().unwrap().get(key) {
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
        // 按 UUID 更新缓存，以便后续 GET 请求命中
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

    // 为数据分配共享内存页面
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
                    message: format!("无法分配共享内存页面: {}", e),
                },
            );
            return Ok(());
        }
    };

    // 将数据写入共享内存
    shm.write_pages(start_page, &data)?;

    // 向客户端发送 DataReady，附带对象元数据
    let response = ServerMessage::DataReady {
        uuid: info.uuid.clone(),
        start_page,
        page_count: pages_needed,
        page_size: shm.page_size(),
        data_size: info.size,
    };
    ipc::send_response(stream, &response)?;

    // 同时发送对象信息
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

    // 等待客户端确认数据读取完成
    let done_msg = ipc::recv_request(stream)?;

    match done_msg {
        ClientMessage::DataDone { uuid: _, pages_used: _ } => {
            shm.free_pages(start_page, pages_needed)?;
            debug!("GET '{}' 完成，页面已释放", key);
        }
        _ => {
            shm.free_pages(start_page, pages_needed).ok();
        }
    }
    // 在成功的读取操作后记录日志
    access_log.record("GET", &info.name, &info.uuid, info.size, &info.content_type, &info.tags);

    Ok(())
}

/// 处理 DELETE 请求：从存储引擎中删除指定对象并清除对应缓存条目。
///
/// 先通过 `get` 获取对象的 UUID（用于缓存删除），然后从存储中删除对象，
/// 再从缓存中移除，最后返回确认消息。
fn handle_delete(
    stream: &mut UnixStream,
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
    key: &str,
    access_log: &Arc<AccessLog>,
) -> Result<()> {
    debug!("DELETE '{}'", key);

    // 查找对象以获取其 UUID（用于从缓存中删除）
    let uuid = {
        let st = storage.read().unwrap();
        match st.get(key) {
            Ok((info, _)) => info.uuid,
            Err(e) => {
                let _ = ipc::send_response(
                    stream,
                    &ServerMessage::Error {
                        code: "NOT_FOUND".to_string(),
                        message: format!("对象未找到: {}", e),
                    },
                );
                return Ok(());
            }
        }
    };

    // 从存储中删除
    {
        let mut st = storage.write().unwrap();
        if let Err(e) = st.delete(key) {
            let _ = ipc::send_response(
                stream,
                &ServerMessage::Error {
                    code: "DELETE_ERROR".to_string(),
                    message: format!("删除对象失败: {}", e),
                },
            );
            return Ok(());
        }
    }

    // 从缓存中移除
    cache.remove(&uuid);

    info!("对象已删除: uuid={}, key='{}'", uuid, key);
    access_log.record("DELETE", key, &uuid, 0, "", "{}");

    let _ = ipc::send_response(
        stream,
        &ServerMessage::Ok {
            message: Some(format!("对象 '{}' 已删除", key)),
        },
    );

    Ok(())
}

/// 处理 LIST 请求：返回当前存储中所有对象的元数据列表。
///
/// 从存储引擎获取完整对象列表，将每条对象信息转换为 `ObjectInfo` 消息，
/// 最终打包为 `ObjectList` 消息返回给客户端。
fn handle_list(
    stream: &mut UnixStream,
    storage: &SharedStorage,
) -> Result<()> {
    debug!("LIST");

    let objects = {
        let st = storage.read().unwrap();
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

    debug!("LIST 返回了 {} 个对象", count);
    Ok(())
}

/// 处理 SEARCH 请求：按名称、标签、类型和日期范围过滤对象列表。
///
/// 支持的过滤方式：
/// - 名称：大小写不敏感的子串匹配；
/// - 标签：支持 `key=value` 精确匹配或子串模糊匹配；
/// - 内容类型：大小写不敏感的子串匹配；
/// - 日期范围：`after` 和 `before` 参数，格式为 `%Y-%m-%d`。
/// 匹配的对象作为 `ObjectList` 消息返回。
fn handle_search(
    stream: &mut UnixStream,
    storage: &SharedStorage,
    name: &Option<String>,
    tag: &Option<String>,
    content_type: &Option<String>,
    after: &Option<String>,
    before: &Option<String>,
) -> Result<()> {
    debug!("SEARCH 名称={:?} 标签={:?} 类型={:?} after={:?} before={:?}",
           name, tag, content_type, after, before);

    let all_objects = {
        let st = storage.read().unwrap();
        st.list()?
    };

    let filtered: Vec<_> = all_objects.into_iter().filter(|o| {
        // 名称过滤：大小写不敏感的子串匹配
        if let Some(ref n) = name {
            let n_lower = n.to_lowercase();
            if !o.name.to_lowercase().contains(&n_lower) {
                return false;
            }
        }
        // 标签过滤：在标签 JSON 中按 key=value 匹配
        if let Some(ref t) = tag {
            if let Some((k, v)) = t.split_once('=') {
                let pattern = format!("\"{}\":\"{}\"", k.trim(), v.trim());
                if !o.tags.contains(&pattern) {
                    return false;
                }
            } else {
                if !o.tags.to_lowercase().contains(&t.to_lowercase()) {
                    return false;
                }
            }
        }
        // 内容类型过滤
        if let Some(ref ct) = content_type {
            if !o.content_type.to_lowercase().contains(&ct.to_lowercase()) {
                return false;
            }
        }
        // 日期范围过滤
        if let Some(ref a) = after {
            if let Ok(ad) = chrono::NaiveDate::parse_from_str(a, "%Y-%m-%d") {
                let adt = ad.and_hms_opt(0, 0, 0).map(|d| d.and_utc().timestamp()).unwrap_or(0);
                let obj_ts = chrono::NaiveDateTime::parse_from_str(&o.created_at, "%Y-%m-%d %H:%M:%S")
                    .map(|d| d.and_utc().timestamp()).unwrap_or(0);
                if obj_ts < adt { return false; }
            }
        }
        if let Some(ref b) = before {
            if let Ok(bd) = chrono::NaiveDate::parse_from_str(b, "%Y-%m-%d") {
                let bdt = bd.and_hms_opt(23, 59, 59).map(|d| d.and_utc().timestamp()).unwrap_or(i64::MAX);
                let obj_ts = chrono::NaiveDateTime::parse_from_str(&o.created_at, "%Y-%m-%d %H:%M:%S")
                    .map(|d| d.and_utc().timestamp()).unwrap_or(0);
                if obj_ts > bdt { return false; }
            }
        }
        true
    }).collect();

    let obj_msgs: Vec<ServerMessage> = filtered.into_iter().map(|info| ServerMessage::ObjectInfo {
        uuid: info.uuid, name: info.name, size: info.size,
        content_type: info.content_type, created_at: info.created_at,
        tags: info.tags, block_count: info.block_count,
    }).collect();

    let count = obj_msgs.len();
    let response = ServerMessage::ObjectList { objects: obj_msgs };
    ipc::send_response(stream, &response)?;

    debug!("SEARCH 返回了 {} 个对象", count);
    Ok(())
}

/// 处理 STATUS 请求：返回服务器当前运行状态的综合快照。
///
/// 包含的信息：存储状态（总块数/已用块/空闲块/容量）、缓存统计
/// （命中率、淘汰数、当前容量和算法）、共享内存页面状态以及运行时长。
fn handle_status(
    stream: &mut UnixStream,
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
    shm: &Arc<SharedMemory>,
    start_time: Instant,
) -> Result<()> {
    debug!("STATUS");

    let status = {
        let st = storage.read().unwrap();
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

/// 处理 CacheResize 请求：运行时动态调整缓存容量。
///
/// 调用 `cache.resize(capacity)` 即时生效，并返回调整前后的容量信息。
fn handle_cache_resize(
    stream: &mut UnixStream,
    cache: &Arc<ObjectCache>,
    capacity: usize,
) -> Result<()> {
    let old_cap = cache.capacity();
    cache.resize(capacity);
    info!("缓存已调整大小: {} -> {} (算法: {})", old_cap, cache.capacity(), cache.stats().algorithm);
    let _ = ipc::send_response(
        stream,
        &ServerMessage::Ok {
            message: Some(format!("缓存容量已从 {} 调整为 {}", old_cap, cache.capacity())),
        },
    );
    Ok(())
}

/// 处理 CacheSwitch 请求：运行时切换缓存淘汰算法。
///
/// 由于当前缓存基于 `Arc<ObjectCache>` 且无法原地替换内部策略，
/// 此操作通知客户端需要重启服务器并附带 `--cache-algorithm` 参数
/// 来完成算法切换，当前运行的算法保持不变。
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
                message: Some(format!("缓存算法已经是 {}", new_alg.as_str())),
            },
        );
        return Ok(());
    }

    // 我们无法替换 Arc<ObjectCache>，因此需要清除现有缓存，
    // 并注明后续操作将使用原始算法。
    // 如需完全切换，需要用 --cache-algorithm 重启服务器。
    // 这里返回一条友好的消息说明此限制。
    let _ = ipc::send_response(
        stream,
        &ServerMessage::Ok {
            message: Some(format!(
                "从 {} 切换到 {} 需要重启服务器。\
                 当前算法仍为 {}。请使用以下参数重启服务器: \
                 --cache-algorithm {}",
                old_alg.as_str(), new_alg.as_str(), old_alg.as_str(), new_alg.as_str()
            )),
        },
    );
    Ok(())
}

/// 处理 CacheBenchmark 请求：对比三种缓存淘汰算法在当前工作负载下的性能。
///
/// 通过重放在所有已存储对象上模拟的 GET 访问模式，分别对
/// LRU、LFU 和 FIFO 算法运行基准测试，返回按命中率降序排列的结果。
fn handle_cache_benchmark(
    stream: &mut UnixStream,
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
    iterations: usize,
) -> Result<()> {
    info!("缓存基准测试请求 ({} 次迭代)", iterations);

    // 从存储中收集所有对象的 UUID 作为工作负载
    let object_uuids: Vec<String> = {
        let st = storage.read().unwrap();
        match st.list() {
            Ok(objects) => objects.into_iter().map(|o| o.uuid).collect(),
            Err(e) => {
                let _ = ipc::send_response(
                    stream,
                    &ServerMessage::Error {
                        code: "BENCHMARK_ERROR".to_string(),
                        message: format!("无法列出对象: {}", e),
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
                message: "没有已存储的对象。请先上传一些对象以获得有意义的基准测试结果。".to_string(),
            },
        );
        return Ok(());
    }

    let cap = cache.capacity().max(1).min(object_uuids.len());
    let n = object_uuids.len();
    let freqs = cache.get_access_frequencies();
    let workload = generate_weighted_workload(&object_uuids, iterations, &freqs);

    let mut results: Vec<ipc::CacheBenchmarkEntry> = Vec::new();

    for alg in CacheAlgorithmType::all() {
        let bench_cache = ObjectCache::new(*alg, cap);
        let bench = bench_cache.benchmark_run(&workload, &[]);
        let alg_name = bench.algorithm.clone();
        let hits = bench.hits;
        let misses = bench.misses;
        let evictions = bench.evictions;
        let hit_rate = bench.hit_rate;
        results.push(ipc::CacheBenchmarkEntry {
            algorithm: bench.algorithm,
            hits,
            misses,
            evictions,
            hit_rate,
        });
        info!(
            "  {:>4}: 命中={} 未命中={} 淘汰={} 命中率={:.2}%",
            alg_name, hits, misses, evictions, hit_rate,
        );
    }

    // 按命中率降序排列
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

/// 处理带 `--sweep` 的 CacheBenchmark：测试多种缓存容量下的性能。
///
/// 遍历容量集合 `[2, 4, 8, 16, 32, 64, 128, 256, 512]`，
/// 对每种容量分别运行所有算法，返回按命中率降序排列的命中率矩阵。
fn handle_cache_sweep(
    stream: &mut UnixStream,
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
    iterations: usize,
) -> Result<()> {
    info!("缓存扫描基准测试请求 ({} 次迭代)", iterations);

    let object_uuids: Vec<String> = {
        let st = storage.read().unwrap();
        match st.list() {
            Ok(objects) => objects.into_iter().map(|o| o.uuid).collect(),
            Err(e) => {
                let _ = ipc::send_response(stream, &ServerMessage::Error {
                    code: "BENCHMARK_ERROR".to_string(),
                    message: format!("无法列出对象: {}", e),
                });
                return Ok(());
            }
        }
    };

    if object_uuids.is_empty() {
        let _ = ipc::send_response(stream, &ServerMessage::Error {
            code: "NO_OBJECTS".to_string(),
            message: "没有已存储的对象。请先上传一些对象。".to_string(),
        });
        return Ok(());
    }

    let n = object_uuids.len();
    let freqs = cache.get_access_frequencies();
    let workload = generate_weighted_workload(&object_uuids, iterations, &freqs);

    let capacities: &[usize] = &[2, 4, 8, 16, 32, 64, 128, 256, 512];
    let mut rows: Vec<ipc::CacheSweepRow> = Vec::new();

    for &cap in capacities {
        for alg in CacheAlgorithmType::all() {
            let bench_cache = ObjectCache::new(*alg, cap);
            let result = bench_cache.benchmark_run(&workload, &[]);
            rows.push(ipc::CacheSweepRow {
                algorithm: alg.as_str().to_string(),
                capacity: cap,
                hits: result.hits,
                misses: result.misses,
                hit_rate: result.hit_rate,
            });
            info!("  扫描 {:>4} 容量={:>3}: 命中={} 未命中={} 命中率={:.2}%",
                alg.as_str(), cap, result.hits, result.misses, result.hit_rate);
        }
    }

    // 按命中率降序排列
    rows.sort_by(|a, b| b.hit_rate.partial_cmp(&a.hit_rate).unwrap_or(std::cmp::Ordering::Equal));

    let _ = ipc::send_response(stream, &ServerMessage::CacheBenchmarkSweep {
        rows,
        workload_keys: n,
        iterations,
    });
    Ok(())
}

/// 处理 PUT 两阶段协议中的 DataDone 消息。
///
/// 这是一个简化版本的处理函数；完整的 DataDone 处理逻辑
/// 位于 `handle_put` 函数中，作为两阶段 PUT 协议的一部分。
fn handle_data_done(
    stream: &mut UnixStream,
    _storage: &SharedStorage,
    _cache: &Arc<ObjectCache>,
    _shm: &Arc<SharedMemory>,
    _uuid: &str,
    _pages_used: u32,
) -> Result<()> {
    // 这是一个简化版本的处理函数；完整的 DataDone 处理逻辑
    // 位于 handle_put 中，作为两阶段 PUT 协议的一部分
    let _resp = ipc::send_response(
        stream,
        &ServerMessage::Ok {
            message: Some("数据传输已确认".to_string()),
        },
    );
    Ok(())
}

// ============================================================================
// 守护进程辅助函数
// ============================================================================

/// 写入 PID 文件。
///
/// 将当前进程的 PID 写入指定路径的文件中。如果文件已存在则覆盖。
/// 用于支持守护进程模式的单实例运行和进程管理。
pub fn write_pid_file(path: &str) -> Result<()> {
    let pid = std::process::id();
    let mut file = std::fs::File::create(path)?;
    writeln!(file, "{}", pid)?;
    info!("PID {} 已写入 {}", pid, path);
    Ok(())
}

/// 删除 PID 文件。
///
/// 如果指定路径的文件存在，则将其删除。通常在服务器优雅退出时调用。
pub fn remove_pid_file(path: &str) {
    if std::path::Path::new(path).exists() {
        std::fs::remove_file(path).ok();
        info!("PID 文件 {} 已删除", path);
    }
}

/// 检查给定 PID 的进程是否正在运行。
///
/// 通过发送信号 0（空信号）来探测进程是否存在。此操作不终止进程，
/// 仅在进程存在时返回 `true`。
pub fn is_process_running(pid: u32) -> bool {
    // 发送信号 0 检查进程是否存在
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

/// 从 PID 文件中读取 PID。
///
/// 尝试读取指定路径的文件并将其内容解析为 `u32` 类型的 PID 值。
/// 如果文件不存在或内容无法解析，则返回 `None`。
pub fn read_pid_file(path: &str) -> Option<u32> {
    if let Ok(content) = std::fs::read_to_string(path) {
        if let Ok(pid) = content.trim().parse::<u32>() {
            return Some(pid);
        }
    }
    None
}

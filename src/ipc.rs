use crate::error::{MiniOsError, Result};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// 缓存基准测试结果中的单条记录（用于 `ServerMessage`）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheBenchmarkEntry {
    /// 缓存算法的名称
    pub algorithm: String,
    /// 缓存命中次数
    pub hits: u64,
    /// 缓存未命中次数
    pub misses: u64,
    /// 缓存驱逐次数
    pub evictions: u64,
    /// 缓存命中率（0.0 ~ 1.0）
    pub hit_rate: f64,
}

/// 容量扫描基准测试中的一行结果。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheSweepRow {
    /// 缓存算法的名称
    pub algorithm: String,
    /// 测试时使用的缓存容量
    pub capacity: usize,
    /// 在该容量下的缓存命中次数
    pub hits: u64,
    /// 在该容量下的缓存未命中次数
    pub misses: u64,
    /// 在该容量下的缓存命中率（0.0 ~ 1.0）
    pub hit_rate: f64,
}

// ============================================================================
// IPC 消息协议
// ============================================================================

/// 控制消息的最大大小（不包含数据载荷）
const MAX_MSG_SIZE: usize = 64 * 1024; // 64KB

/// 客户端发送给服务端的命令消息
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd")]
pub enum ClientMessage {
    /// 上传一个对象（仅元数据；数据通过共享内存传输）
    Put {
        /// 对象名称
        name: String,
        /// 对象大小（字节）
        size: u64,
        /// 内容类型（MIME）
        content_type: String,
        /// 标签（以某种分隔符分隔的字符串）
        tags: String,
    },

    /// 根据键（UUID 或名称）下载一个对象
    Get {
        /// 对象的唯一标识（UUID）或名称
        key: String,
    },

    /// 根据键（UUID 或名称）删除一个对象
    Delete {
        /// 要删除的对象的 UUID 或名称
        key: String,
    },

    /// 列出所有对象
    List,

    /// 查询服务端状态
    Status,

    /// 停止服务端进程
    Stop,

    /// 客户端已完成共享内存页面的读写操作
    DataDone {
        /// 相关对象的 UUID（put 时为新建的 uuid；get 时表示确认读取完成）
        uuid: String,
        /// 已使用的共享内存页面数量
        pages_used: u32,
    },

    /// 客户端在数据传输过程中遇到了错误
    DataError {
        /// 相关对象的 UUID
        uuid: String,
        /// 错误描述信息
        error: String,
    },

    /// 在运行时调整缓存容量
    CacheResize {
        /// 新的缓存容量（可容纳的元素个数）
        capacity: usize,
    },

    /// 在运行时切换缓存算法
    CacheSwitch {
        /// 要切换到的缓存算法名称
        algorithm: String,
    },

    /// 根据筛选条件搜索对象
    Search {
        /// 按名称模糊搜索（可选）
        name: Option<String>,
        /// 按标签搜索（可选）
        tag: Option<String>,
        /// 按内容类型搜索（可选）
        content_type: Option<String>,
        /// 筛选此时间之后创建的对象（ISO 8601 格式，可选）
        after: Option<String>,
        /// 筛选此时间之前创建的对象（ISO 8601 格式，可选）
        before: Option<String>,
    },

    /// 运行缓存算法基准测试
    CacheBenchmark {
        /// 模拟工作负载的迭代次数
        iterations: usize,
        /// 扫描模式：在多个容量下进行测试
        sweep: bool,
    },
}

/// 服务端发送给客户端的响应消息
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status")]
pub enum ServerMessage {
    /// 操作成功
    Ok {
        /// 可选的附加消息/数据
        message: Option<String>,
    },

    /// 错误响应
    Error {
        /// 错误码
        code: String,
        /// 错误描述信息
        message: String,
    },

    /// 对象元数据（用于 List/Get 响应中嵌入的对象信息）
    ObjectInfo {
        /// 对象的唯一标识符
        uuid: String,
        /// 对象名称
        name: String,
        /// 对象大小（字节）
        size: u64,
        /// 内容类型（MIME）
        content_type: String,
        /// 对象创建时间（ISO 8601 格式）
        created_at: String,
        /// 标签字符串
        tags: String,
        /// 对象占用的存储块数量
        block_count: u32,
    },

    /// 对象列表（用于 List 响应）
    ObjectList {
        /// 对象信息列表，每个元素为 `ObjectInfo` 消息
        objects: Vec<ServerMessage>, // Vec 中存放的是 ObjectInfo 消息
    },

    /// 服务端状态信息
    Status {
        /// 存储块总数
        total_blocks: u64,
        /// 空闲存储块数量
        free_blocks: u64,
        /// 已用存储块数量
        used_blocks: u64,
        /// 每个存储块的大小（字节）
        block_size: u32,
        /// 当前存储的对象数量
        object_count: u64,
        /// 最大可存储对象数量
        max_objects: u64,
        /// 总存储容量（字节）
        total_capacity: u64,
        /// 已用存储容量（字节）
        used_capacity: u64,
        /// 空闲存储容量（字节）
        free_capacity: u64,
        /// 缓存命中累计次数
        cache_hits: u64,
        /// 缓存未命中累计次数
        cache_misses: u64,
        /// 缓存命中率（0.0 ~ 1.0）
        cache_hit_rate: f64,
        /// 缓存驱逐累计次数
        cache_evictions: u64,
        /// 当前缓存中的元素个数
        cache_size: usize,
        /// 缓存的最大容量
        cache_capacity: usize,
        /// 当前使用的缓存算法名称
        cache_algorithm: String,
        /// 共享内存页面总数
        shm_pages_total: u32,
        /// 空闲共享内存页面数量
        shm_pages_free: u32,
        /// 服务端运行时长（秒）
        uptime_seconds: u64,
    },

    /// 缓存基准测试结果，比较所有缓存算法
    CacheBenchmarkResult {
        /// 每个算法的基准测试结果列表
        benchmarks: Vec<CacheBenchmarkEntry>,
        /// 基准测试工作负载中的唯一键数量
        workload_keys: usize,
        /// 实际运行的迭代次数
        iterations: usize,
    },

    /// 容量扫描基准测试结果：每个 (算法, 容量) 组合下的命中率
    CacheBenchmarkSweep {
        /// (算法, 容量, 命中率, 命中次数, 未命中次数) 各行数据，按命中率排序
        rows: Vec<CacheSweepRow>,
        /// 基准测试工作负载中的唯一键数量
        workload_keys: usize,
        /// 实际运行的迭代次数
        iterations: usize,
    },

    /// 为数据传输分配的共享内存信息。
    /// 在 Put 请求的响应中发送（服务端已分配页面，等待客户端写入数据），
    /// 或在 Get 请求的响应中发送（服务端已将数据写入页面，等待客户端读取）。
    DataReady {
        /// 相关对象的 UUID
        uuid: String,
        /// 共享内存中的起始页面编号
        start_page: u32,
        /// 分配的页面数量
        page_count: u32,
        /// 每个页面的大小（字节）
        page_size: u32,
        /// 仅对 Get 有效：对象的总大小，以便客户端知道需要读取多少数据
        data_size: u64,
    },
}

// ============================================================================
// Unix 域套接字 IPC
// ============================================================================

/// 通过 Unix 流套接字发送一条消息。
/// 使用长度前缀协议：4 字节大端序长度 + JSON 载荷。
pub fn send_message(stream: &mut UnixStream, msg: &ClientMessage) -> Result<()> {
    let json = serde_json::to_string(msg)?;
    let bytes = json.as_bytes();

    if bytes.len() > MAX_MSG_SIZE {
        return Err(MiniOsError::Ipc(format!(
            "消息过大：{} 字节（最大允许 {} 字节）",
            bytes.len(),
            MAX_MSG_SIZE
        )));
    }

    // 长度前缀协议：4 字节大端序长度 + JSON 载荷
    let len = bytes.len() as u32;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(bytes)?;
    stream.flush()?;

    debug!("已发送消息: {:?}", msg);
    Ok(())
}

/// 通过 Unix 流套接字接收一条消息（服务端响应）。
/// 使用长度前缀协议：先读取 4 字节大端序长度，再读取 JSON 载荷。
pub fn recv_message(stream: &mut UnixStream) -> Result<ServerMessage> {
    // 读取 4 字节长度前缀
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > MAX_MSG_SIZE {
        return Err(MiniOsError::Ipc(format!(
            "消息过大：{} 字节（最大允许 {} 字节）",
            len, MAX_MSG_SIZE
        )));
    }

    // 读取 JSON 载荷
    let mut json_buf = vec![0u8; len];
    stream.read_exact(&mut json_buf)?;

    let msg: ServerMessage = serde_json::from_slice(&json_buf)?;
    debug!("已收到消息: {:?}", msg);
    Ok(msg)
}

/// 通过 Unix 流套接字向客户端发送一条服务端响应。
/// 使用长度前缀协议：4 字节大端序长度 + JSON 载荷。
pub fn send_response(stream: &mut UnixStream, msg: &ServerMessage) -> Result<()> {
    let json = serde_json::to_string(msg)?;
    let bytes = json.as_bytes();

    if bytes.len() > MAX_MSG_SIZE {
        return Err(MiniOsError::Ipc(format!(
            "响应过大：{} 字节（最大允许 {} 字节）",
            bytes.len(),
            MAX_MSG_SIZE
        )));
    }

    // 长度前缀协议
    let len = bytes.len() as u32;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(bytes)?;
    stream.flush()?;

    debug!("已发送响应: {:?}", msg);
    Ok(())
}

/// 通过 Unix 流套接字接收一条客户端请求消息。
/// 使用长度前缀协议：先读取 4 字节大端序长度，再读取 JSON 载荷。
pub fn recv_request(stream: &mut UnixStream) -> Result<ClientMessage> {
    // 读取 4 字节长度前缀
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > MAX_MSG_SIZE {
        return Err(MiniOsError::Ipc(format!(
            "请求过大：{} 字节（最大允许 {} 字节）",
            len, MAX_MSG_SIZE
        )));
    }

    // 读取 JSON 载荷
    let mut json_buf = vec![0u8; len];
    stream.read_exact(&mut json_buf)?;

    let msg: ClientMessage = serde_json::from_slice(&json_buf)?;
    debug!("已收到请求: {:?}", msg);
    Ok(msg)
}

// ============================================================================
// IPC 服务端（Unix 域套接字监听器）
// ============================================================================

/// 用于处理单个客户端连接的回调函数类型别名。
/// 接收 `&mut UnixStream` 并返回 `Result<()>`。
#[allow(dead_code)]
pub type ClientHandler = Arc<
    dyn Fn(&mut UnixStream) -> Result<()> + Send + Sync + 'static,
>;

/// 基于 Unix 域套接字的 IPC 服务端，负责监听并分发客户端连接。
pub struct IpcServer {
    /// Unix 域套接字文件的路径
    socket_path: String,
    /// Unix 域套接字监听器实例（启动后为 `Some`，停止后为 `None`）
    listener: Option<UnixListener>,
    /// 原子标志位，指示服务端是否正在运行
    running: Arc<AtomicBool>,
}

impl IpcServer {
    /// 创建一个新的 `IpcServer` 实例，指定套接字文件路径。
    /// 此时服务端尚未启动，需要调用 `start` 方法。
    pub fn new(socket_path: &str) -> Self {
        Self {
            socket_path: socket_path.to_string(),
            listener: None,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 获取当前服务端绑定的套接字文件路径。
    #[allow(dead_code)]
    pub fn socket_path(&self) -> &str {
        &self.socket_path
    }

    /// 检查服务端当前是否正在运行。
    #[allow(dead_code)]
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// 启动 IPC 服务端，使用多生产者-多消费者线程池模型。
    ///
    /// 使用有界通道（容量 = num_workers * 2）作为工作队列。
    /// 接收线程（生产者）将客户端连接推入队列。
    /// `num_workers` 个工作线程（消费者）从队列中取出连接并进行处理。
    /// 这种设计提供了背压机制 —— 当所有工作线程都忙碌时，接收线程
    /// 会在 `tx.send()` 上阻塞，防止无限制地创建线程。
    pub fn start<F>(&mut self, handler: F, num_workers: usize) -> Result<()>
    where
        F: Fn(&mut UnixStream) -> Result<()> + Send + Sync + 'static,
    {
        // 如果旧的套接字文件仍然存在，则删除它
        let path = Path::new(&self.socket_path);
        if path.exists() {
            std::fs::remove_file(path).map_err(|e| {
                MiniOsError::Ipc(format!(
                    "无法删除已存在的套接字文件 {}: {}",
                    self.socket_path, e
                ))
            })?;
        }

        // 如果需要，创建父目录
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                MiniOsError::Ipc(format!(
                    "无法创建套接字目录: {}",
                    e
                ))
            })?;
        }

        let listener = UnixListener::bind(&self.socket_path).map_err(|e| {
            MiniOsError::Ipc(format!(
                "无法绑定到 {}: {}",
                self.socket_path, e
            ))
        })?;

        // 设置套接字文件权限，允许任意用户连接
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&self.socket_path, std::fs::Permissions::from_mode(0o666))
                .ok();
        }

        self.listener = Some(listener);
        self.running.store(true, Ordering::SeqCst);

        let listener_ref = self.listener.as_ref().unwrap();
        let listener_copy = listener_ref.try_clone().map_err(|e| {
            MiniOsError::Ipc(format!("无法克隆监听器: {}", e))
        })?;

        let running = self.running.clone();
        let handler = Arc::new(handler);
        let nw = num_workers.max(1);

        info!(
            "IPC 服务端正在监听 {}（MP-MC 模式：{} 个工作线程，队列深度 {}）",
            self.socket_path, nw, nw * 2
        );

        // 有界通道作为工作队列：接收线程 → 通道 → 工作线程
        let (tx, rx): (mpsc::SyncSender<UnixStream>, mpsc::Receiver<UnixStream>) =
            mpsc::sync_channel(nw * 2);

        // 启动工作线程（消费者）。
        // 使用 recv_timeout 以避免工作线程在持有 Mutex 时无限阻塞。
        let rx = Arc::new(Mutex::new(rx));
        for id in 0..nw {
            let rx = rx.clone();
            let h = handler.clone();
            let r = running.clone();
            thread::spawn(move || {
                loop {
                    let stream_opt = {
                        let rx_lock = rx.lock().unwrap();
                        rx_lock.recv_timeout(Duration::from_millis(200)).ok()
                    };
                    match stream_opt {
                        None => {
                            // 超时或通道已断开 —— 检查是否需要退出
                            if !r.load(Ordering::SeqCst) {
                                break;
                            }
                            // 否则继续轮询
                        }
                        Some(mut stream) => {
                            debug!("工作线程 {} 正在处理连接", id);
                            if let Err(e) = h(&mut stream) {
                                error!("工作线程 {} 处理客户端时出错: {}", id, e);
                            }
                        }
                    }
                }
            });
        }

        // 接收线程（生产者）
        thread::spawn(move || {
            for stream in listener_copy.incoming() {
                if !running.load(Ordering::SeqCst) {
                    break;
                }
                match stream {
                    Ok(client_stream) => {
                        // 如果通道已满，send 将阻塞（背压机制）
                        if tx.send(client_stream).is_err() {
                            break; // 所有接收端已丢弃
                        }
                    }
                    Err(e) => {
                        if running.load(Ordering::SeqCst) {
                            error!("连接错误: {}", e);
                        }
                        break;
                    }
                }
            }
            info!("IPC 服务端已停止接受连接");
        });

        Ok(())
    }

    /// 停止 IPC 服务端。
    /// 将 `running` 标志设为 `false`、释放监听器并清理套接字文件。
    pub fn stop(&mut self) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);
        self.listener = None;

        // 清理套接字文件
        if Path::new(&self.socket_path).exists() {
            std::fs::remove_file(&self.socket_path).ok();
        }

        info!("IPC 服务端已停止");
        Ok(())
    }
}

impl Drop for IpcServer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

// ============================================================================
// IPC 客户端
// ============================================================================

/// 用于连接 MiniOS 服务端的 IPC 客户端。
/// 通过 Unix 域套接字发送请求并接收响应。
pub struct IpcClient {
    /// 目标套接字文件的路径
    socket_path: String,
}

impl IpcClient {
    /// 创建一个新的 `IpcClient` 实例，指定目标服务端的套接字文件路径。
    pub fn new(socket_path: &str) -> Self {
        Self {
            socket_path: socket_path.to_string(),
        }
    }

    /// 发送一条请求消息并等待接收一条响应消息。
    /// 这是一个便捷方法，内部会先建立连接、发送请求、接收响应。
    pub fn request(&self, msg: &ClientMessage) -> Result<ServerMessage> {
        let mut stream = self.connect()?;
        send_message(&mut stream, msg)?;
        recv_message(&mut stream)
    }

    /// 连接到服务端并返回一个 `UnixStream`（用于多消息交换场景）。
    ///
    /// 由于标准库的 `UnixStream::connect()` 没有内置超时机制，这里使用
    /// 一个独立的线程配合 channel 来实现连接超时。
    pub fn connect(&self) -> Result<UnixStream> {
        // 快速失败：检查套接字文件是否存在
        let path = Path::new(&self.socket_path);
        if !path.exists() {
            return Err(MiniOsError::Ipc(format!(
                "在 {} 处未找到服务端套接字。服务端是否正在运行？",
                self.socket_path
            )));
        }

        // 使用线程 + channel 为 UnixStream::connect 添加超时功能
        // 这样可以避免直接操作原始套接字/非阻塞模式/poll 等复杂逻辑
        let socket_path = self.socket_path.clone();
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let result = UnixStream::connect(&socket_path);
            // 忽略 send 错误 —— 接收端可能已因超时而丢弃 channel
            let _ = tx.send(result);
        });

        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(stream)) => {
                stream
                    .set_read_timeout(Some(Duration::from_secs(30)))
                    .ok();
                stream
                    .set_write_timeout(Some(Duration::from_secs(30)))
                    .ok();
                Ok(stream)
            }
            Ok(Err(e)) => Err(MiniOsError::Ipc(format!(
                "无法连接到服务端 {}: {}。服务端是否正在运行？",
                self.socket_path, e
            ))),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                Err(MiniOsError::Ipc(format!(
                    "连接到服务端 {} 超时（5 秒后）。服务端是否正在运行？",
                    self.socket_path
                )))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err(MiniOsError::Ipc(format!(
                    "连接到服务端 {} 失败（内部错误）。",
                    self.socket_path
                )))
            }
        }
    }
}

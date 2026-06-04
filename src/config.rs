//! 命令行参数与配置模块。
//!
//! 使用 `clap` 的 derive 模式定义所有 CLI 参数和子命令。

use clap::Parser;

/// MiniOS 命令行参数（服务器和客户端共用）。
///
/// 所有 `--xxx` 形式的参数均在此定义，未指定的参数使用默认值。
#[derive(Parser, Debug, Clone)]
#[command(name = "minios", version, about = "Mini Object Storage Service")]
pub struct CliArgs {
    /// 以服务器模式运行（不加此参数则为客户端模式）
    #[arg(short = 's', long = "server")]
    pub server_mode: bool,

    /// Unix Domain Socket 监听路径
    #[arg(long, default_value = "/tmp/minios.sock")]
    pub socket_path: String,

    /// 共享内存名称（用于客户端-服务器数据传输）
    #[arg(long, default_value = "/minios_shm")]
    pub shm_name: String,

    /// 共享内存总大小（字节），默认 16MB
    #[arg(long, default_value = "16777216")]
    pub shm_size: usize,

    /// 共享内存页大小（字节），默认 4KB
    #[arg(long, default_value = "4096")]
    pub page_size: usize,

    /// store.odb 持久化文件的存储路径
    #[arg(long, default_value = "./store.odb")]
    pub store_path: String,

    /// 数据块大小（字节），默认 4KB
    #[arg(long, default_value = "4096")]
    pub block_size: u32,

    /// 数据块总数（默认 25600，约 100MB 容量）
    #[arg(long, default_value = "25600")]
    pub total_blocks: u64,

    /// 最大可存储对象数（用于预估元数据区大小）
    #[arg(long, default_value = "10000")]
    pub max_objects: u64,

    /// 缓存淘汰算法：lru（默认）、fifo 或 lfu
    #[arg(long, default_value = "lru")]
    pub cache_algorithm: String,

    /// 缓存容量（可缓存的对象数量）
    #[arg(long, default_value = "128")]
    pub cache_capacity: usize,

    /// 运行时动态调整缓存容量的新值（0 表示不调整）
    #[arg(long, default_value = "0")]
    pub cache_resize: usize,

    /// 缓存预热：启动时预加载最近访问的 N 个对象
    #[arg(long, default_value = "0")]
    pub cache_warmup: usize,

    /// Prometheus 监控 HTTP 端口（0 表示禁用 Web 服务和监控）
    #[arg(long, default_value = "9090")]
    pub metrics_port: u16,

    /// 多生产者-多消费者线程池的工作线程数（默认 4）
    #[arg(long, default_value = "4")]
    pub worker_threads: usize,

    /// 日志级别：trace / debug / info / warn / error
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// 访问日志文件路径（空字符串表示禁用）
    #[arg(long, default_value = "")]
    pub access_log: String,

    /// 以守护进程方式运行服务器
    #[arg(long)]
    pub daemonize: bool,

    /// PID 文件路径（守护进程模式使用）
    #[arg(long, default_value = "/tmp/minios.pid")]
    pub pid_file: String,

    /// 客户端子命令：put / get / delete / list / search / status / start / stop 等
    #[command(subcommand)]
    pub command: Option<ClientCommand>,
}

/// 客户端子命令枚举。
///
/// 每个变体对应一种客户端操作。
#[derive(Parser, Debug, Clone)]
pub enum ClientCommand {
    /// 上传对象到服务器
    Put {
        /// 对象名称
        #[arg(short = 'n', long = "name")]
        name: String,
        /// 本地文件路径
        #[arg(short = 'f', long = "file")]
        file: String,
        /// 内容类型（MIME），如 text/plain、image/png
        #[arg(short = 't', long = "type", default_value = "application/octet-stream")]
        content_type: String,
        /// 自定义标签（JSON 格式字符串）
        #[arg(long = "tags", default_value = "{}")]
        tags: String,
    },

    /// 从服务器下载对象
    Get {
        /// 对象的 UUID 或名称
        #[arg(short = 'k', long = "key")]
        key: String,
        /// 下载后的输出文件路径（不指定则使用对象名称）
        #[arg(short = 'o', long = "output")]
        output: Option<String>,
    },

    /// 删除服务器上的对象
    Delete {
        /// 对象的 UUID 或名称
        #[arg(short = 'k', long = "key")]
        key: String,
    },

    /// 列出所有已存储对象
    List {
        /// 显示详细信息（UUID、创建时间、类型等）
        #[arg(short = 'l', long = "long")]
        long_format: bool,
    },

    /// 按名称、标签、类型或日期范围搜索对象
    Search {
        /// 按名称模糊搜索（大小写不敏感）
        #[arg(short = 'n', long = "name")]
        name: Option<String>,
        /// 按标签筛选（格式 key=value，如 author=me）
        #[arg(short = 't', long = "tag")]
        tag: Option<String>,
        /// 按内容类型筛选（如 image、text）
        #[arg(short = 'T', long = "type")]
        content_type: Option<String>,
        /// 筛选该日期之后创建的对象（格式 YYYY-MM-DD）
        #[arg(long = "after")]
        after: Option<String>,
        /// 筛选该日期之前创建的对象
        #[arg(long = "before")]
        before: Option<String>,
    },

    /// 查询服务器状态（存储、缓存、共享内存、运行时间）
    Status,

    /// 运行时调整缓存容量
    CacheResize {
        /// 新的缓存容量（对象数）
        #[arg(short = 'n', long = "capacity")]
        capacity: usize,
    },

    /// 切换缓存淘汰算法
    CacheSwitch {
        /// 目标算法：lru、fifo 或 lfu
        #[arg(short = 'a', long = "algorithm")]
        algorithm: String,
    },

    /// 运行缓存性能测试，对比 LRU/FIFO/LFU 在当前负载下的命中率
    CacheBenchmark {
        /// 模拟的 GET 访问次数
        #[arg(short = 'n', long = "iterations", default_value = "100")]
        iterations: usize,
        /// 扫描模式：测试 9 种容量 × 3 种算法共 27 种组合
        #[arg(long = "sweep")]
        sweep: bool,
    },

    /// 启动服务器
    Start {
        /// 以守护进程方式在后台启动
        #[arg(long)]
        daemon: bool,
    },

    /// 停止服务器
    Stop,
}

impl Default for CliArgs {
    /// 返回所有参数的默认值。
    fn default() -> Self {
        Self {
            server_mode: false,
            socket_path: "/tmp/minios.sock".to_string(),
            shm_name: "/minios_shm".to_string(),
            shm_size: 16 * 1024 * 1024,      // 16MB
            page_size: 4096,
            store_path: "./store.odb".to_string(),
            block_size: 4096,                  // 4KB 块
            total_blocks: 25600,               // ~100MB
            max_objects: 10000,
            cache_algorithm: "lru".to_string(),
            cache_capacity: 128,
            cache_resize: 0,
            cache_warmup: 0,
            metrics_port: 9090,
            worker_threads: 4,
            access_log: String::new(),          // 默认不记录访问日志
            log_level: "info".to_string(),
            daemonize: false,
            pid_file: "/tmp/minios.pid".to_string(),
            command: None,
        }
    }
}

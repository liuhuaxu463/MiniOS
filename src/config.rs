use clap::Parser;
use serde::{Deserialize, Serialize};

/// MiniOS - Mini Object Storage Service
#[derive(Parser, Debug, Clone)]
#[command(name = "minios", version, about = "Mini Object Storage Service")]
pub struct CliArgs {
    /// Run as server daemon
    #[arg(short = 's', long = "server")]
    pub server_mode: bool,

    /// Server socket path
    #[arg(long, default_value = "/tmp/minios.sock")]
    pub socket_path: String,

    /// Shared memory name
    #[arg(long, default_value = "/minios_shm")]
    pub shm_name: String,

    /// Shared memory size in bytes (default 16MB)
    #[arg(long, default_value = "16777216")]
    pub shm_size: usize,

    /// Page size for shared memory (default 4KB)
    #[arg(long, default_value = "4096")]
    pub page_size: usize,

    /// Path to the object database file
    #[arg(long, default_value = "./store.odb")]
    pub store_path: String,

    /// Block size for data blocks (default 4KB)
    #[arg(long, default_value = "4096")]
    pub block_size: u32,

    /// Total number of data blocks in the store file (default 25600 = ~100MB)
    #[arg(long, default_value = "25600")]
    pub total_blocks: u64,

    /// Maximum number of objects (for metadata area sizing)
    #[arg(long, default_value = "10000")]
    pub max_objects: u64,

    /// LRU cache capacity (number of objects)
    #[arg(long, default_value = "128")]
    pub cache_capacity: usize,

    /// Cache warm-up: pre-load N most recently accessed objects on startup
    #[arg(long, default_value = "0")]
    pub cache_warmup: usize,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// Daemonize the server process
    #[arg(long)]
    pub daemonize: bool,

    /// PID file path (for daemon mode)
    #[arg(long, default_value = "/tmp/minios.pid")]
    pub pid_file: String,

    // --- Client subcommands ---
    /// Client command: put/get/delete/list/status/start/stop
    #[command(subcommand)]
    pub command: Option<ClientCommand>,
}

#[derive(Parser, Debug, Clone)]
pub enum ClientCommand {
    /// Upload an object
    Put {
        /// Object name
        #[arg(short = 'n', long = "name")]
        name: String,

        /// Path to the file to upload
        #[arg(short = 'f', long = "file")]
        file: String,

        /// Content type (e.g., "text/plain", "image/png")
        #[arg(short = 't', long = "type", default_value = "application/octet-stream")]
        content_type: String,

        /// Custom tags in JSON format (e.g., '{"author":"me","version":"1"}')
        #[arg(long = "tags", default_value = "{}")]
        tags: String,
    },

    /// Download an object
    Get {
        /// UUID or name of the object
        #[arg(short = 'k', long = "key")]
        key: String,

        /// Output file path (default: stdout or use object name)
        #[arg(short = 'o', long = "output")]
        output: Option<String>,
    },

    /// Delete an object
    Delete {
        /// UUID or name of the object
        #[arg(short = 'k', long = "key")]
        key: String,
    },

    /// List all objects
    List {
        /// Show detailed information
        #[arg(short = 'l', long = "long")]
        long_format: bool,
    },

    /// Query server status
    Status,

    /// Start the server (as daemon)
    Start {
        /// Start in daemon mode
        #[arg(long)]
        daemon: bool,
    },

    /// Stop the server
    Stop,
}

impl Default for CliArgs {
    fn default() -> Self {
        Self {
            server_mode: false,
            socket_path: "/tmp/minios.sock".to_string(),
            shm_name: "/minios_shm".to_string(),
            shm_size: 16 * 1024 * 1024, // 16MB
            page_size: 4096,
            store_path: "./store.odb".to_string(),
            block_size: 4096,
            total_blocks: 25600,
            max_objects: 10000,
            cache_capacity: 128,
            cache_warmup: 0,
            log_level: "info".to_string(),
            daemonize: false,
            pid_file: "/tmp/minios.pid".to_string(),
            command: None,
        }
    }
}

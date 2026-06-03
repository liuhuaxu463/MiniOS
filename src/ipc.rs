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

/// A single entry in a cache benchmark result (used in ServerMessage).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheBenchmarkEntry {
    pub algorithm: String,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub hit_rate: f64,
}

/// A row in a capacity-sweep benchmark.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheSweepRow {
    pub algorithm: String,
    pub capacity: usize,
    pub hits: u64,
    pub misses: u64,
    pub hit_rate: f64,
}

// ============================================================================
// IPC Message Protocol
// ============================================================================

/// Maximum message size for control messages (not data)
const MAX_MSG_SIZE: usize = 64 * 1024; // 64KB

/// Command messages sent from client to server
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd")]
pub enum ClientMessage {
    /// Upload an object (metadata only; data via shared memory)
    Put {
        name: String,
        size: u64,
        content_type: String,
        tags: String,
    },

    /// Download an object by key (UUID or name)
    Get {
        key: String,
    },

    /// Delete an object by key (UUID or name)
    Delete {
        key: String,
    },

    /// List all objects
    List,

    /// Query server status
    Status,

    /// Stop the server
    Stop,

    /// Client has finished reading/writing shared memory pages
    DataDone {
        /// UUID of the object involved (for put: new uuid; for get: confirms read)
        uuid: String,
        /// Number of pages used
        pages_used: u32,
    },

    /// Client encountered an error during data transfer
    DataError {
        uuid: String,
        error: String,
    },

    /// Resize cache at runtime
    CacheResize {
        capacity: usize,
    },

    /// Switch cache algorithm at runtime
    CacheSwitch {
        algorithm: String,
    },

    /// Search objects by filters
    Search {
        name: Option<String>,
        tag: Option<String>,
        content_type: Option<String>,
        after: Option<String>,
        before: Option<String>,
    },

    /// Run cache algorithm benchmark
    CacheBenchmark {
        /// Number of iterations for the simulated workload
        iterations: usize,
        /// Sweep mode: test across multiple capacities
        sweep: bool,
    },
}

/// Response messages sent from server to client
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status")]
pub enum ServerMessage {
    /// Operation succeeded
    Ok {
        /// Optional message/data
        message: Option<String>,
    },

    /// Error response
    Error {
        code: String,
        message: String,
    },

    /// Object metadata (for List/Get)
    ObjectInfo {
        uuid: String,
        name: String,
        size: u64,
        content_type: String,
        created_at: String,
        tags: String,
        block_count: u32,
    },

    /// Object list (for List response)
    ObjectList {
        objects: Vec<ServerMessage>, // Vec of ObjectInfo messages
    },

    /// Server status
    Status {
        total_blocks: u64,
        free_blocks: u64,
        used_blocks: u64,
        block_size: u32,
        object_count: u64,
        max_objects: u64,
        total_capacity: u64,
        used_capacity: u64,
        free_capacity: u64,
        cache_hits: u64,
        cache_misses: u64,
        cache_hit_rate: f64,
        cache_evictions: u64,
        cache_size: usize,
        cache_capacity: usize,
        cache_algorithm: String,
        shm_pages_total: u32,
        shm_pages_free: u32,
        uptime_seconds: u64,
    },

    /// Cache benchmark result comparing all algorithms
    CacheBenchmarkResult {
        benchmarks: Vec<CacheBenchmarkEntry>,
        /// Number of unique keys in the benchmark workload
        workload_keys: usize,
        /// Total iterations run
        iterations: usize,
    },

    /// Capacity-sweep benchmark result: hit rate per (algorithm, capacity)
    CacheBenchmarkSweep {
        /// (algorithm, capacity, hit_rate, hits, misses) rows, sorted by hit rate
        rows: Vec<CacheSweepRow>,
        workload_keys: usize,
        iterations: usize,
    },

    /// Shared memory allocation for data transfer
    /// Sent in response to Put (server has allocated pages, client writes data)
    /// or Get (server has written data to pages, client reads data)
    DataReady {
        uuid: String,
        /// Starting page number in shared memory
        start_page: u32,
        /// Number of pages allocated
        page_count: u32,
        /// Page size in bytes
        page_size: u32,
        /// For Get: total object size (so client knows how much to read)
        data_size: u64,
    },
}

// ============================================================================
// Unix Domain Socket IPC
// ============================================================================

/// Send a message over a Unix stream socket
pub fn send_message(stream: &mut UnixStream, msg: &ClientMessage) -> Result<()> {
    let json = serde_json::to_string(msg)?;
    let bytes = json.as_bytes();

    if bytes.len() > MAX_MSG_SIZE {
        return Err(MiniOsError::Ipc(format!(
            "Message too large: {} bytes (max {})",
            bytes.len(),
            MAX_MSG_SIZE
        )));
    }

    // Length-prefixed protocol: 4-byte big-endian length + JSON payload
    let len = bytes.len() as u32;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(bytes)?;
    stream.flush()?;

    debug!("Sent message: {:?}", msg);
    Ok(())
}

/// Receive a message over a Unix stream socket
pub fn recv_message(stream: &mut UnixStream) -> Result<ServerMessage> {
    // Read 4-byte length prefix
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > MAX_MSG_SIZE {
        return Err(MiniOsError::Ipc(format!(
            "Message too large: {} bytes (max {})",
            len, MAX_MSG_SIZE
        )));
    }

    // Read JSON payload
    let mut json_buf = vec![0u8; len];
    stream.read_exact(&mut json_buf)?;

    let msg: ServerMessage = serde_json::from_slice(&json_buf)?;
    debug!("Received message: {:?}", msg);
    Ok(msg)
}

/// Send a server response over a Unix stream socket
pub fn send_response(stream: &mut UnixStream, msg: &ServerMessage) -> Result<()> {
    let json = serde_json::to_string(msg)?;
    let bytes = json.as_bytes();

    if bytes.len() > MAX_MSG_SIZE {
        return Err(MiniOsError::Ipc(format!(
            "Response too large: {} bytes (max {})",
            bytes.len(),
            MAX_MSG_SIZE
        )));
    }

    // Length-prefixed protocol
    let len = bytes.len() as u32;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(bytes)?;
    stream.flush()?;

    debug!("Sent response: {:?}", msg);
    Ok(())
}

/// Receive a client message over a Unix stream socket
pub fn recv_request(stream: &mut UnixStream) -> Result<ClientMessage> {
    // Read 4-byte length prefix
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > MAX_MSG_SIZE {
        return Err(MiniOsError::Ipc(format!(
            "Request too large: {} bytes (max {})",
            len, MAX_MSG_SIZE
        )));
    }

    // Read JSON payload
    let mut json_buf = vec![0u8; len];
    stream.read_exact(&mut json_buf)?;

    let msg: ClientMessage = serde_json::from_slice(&json_buf)?;
    debug!("Received request: {:?}", msg);
    Ok(msg)
}

// ============================================================================
// IPC Server (Unix Domain Socket Listener)
// ============================================================================

/// Type alias for a function that handles a single client connection
#[allow(dead_code)]
pub type ClientHandler = Arc<
    dyn Fn(&mut UnixStream) -> Result<()> + Send + Sync + 'static,
>;

/// IPC server that listens on a Unix domain socket
pub struct IpcServer {
    socket_path: String,
    listener: Option<UnixListener>,
    running: Arc<AtomicBool>,
}

impl IpcServer {
    /// Create a new IPC server
    pub fn new(socket_path: &str) -> Self {
        Self {
            socket_path: socket_path.to_string(),
            listener: None,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    #[allow(dead_code)]
    pub fn socket_path(&self) -> &str {
        &self.socket_path
    }

    #[allow(dead_code)]
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Start the IPC server with a multi-producer multi-consumer thread pool.
    ///
    /// Uses a bounded channel (capacity = num_workers * 2) as the work queue.
    /// The accept thread (producer) pushes connections into the queue.
    /// `num_workers` threads (consumers) dequeue and handle each connection.
    /// This provides backpressure — when all workers are busy, the accept
    /// thread blocks on `tx.send()`, preventing unbounded thread creation.
    pub fn start<F>(&mut self, handler: F, num_workers: usize) -> Result<()>
    where
        F: Fn(&mut UnixStream) -> Result<()> + Send + Sync + 'static,
    {
        // Remove old socket file if it exists
        let path = Path::new(&self.socket_path);
        if path.exists() {
            std::fs::remove_file(path).map_err(|e| {
                MiniOsError::Ipc(format!(
                    "Cannot remove existing socket {}: {}",
                    self.socket_path, e
                ))
            })?;
        }

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                MiniOsError::Ipc(format!(
                    "Cannot create socket directory: {}",
                    e
                ))
            })?;
        }

        let listener = UnixListener::bind(&self.socket_path).map_err(|e| {
            MiniOsError::Ipc(format!(
                "Cannot bind to {}: {}",
                self.socket_path, e
            ))
        })?;

        // Set permissions to allow any user to connect
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
            MiniOsError::Ipc(format!("Cannot clone listener: {}", e))
        })?;

        let running = self.running.clone();
        let handler = Arc::new(handler);
        let nw = num_workers.max(1);

        info!(
            "IPC server listening on {} (MP-MC: {} workers, queue depth {})",
            self.socket_path, nw, nw * 2
        );

        // Bounded channel as the work queue: accept thread → channel → workers
        let (tx, rx): (mpsc::SyncSender<UnixStream>, mpsc::Receiver<UnixStream>) =
            mpsc::sync_channel(nw * 2);

        // Spawn worker threads (consumers).
        // Use recv_timeout so no worker holds the mutex while blocking.
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
                            // Timeout or disconnected — check if we should exit
                            if !r.load(Ordering::SeqCst) {
                                break;
                            }
                            // Otherwise keep polling
                        }
                        Some(mut stream) => {
                            debug!("Worker {} handling connection", id);
                            if let Err(e) = h(&mut stream) {
                                error!("Worker {} error handling client: {}", id, e);
                            }
                        }
                    }
                }
            });
        }

        // Accept thread (producer)
        thread::spawn(move || {
            for stream in listener_copy.incoming() {
                if !running.load(Ordering::SeqCst) {
                    break;
                }
                match stream {
                    Ok(client_stream) => {
                        // send will block if the channel is full (backpressure)
                        if tx.send(client_stream).is_err() {
                            break; // all receivers dropped
                        }
                    }
                    Err(e) => {
                        if running.load(Ordering::SeqCst) {
                            error!("Connection error: {}", e);
                        }
                        break;
                    }
                }
            }
            info!("IPC server stopped accepting connections");
        });

        Ok(())
    }

    /// Stop the IPC server
    pub fn stop(&mut self) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);
        self.listener = None;

        // Clean up socket file
        if Path::new(&self.socket_path).exists() {
            std::fs::remove_file(&self.socket_path).ok();
        }

        info!("IPC server stopped");
        Ok(())
    }
}

impl Drop for IpcServer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

// ============================================================================
// IPC Client
// ============================================================================

/// Client for connecting to the MiniOS server
pub struct IpcClient {
    socket_path: String,
}

impl IpcClient {
    /// Create a new IPC client
    pub fn new(socket_path: &str) -> Self {
        Self {
            socket_path: socket_path.to_string(),
        }
    }

    /// Send a request and receive a response
    pub fn request(&self, msg: &ClientMessage) -> Result<ServerMessage> {
        let mut stream = self.connect()?;
        send_message(&mut stream, msg)?;
        recv_message(&mut stream)
    }

    /// Connect to the server and get a stream (for multi-message exchanges).
    ///
    /// Uses a separate thread with a channel to implement connection timeout,
    /// since std's UnixStream::connect() has no built-in timeout mechanism.
    pub fn connect(&self) -> Result<UnixStream> {
        // Fast-fail: check if socket file exists
        let path = Path::new(&self.socket_path);
        if !path.exists() {
            return Err(MiniOsError::Ipc(format!(
                "Server socket not found at {}. Is the server running?",
                self.socket_path
            )));
        }

        // Use a thread + channel to add timeout to UnixStream::connect
        // This avoids all the complexity of raw sockets / non-blocking / poll
        let socket_path = self.socket_path.clone();
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let result = UnixStream::connect(&socket_path);
            // Ignore send error — receiver may have timed out and dropped
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
                "Cannot connect to server at {}: {}. Is the server running?",
                self.socket_path, e
            ))),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                Err(MiniOsError::Ipc(format!(
                    "Connection to server at {} timed out after 5s. Is the server running?",
                    self.socket_path
                )))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err(MiniOsError::Ipc(format!(
                    "Connection to server at {} failed (internal error).",
                    self.socket_path
                )))
            }
        }
    }
}

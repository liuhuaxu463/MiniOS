use crate::error::{MiniOsError, Result};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::ffi::CString;
use std::io::{self, Read, Write};
use std::os::unix::io::FromRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

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
        cache_size: usize,
        cache_capacity: usize,
        shm_pages_total: u32,
        shm_pages_free: u32,
        uptime_seconds: u64,
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

    /// Get the socket path
    pub fn socket_path(&self) -> &str {
        &self.socket_path
    }

    /// Check if the server is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Start the IPC server.
    ///
    /// Spawns a thread for each client connection. The `handler` function
    /// is called with each connected client stream.
    pub fn start<F>(&mut self, handler: F) -> Result<()>
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
        // (on Linux, use fchmod; on other Unix, try chmod on the path)
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

        info!(
            "IPC server listening on {}",
            self.socket_path
        );

        // Accept connections in a dedicated thread
        thread::spawn(move || {
            for stream in listener_copy.incoming() {
                if !running.load(Ordering::SeqCst) {
                    break;
                }

                match stream {
                    Ok(mut client_stream) => {
                        debug!("New client connection");
                        let h = handler.clone();
                        thread::spawn(move || {
                            if let Err(e) = h(&mut client_stream) {
                                error!("Error handling client: {}", e);
                            }
                        });
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
    /// Uses non-blocking connect with a 5-second timeout to avoid hanging
    /// when the server is not running.
    pub fn connect(&self) -> Result<UnixStream> {
        // Fast-fail: check if socket file exists
        let path = Path::new(&self.socket_path);
        if !path.exists() {
            return Err(MiniOsError::Ipc(format!(
                "Server socket not found at {}. Is the server running?",
                self.socket_path
            )));
        }

        // Build sockaddr_un for the socket path
        let sockaddr = self.make_sockaddr()?;

        // Create raw socket
        let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        if fd < 0 {
            return Err(MiniOsError::Ipc(format!(
                "Failed to create socket: {}",
                io::Error::last_os_error()
            )));
        }

        // Set non-blocking to enable timeout
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL, 0) };
        if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            unsafe { libc::close(fd) };
            return Err(MiniOsError::Ipc(format!(
                "Failed to set non-blocking: {}",
                io::Error::last_os_error()
            )));
        }

        // Attempt non-blocking connect
        let ret = unsafe {
            libc::connect(
                fd,
                sockaddr.as_ptr() as *const libc::sockaddr,
                sockaddr.len() as libc::socklen_t,
            )
        };

        if ret < 0 {
            let err = io::Error::last_os_error();
            // EINPROGRESS means connect is in progress (expected for non-blocking)
            if err.raw_os_error() != Some(libc::EINPROGRESS) {
                unsafe { libc::close(fd) };
                return Err(MiniOsError::Ipc(format!(
                    "Cannot connect to server at {}: {}. Is the server running?",
                    self.socket_path, err
                )));
            }

            // Wait for the connection to complete with a poll timeout
            let mut pfd = libc::pollfd {
                fd,
                events: libc::POLLOUT,
                revents: 0,
            };

            let poll_timeout: i32 = 5000; // 5 seconds
            let poll_ret = unsafe { libc::poll(&mut pfd, 1, poll_timeout) };

            if poll_ret < 0 {
                unsafe { libc::close(fd) };
                return Err(MiniOsError::Ipc(format!(
                    "Connection poll failed: {}",
                    io::Error::last_os_error()
                )));
            }
            if poll_ret == 0 {
                unsafe { libc::close(fd) };
                return Err(MiniOsError::Ipc(format!(
                    "Connection to server at {} timed out after 5s. Is the server running?",
                    self.socket_path
                )));
            }
            // Check if there was an error on the socket
            if pfd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
                unsafe { libc::close(fd) };
                return Err(MiniOsError::Ipc(format!(
                    "Cannot connect to server at {}. Server may not be running.",
                    self.socket_path
                )));
            }
        }

        // Restore blocking mode
        if flags >= 0 {
            unsafe { libc::fcntl(fd, libc::F_SETFL, flags) };
        }

        // Wrap fd in UnixStream
        let stream = unsafe { UnixStream::from_raw_fd(fd) };

        // Set read/write timeouts
        let timeout = Duration::from_secs(30);
        stream.set_read_timeout(Some(timeout)).ok();
        stream.set_write_timeout(Some(timeout)).ok();

        Ok(stream)
    }

    /// Build a sockaddr_un for the socket path
    fn make_sockaddr(&self) -> Result<[libc::c_char; 110]> {
        let path_c =
            CString::new(self.socket_path.as_bytes()).map_err(|e| {
                MiniOsError::Ipc(format!("Invalid socket path: {}", e))
            })?;

        let path_bytes = path_c.as_bytes_with_nul();
        if path_bytes.len() > 108 {
            return Err(MiniOsError::Ipc(format!(
                "Socket path too long (max 108 bytes): {}",
                self.socket_path
            )));
        }

        // sockaddr_un: sa_family_t (2 bytes) + sun_path (108 bytes) = 110 bytes
        let mut addr: [libc::c_char; 110] = [0; 110];
        // AF_UNIX = 1 (usually u16, little-endian)
        addr[0] = 1; // AF_UNIX lo-byte
        addr[1] = 0; // AF_UNIX hi-byte
        // Copy path (with null) starting at offset 2
        for (i, &b) in path_bytes.iter().enumerate() {
            addr[2 + i] = b as libc::c_char;
        }
        Ok(addr)
    }
}

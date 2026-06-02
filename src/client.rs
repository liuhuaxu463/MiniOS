use crate::cache::human_readable_size;
use crate::config::CliArgs;
use crate::error::{MiniOsError, Result};
use crate::ipc::{self, ClientMessage, IpcClient, ServerMessage};
use crate::server;
use crate::shm::SharedMemory;
use log::info;
use std::io::{self, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

/// MiniOS CLI client
pub struct Client {
    config: CliArgs,
    ipc: IpcClient,
}

impl Client {
    /// Create a new client from configuration
    pub fn new(config: CliArgs) -> Self {
        let ipc = IpcClient::new(&config.socket_path);
        Self { config, ipc }
    }

    /// Execute the requested command
    pub fn execute(&self, cmd: &crate::config::ClientCommand) -> Result<()> {
        match cmd {
            crate::config::ClientCommand::Put {
                name,
                file,
                content_type,
                tags,
            } => self.cmd_put(name, file, content_type, tags),

            crate::config::ClientCommand::Get { key, output } => {
                self.cmd_get(key, output.as_deref())
            }

            crate::config::ClientCommand::Delete { key } => {
                self.cmd_delete(key)
            }

            crate::config::ClientCommand::List { long_format } => {
                self.cmd_list(*long_format)
            }

            crate::config::ClientCommand::Status => {
                self.cmd_status()
            }

            crate::config::ClientCommand::CacheResize { capacity } => {
                self.cmd_cache_resize(*capacity)
            }

            crate::config::ClientCommand::CacheSwitch { algorithm } => {
                self.cmd_cache_switch(algorithm)
            }

            crate::config::ClientCommand::CacheBenchmark { iterations, sweep } => {
                self.cmd_cache_benchmark(*iterations, *sweep)
            }

            crate::config::ClientCommand::Start { daemon } => {
                self.cmd_start(*daemon)
            }

            crate::config::ClientCommand::Stop => {
                self.cmd_stop()
            }
        }
    }

    // --- PUT ---
    fn cmd_put(
        &self,
        name: &str,
        file_path: &str,
        content_type: &str,
        tags: &str,
    ) -> Result<()> {
        // Validate tags JSON
        if tags != "{}" {
            serde_json::from_str::<serde_json::Value>(tags).map_err(|e| {
                MiniOsError::InvalidArgument(format!("Invalid tags JSON: {}", e))
            })?;
        }

        // Read the file
        let path = Path::new(file_path);
        if !path.exists() {
            return Err(MiniOsError::Client(format!(
                "File not found: {}",
                file_path
            )));
        }

        let data = std::fs::read(path).map_err(|e| {
            MiniOsError::Client(format!("Cannot read file '{}': {}", file_path, e))
        })?;

        let data_size = data.len() as u64;
        println!(
            "Uploading '{}' ({} bytes, type={}) as '{}'...",
            file_path,
            data_size,
            content_type,
            name
        );

        // Connect to server
        let mut stream = self.ipc.connect()?;

        // Send PUT request
        let put_msg = ClientMessage::Put {
            name: name.to_string(),
            size: data_size,
            content_type: content_type.to_string(),
            tags: tags.to_string(),
        };
        ipc::send_message(&mut stream, &put_msg)?;

        // Receive DataReady response (server has allocated shared memory pages)
        let response = ipc::recv_message(&mut stream)?;

        match response {
            ServerMessage::DataReady {
                uuid,
                start_page,
                page_count,
                page_size: _,
                data_size: _,
            } => {
                // Open shared memory and write data
                let shm = SharedMemory::open(&self.config.shm_name)?;
                shm.write_pages(start_page, &data)?;

                println!(
                    "  Wrote {} bytes to shared memory (pages {}-{})",
                    data.len(),
                    start_page,
                    start_page + page_count - 1
                );

                // Send DataDone confirmation
                let done_msg = ClientMessage::DataDone {
                    uuid: uuid.clone(),
                    pages_used: page_count,
                };
                ipc::send_message(&mut stream, &done_msg)?;

                // Receive final response (object info)
                let final_resp = ipc::recv_message(&mut stream)?;
                match final_resp {
                    ServerMessage::ObjectInfo {
                        uuid,
                        name,
                        size,
                        content_type,
                        created_at,
                        tags,
                        block_count,
                    } => {
                        println!("\n✓ Object stored successfully!");
                        println!("  UUID:        {}", uuid);
                        println!("  Name:        {}", name);
                        println!("  Size:        {} ({})", size, human_readable_size(size));
                        println!("  Type:        {}", content_type);
                        println!("  Created:     {}", created_at);
                        println!("  Tags:        {}", tags);
                        println!("  Data blocks: {}", block_count);
                    }
                    ServerMessage::Error { code, message } => {
                        eprintln!("Error [{}]: {}", code, message);
                    }
                    other => {
                        eprintln!("Unexpected response: {:?}", other);
                    }
                }
            }
            ServerMessage::Error { code, message } => {
                eprintln!("Error [{}]: {}", code, message);
            }
            other => {
                eprintln!("Unexpected response: {:?}", other);
            }
        }

        Ok(())
    }

    // --- GET ---
    fn cmd_get(&self, key: &str, output: Option<&str>) -> Result<()> {
        println!("Downloading '{}'...", key);

        let mut stream = self.ipc.connect()?;

        // Send GET request
        let get_msg = ClientMessage::Get {
            key: key.to_string(),
        };
        ipc::send_message(&mut stream, &get_msg)?;

        // Receive DataReady response
        let response = ipc::recv_message(&mut stream)?;

        match response {
            ServerMessage::DataReady {
                uuid,
                start_page,
                page_count,
                page_size: _,
                data_size,
            } => {
                // Receive object info
                let info_resp = ipc::recv_message(&mut stream)?;

                let (obj_name, obj_size, obj_type) = match &info_resp {
                    ServerMessage::ObjectInfo {
                        uuid: _,
                        name,
                        size,
                        content_type,
                        created_at: _,
                        tags: _,
                        block_count: _,
                    } => (name.clone(), *size, content_type.clone()),
                    ServerMessage::Error { code, message } => {
                        eprintln!("Error [{}]: {}", code, message);
                        // Still need to free pages: send DataDone
                        let _ = ipc::send_message(
                            &mut stream,
                            &ClientMessage::DataDone {
                                uuid,
                                pages_used: page_count,
                            },
                        );
                        return Ok(());
                    }
                    _ => {
                        eprintln!("Unexpected response: {:?}", info_resp);
                        let _ = ipc::send_message(
                            &mut stream,
                            &ClientMessage::DataDone {
                                uuid: uuid.clone(),
                                pages_used: page_count,
                            },
                        );
                        return Ok(());
                    }
                };

                // Open shared memory and read data
                let shm = SharedMemory::open(&self.config.shm_name)?;
                let data = shm.read_pages(start_page, page_count, data_size)?;

                // Send DataDone confirmation
                let done_msg = ClientMessage::DataDone {
                    uuid: uuid.clone(),
                    pages_used: page_count,
                };
                ipc::send_message(&mut stream, &done_msg)?;

                // Write to output file or stdout
                let out_path = output.unwrap_or(&obj_name);
                if out_path == "-" {
                    // Write to stdout
                    let stdout = io::stdout();
                    let mut handle = stdout.lock();
                    handle.write_all(&data)?;
                    handle.flush()?;
                } else {
                    std::fs::write(out_path, &data).map_err(|e| {
                        MiniOsError::Client(format!(
                            "Cannot write output file '{}': {}",
                            out_path, e
                        ))
                    })?;
                    println!(
                        "✓ Downloaded '{}' -> '{}' ({} bytes, type={})",
                        key,
                        out_path,
                        obj_size,
                        obj_type
                    );
                }
            }
            ServerMessage::Error { code, message } => {
                eprintln!("Error [{}]: {}", code, message);
            }
            other => {
                eprintln!("Unexpected response: {:?}", other);
            }
        }

        Ok(())
    }

    // --- DELETE ---
    fn cmd_delete(&self, key: &str) -> Result<()> {
        println!("Deleting '{}'...", key);

        let response = self.ipc.request(&ClientMessage::Delete {
            key: key.to_string(),
        })?;

        match response {
            ServerMessage::Ok { message } => {
                println!("✓ {}", message.unwrap_or_else(|| "Deleted".to_string()));
            }
            ServerMessage::Error { code, message } => {
                eprintln!("Error [{}]: {}", code, message);
            }
            _ => {
                eprintln!("Unexpected response: {:?}", response);
            }
        }

        Ok(())
    }

    // --- LIST ---
    fn cmd_list(&self, long_format: bool) -> Result<()> {
        let response = self.ipc.request(&ClientMessage::List)?;

        match response {
            ServerMessage::ObjectList { objects } => {
                if objects.is_empty() {
                    println!("No objects stored.");
                    return Ok(());
                }

                println!("Objects: {}\n", objects.len());

                if long_format {
                    // Detailed listing
                    println!(
                        "{:<38} {:<24} {:<12} {:<20} {:<16}",
                        "UUID", "Name", "Size", "Created", "Type"
                    );
                    println!("{}", "-".repeat(120));

                    for obj in &objects {
                        if let ServerMessage::ObjectInfo {
                            uuid,
                            name,
                            size,
                            content_type,
                            created_at,
                            tags: _,
                            block_count: _,
                        } = obj
                        {
                            println!(
                                "{:<38} {:<24} {:<12} {:<20} {:<16}",
                                uuid,
                                if name.len() > 23 {
                                    format!("{}...", &name[..20])
                                } else {
                                    name.clone()
                                },
                                human_readable_size(*size),
                                created_at,
                                content_type
                            );
                        }
                    }
                } else {
                    // Short listing
                    println!(
                        "{:<38} {:<24} {:<12}",
                        "UUID", "Name", "Size"
                    );
                    println!("{}", "-".repeat(80));

                    for obj in &objects {
                        if let ServerMessage::ObjectInfo {
                            uuid,
                            name,
                            size,
                            content_type: _,
                            created_at: _,
                            tags: _,
                            block_count: _,
                        } = obj
                        {
                            println!(
                                "{:<38} {:<24} {:<12}",
                                uuid,
                                if name.len() > 23 {
                                    format!("{}...", &name[..20])
                                } else {
                                    name.clone()
                                },
                                human_readable_size(*size),
                            );
                        }
                    }
                }
            }
            ServerMessage::Error { code, message } => {
                eprintln!("Error [{}]: {}", code, message);
            }
            _ => {
                eprintln!("Unexpected response: {:?}", response);
            }
        }

        Ok(())
    }

    // --- STATUS ---
    fn cmd_status(&self) -> Result<()> {
        let response = self.ipc.request(&ClientMessage::Status)?;

        match response {
            ServerMessage::Status {
                total_blocks,
                free_blocks,
                used_blocks,
                block_size,
                object_count,
                max_objects,
                total_capacity,
                used_capacity,
                free_capacity,
                cache_hits,
                cache_misses,
                cache_hit_rate,
                cache_evictions,
                cache_size,
                cache_capacity,
                cache_algorithm,
                shm_pages_total,
                shm_pages_free,
                uptime_seconds,
            } => {
                let uptime_mins = uptime_seconds / 60;
                let uptime_secs = uptime_seconds % 60;

                println!("=== MiniOS Server Status ===");
                println!();
                println!("  Uptime:           {}m {}s", uptime_mins, uptime_secs);
                println!();
                println!("  --- Storage ---");
                println!(
                    "  Objects:          {} / {}",
                    object_count, max_objects
                );
                println!(
                    "  Capacity:         {} / {} ({} free)",
                    human_readable_size(used_capacity),
                    human_readable_size(total_capacity),
                    human_readable_size(free_capacity),
                );
                println!(
                    "  Data blocks:      {} used / {} free / {} total ({} each)",
                    used_blocks,
                    free_blocks,
                    total_blocks,
                    human_readable_size(block_size as u64),
                );
                println!();
                println!("  --- Cache ---");
                println!("  Algorithm:        {}", cache_algorithm);
                println!(
                    "  Entries:          {} / {}",
                    cache_size, cache_capacity
                );
                println!("  Hits:             {}", cache_hits);
                println!("  Misses:           {}", cache_misses);
                println!("  Evictions:        {}", cache_evictions);
                println!("  Hit rate:         {:.2}%", cache_hit_rate);
                println!();
                println!("  --- Shared Memory ---");
                println!(
                    "  Pages:            {} free / {} total",
                    shm_pages_free, shm_pages_total
                );
            }
            ServerMessage::Error { code, message } => {
                eprintln!("Error [{}]: {}", code, message);
            }
            _ => {
                eprintln!("Unexpected response: {:?}", response);
            }
        }

        Ok(())
    }

    // --- CACHE RESIZE ---
    fn cmd_cache_resize(&self, capacity: usize) -> Result<()> {
        println!("Resizing cache to {} entries...", capacity);
        let response = self.ipc.request(&ClientMessage::CacheResize { capacity })?;
        match response {
            ServerMessage::Ok { message } => {
                println!("✓ {}", message.unwrap_or_default());
            }
            ServerMessage::Error { code, message } => {
                eprintln!("Error [{}]: {}", code, message);
            }
            _ => { eprintln!("Unexpected response: {:?}", response); }
        }
        Ok(())
    }

    // --- CACHE SWITCH ---
    fn cmd_cache_switch(&self, algorithm: &str) -> Result<()> {
        println!("Switching cache algorithm to {}...", algorithm);
        let response = self.ipc.request(&ClientMessage::CacheSwitch {
            algorithm: algorithm.to_string(),
        })?;
        match response {
            ServerMessage::Ok { message } => {
                println!("✓ {}", message.unwrap_or_default());
            }
            ServerMessage::Error { code, message } => {
                eprintln!("Error [{}]: {}", code, message);
            }
            _ => { eprintln!("Unexpected response: {:?}", response); }
        }
        Ok(())
    }

    // --- CACHE BENCHMARK ---
    fn cmd_cache_benchmark(&self, iterations: usize, sweep: bool) -> Result<()> {
        let mode = if sweep { "sweep" } else { "standard" };
        println!("Running cache benchmark ({} iterations, {} mode)...\n", iterations, mode);
        let response = self.ipc.request(&ClientMessage::CacheBenchmark { iterations, sweep })?;
        match response {
            ServerMessage::CacheBenchmarkResult {
                benchmarks,
                workload_keys,
                iterations: actual_iters,
            } => {
                println!("  Workload:  {} unique objects, {} iterations", workload_keys, actual_iters);
                println!();
                println!("  {:<6} {:<12} {:<12} {:<12} {:<12}",
                         "Rank", "Algorithm", "Hits", "Misses", "Hit Rate");
                println!("  {:-<60}", "");
                for (i, b) in benchmarks.iter().enumerate() {
                    let rank_str = match i {
                        0 => "1st",
                        1 => "2nd",
                        2 => "3rd",
                        _ => "",
                    };
                    let rank_str = if rank_str.is_empty() {
                        format!("{}.", i + 1)
                    } else {
                        rank_str.to_string()
                    };
                    println!(
                        "  {:<6} {:<12} {:<12} {:<12} {:.2}%",
                        rank_str, b.algorithm, b.hits, b.misses, b.hit_rate,
                    );
                }
                println!();
                if let Some(best) = benchmarks.first() {
                    println!("  Best algorithm: {} ({:.2}% hit rate)", best.algorithm, best.hit_rate);
                }
            }
            ServerMessage::CacheBenchmarkSweep {
                rows,
                workload_keys,
                iterations: actual_iters,
            } => {
                println!("  Workload:  {} unique objects, {} iterations", workload_keys, actual_iters);
                println!();
                // Group by capacity for readability
                println!("  {:<8} {:<8} {:<10} {:<10} {:<10}",
                         "Capacity", "Alg", "Hits", "Misses", "Hit Rate");
                println!("  {:-<52}", "");
                for row in &rows {
                    println!(
                        "  {:<8} {:<8} {:<10} {:<10} {:.2}%",
                        row.capacity, row.algorithm, row.hits, row.misses, row.hit_rate,
                    );
                }
                println!();
                // Show the top 3 overall
                println!("  Top 3 (algorithm, capacity) configurations:");
                for (i, row) in rows.iter().take(3).enumerate() {
                    println!("    {}. {} @ cap={} → {:.2}% hit rate",
                             i + 1, row.algorithm, row.capacity, row.hit_rate);
                }
            }
            ServerMessage::Error { code, message } => {
                eprintln!("Error [{}]: {}", code, message);
            }
            _ => { eprintln!("Unexpected response: {:?}", response); }
        }
        Ok(())
    }

    // --- START ---
    fn cmd_start(&self, daemon: bool) -> Result<()> {
        // Check if server is already running
        if let Ok(mut stream) = UnixStream::connect(&self.config.socket_path) {
            // Server appears to be running
            println!("Server is already running (socket {} exists).", self.config.socket_path);
            let _ = ipc::send_message(&mut stream, &ClientMessage::Status);
            if let Ok(resp) = ipc::recv_message(&mut stream) {
                if let ServerMessage::Status { uptime_seconds, .. } = resp {
                    println!("  Uptime: {}s", uptime_seconds);
                }
            }
            return Ok(());
        }

        // Start the server
        if daemon {
            println!("Starting MiniOS server in daemon mode...");
            self.launch_daemon()?;
            // Wait a moment for the server to start
            std::thread::sleep(std::time::Duration::from_millis(500));

            // Verify it started
            match UnixStream::connect(&self.config.socket_path) {
                Ok(_) => println!("✓ Server started successfully."),
                Err(_) => eprintln!("⚠ Server may not have started. Check logs."),
            }
        } else {
            println!("Starting MiniOS server...");
            // Run server in foreground
            let mut server = crate::server::Server::new(self.config.clone())?;
            server.start()?;

            // Block until stopped
            println!("Server running. Press Ctrl+C to stop.");
            while server.is_running() {
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            server.stop()?;
            println!("Server stopped.");
        }

        Ok(())
    }

    /// Launch the server as a daemon process
    fn launch_daemon(&self) -> Result<()> {
        use std::process::Command;

        // Get the current executable path
        let exe = std::env::current_exe().map_err(|e| {
            MiniOsError::Client(format!("Cannot determine executable path: {}", e))
        })?;

        // Launch as background process with --server flag
        let child = Command::new(&exe)
            .arg("--server")
            .arg("--socket-path")
            .arg(&self.config.socket_path)
            .arg("--shm-name")
            .arg(&self.config.shm_name)
            .arg("--shm-size")
            .arg(self.config.shm_size.to_string())
            .arg("--page-size")
            .arg(self.config.page_size.to_string())
            .arg("--store-path")
            .arg(&self.config.store_path)
            .arg("--block-size")
            .arg(self.config.block_size.to_string())
            .arg("--total-blocks")
            .arg(self.config.total_blocks.to_string())
            .arg("--max-objects")
            .arg(self.config.max_objects.to_string())
            .arg("--cache-capacity")
            .arg(self.config.cache_capacity.to_string())
            .arg("--cache-warmup")
            .arg(self.config.cache_warmup.to_string())
            .arg("--log-level")
            .arg(&self.config.log_level)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|e| {
                MiniOsError::Client(format!("Failed to start daemon: {}", e))
            })?;

        let pid = child.id();
        server::write_pid_file(&self.config.pid_file)?;
        info!("Server daemon started with PID {}", pid);

        Ok(())
    }

    // --- STOP ---
    fn cmd_stop(&self) -> Result<()> {
        println!("Stopping MiniOS server...");

        match self.ipc.request(&ClientMessage::Stop) {
            Ok(response) => match response {
                ServerMessage::Ok { message } => {
                    println!("✓ {}", message.unwrap_or_else(|| "Server stopped".to_string()));
                    // Clean up PID file
                    server::remove_pid_file(&self.config.pid_file);
                }
                ServerMessage::Error { code, message } => {
                    eprintln!("Error [{}]: {}", code, message);
                    // Try to kill by PID
                    self.force_stop();
                }
                _ => {}
            },
            Err(e) => {
                eprintln!("Could not connect to server: {}", e);
                self.force_stop();
            }
        }

        Ok(())
    }

    /// Force-stop the server by reading PID file and sending SIGTERM
    fn force_stop(&self) {
        if let Some(pid) = server::read_pid_file(&self.config.pid_file) {
            if server::is_process_running(pid) {
                println!("Sending SIGTERM to PID {}...", pid);
                unsafe {
                    libc::kill(pid as libc::pid_t, libc::SIGTERM);
                }
                std::thread::sleep(std::time::Duration::from_millis(500));

                if server::is_process_running(pid) {
                    println!("Process didn't stop, sending SIGKILL...");
                    unsafe {
                        libc::kill(pid as libc::pid_t, libc::SIGKILL);
                    }
                }
                println!("✓ Server process {} terminated.", pid);
            } else {
                println!("Server process {} is not running.", pid);
            }
        }
        server::remove_pid_file(&self.config.pid_file);
    }
}

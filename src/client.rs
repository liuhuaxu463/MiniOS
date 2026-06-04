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

/// MiniOS CLI 客户端
pub struct Client {
    config: CliArgs,
    ipc: IpcClient,
}

impl Client {
    /// 从配置创建新的客户端实例
    pub fn new(config: CliArgs) -> Self {
        let ipc = IpcClient::new(&config.socket_path);
        Self { config, ipc }
    }

    /// 执行请求的命令，根据命令类型分发到对应的处理方法
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

            crate::config::ClientCommand::Search { name, tag, content_type, after, before } => {
                self.cmd_search(name.as_deref(), tag.as_deref(), content_type.as_deref(), after.as_deref(), before.as_deref())
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

    // --- PUT（上传）---

    /// 执行 PUT（上传）命令：验证标签 JSON、读取本地文件、通过 IPC 发送到服务器，
    /// 将文件数据写入共享内存，并接收服务器返回的对象信息
    fn cmd_put(
        &self,
        name: &str,
        file_path: &str,
        content_type: &str,
        tags: &str,
    ) -> Result<()> {
        // 验证标签 JSON 格式
        if tags != "{}" {
            serde_json::from_str::<serde_json::Value>(tags).map_err(|e| {
                MiniOsError::InvalidArgument(format!("Invalid tags JSON: {}", e))
            })?;
        }

        // 读取文件
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

        // 连接到服务器
        let mut stream = self.ipc.connect()?;

        // 发送 PUT 请求
        let put_msg = ClientMessage::Put {
            name: name.to_string(),
            size: data_size,
            content_type: content_type.to_string(),
            tags: tags.to_string(),
        };
        ipc::send_message(&mut stream, &put_msg)?;

        // 接收 DataReady 响应（服务器已分配共享内存页）
        let response = ipc::recv_message(&mut stream)?;

        match response {
            ServerMessage::DataReady {
                uuid,
                start_page,
                page_count,
                page_size: _,
                data_size: _,
            } => {
                // 打开共享内存并写入数据
                let shm = SharedMemory::open(&self.config.shm_name)?;
                shm.write_pages(start_page, &data)?;

                println!(
                    "  Wrote {} bytes to shared memory (pages {}-{})",
                    data.len(),
                    start_page,
                    start_page + page_count - 1
                );

                // 发送 DataDone 确认
                let done_msg = ClientMessage::DataDone {
                    uuid: uuid.clone(),
                    pages_used: page_count,
                };
                ipc::send_message(&mut stream, &done_msg)?;

                // 接收最终响应（对象信息）
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

    // --- GET（下载）---

    /// 执行 GET（下载）命令：向服务器请求指定键的对象数据，
    /// 从共享内存读取数据，并写入到输出文件或标准输出
    fn cmd_get(&self, key: &str, output: Option<&str>) -> Result<()> {
        println!("Downloading '{}'...", key);

        let mut stream = self.ipc.connect()?;

        // 发送 GET 请求
        let get_msg = ClientMessage::Get {
            key: key.to_string(),
        };
        ipc::send_message(&mut stream, &get_msg)?;

        // 接收 DataReady 响应
        let response = ipc::recv_message(&mut stream)?;

        match response {
            ServerMessage::DataReady {
                uuid,
                start_page,
                page_count,
                page_size: _,
                data_size,
            } => {
                // 接收对象信息
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
                        // 仍需释放页面：发送 DataDone
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

                // 打开共享内存并读取数据
                let shm = SharedMemory::open(&self.config.shm_name)?;
                let data = shm.read_pages(start_page, page_count, data_size)?;

                // 发送 DataDone 确认
                let done_msg = ClientMessage::DataDone {
                    uuid: uuid.clone(),
                    pages_used: page_count,
                };
                ipc::send_message(&mut stream, &done_msg)?;

                // 写入输出文件或标准输出
                let out_path = output.unwrap_or(&obj_name);
                if out_path == "-" {
                    // 写入标准输出
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

    // --- DELETE（删除）---

    /// 执行 DELETE（删除）命令：向服务器发送删除请求，移除指定键的存储对象
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

    // --- LIST（列表）---

    /// 执行 LIST（列表）命令：查询服务器中所有存储对象的列表，
    /// 支持简短格式和详细格式两种显示模式
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
                    // 详细列表
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
                    // 简短列表
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

    // --- SEARCH（搜索）---

    /// 执行 SEARCH（搜索）命令：根据名称、标签、内容类型和时间范围等条件
    /// 搜索匹配的存储对象，并以表格形式展示结果
    fn cmd_search(&self, name: Option<&str>, tag: Option<&str>, content_type: Option<&str>, after: Option<&str>, before: Option<&str>) -> Result<()> {
        let response = self.ipc.request(&ClientMessage::Search {
            name: name.map(|s| s.to_string()),
            tag: tag.map(|s| s.to_string()),
            content_type: content_type.map(|s| s.to_string()),
            after: after.map(|s| s.to_string()),
            before: before.map(|s| s.to_string()),
        })?;

        match response {
            ServerMessage::ObjectList { objects } => {
                if objects.is_empty() {
                    println!("没有匹配的对象。");
                    return Ok(());
                }
                println!("找到 {} 个对象:\n", objects.len());
                println!("{:<38} {:<24} {:<12} {:<20} {:<16}",
                         "UUID", "名称", "大小", "创建时间", "类型");
                println!("{}", "-".repeat(120));
                for obj in &objects {
                    if let ServerMessage::ObjectInfo { uuid, name, size, content_type, created_at, tags: _, block_count: _ } = obj {
                        println!("{:<38} {:<24} {:<12} {:<20} {:<16}",
                                 uuid,
                                 if name.len() > 23 { format!("{}...", &name[..20]) } else { name.clone() },
                                 human_readable_size(*size),
                                 created_at,
                                 content_type);
                    }
                }
            }
            ServerMessage::Error { code, message } => {
                eprintln!("错误 [{}]: {}", code, message);
            }
            _ => { eprintln!("意外响应: {:?}", response); }
        }
        Ok(())
    }

    // --- STATUS（状态）---

    /// 执行 STATUS（状态）命令：查询并显示服务器的运行状态，
    /// 包括运行时间、存储信息、缓存统计和共享内存使用情况
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

    // --- CACHE RESIZE（缓存调整）---

    /// 执行 CACHE RESIZE（缓存调整）命令：调整服务器端缓存的容量（条目数）
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

    // --- CACHE SWITCH（缓存切换）---

    /// 执行 CACHE SWITCH（缓存切换）命令：切换服务器端缓存所使用的淘汰算法
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

    // --- CACHE BENCHMARK（缓存基准测试）---

    /// 执行 CACHE BENCHMARK（缓存基准测试）命令：在服务器端运行缓存性能基准测试，
    /// 支持标准模式和扫描（sweep）模式，对比不同缓存算法在不同容量下的命中率
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
                // 按容量分组以便阅读
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
                // 显示总体排名前 3
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

    // --- START（启动）---

    /// 执行 START（启动）命令：启动 MiniOS 服务器。
    /// 支持守护进程（daemon）模式和前台模式：
    /// - 守护进程模式：以后台子进程方式启动服务器
    /// - 前台模式：在当前进程中启动服务器并阻塞直到停止
    fn cmd_start(&self, daemon: bool) -> Result<()> {
        // 检查服务器是否已在运行
        if let Ok(mut stream) = UnixStream::connect(&self.config.socket_path) {
            // 服务器似乎正在运行
            println!("Server is already running (socket {} exists).", self.config.socket_path);
            let _ = ipc::send_message(&mut stream, &ClientMessage::Status);
            if let Ok(resp) = ipc::recv_message(&mut stream) {
                if let ServerMessage::Status { uptime_seconds, .. } = resp {
                    println!("  Uptime: {}s", uptime_seconds);
                }
            }
            return Ok(());
        }

        // 启动服务器
        if daemon {
            println!("Starting MiniOS server in daemon mode...");
            self.launch_daemon()?;
            // 等待服务器启动
            std::thread::sleep(std::time::Duration::from_millis(500));

            // 验证服务器是否已启动
            match UnixStream::connect(&self.config.socket_path) {
                Ok(_) => println!("✓ Server started successfully."),
                Err(_) => eprintln!("⚠ Server may not have started. Check logs."),
            }
        } else {
            println!("Starting MiniOS server...");
            // 在前台运行服务器
            let mut server = crate::server::Server::new(self.config.clone())?;
            server.start()?;

            // 阻塞直到停止
            println!("Server running. Press Ctrl+C to stop.");
            while server.is_running() {
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            server.stop()?;
            println!("Server stopped.");
        }

        Ok(())
    }

    /// 以后台守护进程方式启动服务器：
    /// 获取当前可执行文件路径，使用 --server 标志以子进程方式启动，
    /// 并将标准输入输出重定向到 /dev/null，最后写入 PID 文件
    fn launch_daemon(&self) -> Result<()> {
        use std::process::Command;

        // 获取当前可执行文件路径
        let exe = std::env::current_exe().map_err(|e| {
            MiniOsError::Client(format!("Cannot determine executable path: {}", e))
        })?;

        // 使用 --server 标志以后台进程启动
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

    // --- STOP（停止）---

    /// 执行 STOP（停止）命令：通过 IPC 向服务器发送停止请求，
    /// 如果无法正常停止则通过 PID 文件强制终止服务器进程
    fn cmd_stop(&self) -> Result<()> {
        println!("Stopping MiniOS server...");

        match self.ipc.request(&ClientMessage::Stop) {
            Ok(response) => match response {
                ServerMessage::Ok { message } => {
                    println!("✓ {}", message.unwrap_or_else(|| "Server stopped".to_string()));
                    // 清理 PID 文件
                    server::remove_pid_file(&self.config.pid_file);
                }
                ServerMessage::Error { code, message } => {
                    eprintln!("Error [{}]: {}", code, message);
                    // 尝试通过 PID 终止
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

    /// 通过读取 PID 文件并发送 SIGTERM 强制停止服务器。
    /// 如果进程未响应 SIGTERM，则发送 SIGKILL 强制终止，
    /// 最后清理 PID 文件
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

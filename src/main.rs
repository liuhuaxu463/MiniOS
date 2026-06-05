//! MiniOS 程序入口。
//!
//! 根据命令行参数决定以服务器模式还是客户端模式运行。
//! 服务器模式启动 IPC 监听、Web 管理界面和 Prometheus 监控；
//! 客户端模式执行用户的子命令（put/get/delete/list/search 等）。

mod access_log;
mod cache;
mod client;
mod config;
mod error;
mod ipc;
mod metrics;
mod server;
mod shm;
mod storage;

use clap::Parser;
use config::{CliArgs, ClientCommand};
use log::{error, info, LevelFilter};
use std::io::Write;
use std::path::Path;

/// 程序主入口。
///
/// 1. 解析命令行参数
/// 2. 初始化日志系统（env_logger）
/// 3. 根据 `--server` 标志决定运行模式
fn main() {
    let args = CliArgs::parse();

    // 初始化日志系统
    let log_level = match args.log_level.to_lowercase().as_str() {
        "trace" => LevelFilter::Trace,
        "debug" => LevelFilter::Debug,
        "info"  => LevelFilter::Info,
        "warn"  => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        _       => LevelFilter::Info,
    };

    env_logger::Builder::new()
        .filter_level(log_level)
        .format(|buf, record| {
            writeln!(
                buf,
                "[{} {} {}] {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.target(),
                record.args()
            )
        })
        .init();

    if args.server_mode {
        run_server(args);
    } else if let Some(cmd) = args.command.clone() {
        run_client_command(args, &cmd);
    } else {
        // 无参数时打印帮助
        eprintln!("MiniOS - 轻量级对象存储服务");
        eprintln!();
        eprintln!("用法:");
        eprintln!("  服务器模式:  minios --server [参数]");
        eprintln!("  客户端模式:  minios <子命令> [参数]");
        eprintln!();
        eprintln!("子命令:");
        eprintln!("  put     上传对象");
        eprintln!("  get     下载对象");
        eprintln!("  delete  删除对象");
        eprintln!("  list    列出所有对象");
        eprintln!("  search  搜索对象");
        eprintln!("  status  查询服务器状态");
        eprintln!("  start   启动服务器");
        eprintln!("  stop    停止服务器");
        eprintln!();
        eprintln!("运行 'minios --help' 查看完整参数列表。");
        std::process::exit(1);
    }
}

/// 以服务器模式运行。
///
/// 启动 IPC 服务器、Web 管理界面（含 Prometheus 监控端点）、
/// 多线程工作池，并处理 SIGINT 信号实现优雅关闭。
fn run_server(args: CliArgs) {
    // 如果指定了 --daemonize，先将相对路径转为绝对路径，
    // 然后通过 double-fork 脱离终端
    let mut args = args;
    if args.daemonize {
        // 把相对路径在 fork 前转化为绝对路径，避免 chdir 后丢失
        args.store_path = to_absolute(&args.store_path);
        args.access_log = to_absolute_opt(&args.access_log);
        daemonize_process();
    }

    info!("正在启动 MiniOS 服务器...");
    info!("  版本: {}", env!("CARGO_PKG_VERSION"));
    info!("  数据文件: {}", args.store_path);
    info!("  Socket:   {}", args.socket_path);
    info!("  共享内存: {} ({} 字节)", args.shm_name, args.shm_size);

    // 保存 pid_file 路径（args 将被移动）
    let pid_file = args.pid_file.clone();

    // 写入 PID 文件（守护进程模式下，fork 后重新写入新的 PID）
    if let Err(e) = server::write_pid_file(&pid_file) {
        error!("无法写入 PID 文件: {}", e);
    }

    // 注册 SIGINT（Ctrl+C）处理，实现优雅关闭
    let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        info!("收到中断信号，正在优雅关闭...");
        r.store(false, std::sync::atomic::Ordering::SeqCst);
    }).ok();

    match server::Server::new(args) {
        Ok(mut srv) => {
            if let Err(e) = srv.start() {
                error!("服务器启动失败: {}", e);
                std::process::exit(1);
            }

            info!("服务器已就绪，等待客户端连接...");

            // 主循环：每 500ms 检查一次是否应该退出
            while srv.is_running() && running.load(std::sync::atomic::Ordering::SeqCst) {
                std::thread::sleep(std::time::Duration::from_millis(500));
            }

            if let Err(e) = srv.stop() {
                error!("关闭过程中发生错误: {}", e);
            }

            info!("服务器已完全关闭。");
        }
        Err(e) => {
            error!("创建服务器实例失败: {}", e);
            std::process::exit(1);
        }
    }

    server::remove_pid_file(&pid_file);
}

/// 以客户端模式运行。
///
/// 解析用户子命令并发送给服务器执行。
fn run_client_command(args: CliArgs, cmd: &ClientCommand) {
    let client = client::Client::new(args);

    if let Err(e) = client.execute(cmd) {
        error!("{}", e);
        std::process::exit(1);
    }
}

/// 将相对路径转为绝对路径。
fn to_absolute(path: &str) -> String {
    if path.is_empty() || Path::new(path).is_absolute() {
        return path.to_string();
    }
    std::env::current_dir()
        .map(|d| d.join(path).to_string_lossy().to_string())
        .unwrap_or_else(|_| path.to_string())
}

/// 将可选路径转为绝对路径（空字符串保持为空）。
fn to_absolute_opt(path: &str) -> String {
    if path.is_empty() { return String::new(); }
    to_absolute(path)
}

/// 通过经典的双重 fork 将当前进程转变为守护进程。
///
/// 步骤：
/// 1. 第一次 fork — 父进程退出，子进程被 init 接管
/// 2. setsid() — 创建新会话，成为会话首领，脱离终端
/// 3. 第二次 fork — 子进程退出，孙进程永远不会重新获取终端
/// 4. chdir("/") — 避免占用文件系统的挂载点
/// 5. 重定向 stdin/stdout/stderr 到 /dev/null
fn daemonize_process() {
    // 第一次 fork
    match unsafe { libc::fork() } {
        -1 => {
            eprintln!("守护进程化失败：第一次 fork 出错");
            std::process::exit(1);
        }
        0 => {
            // 子进程：继续向下执行
        }
        _ => {
            // 父进程：立即退出
            std::process::exit(0);
        }
    }

    // 创建新会话，脱离原始终端
    if unsafe { libc::setsid() } == -1 {
        eprintln!("守护进程化失败：setsid 出错");
        std::process::exit(1);
    }

    // 第二次 fork — 确保进程永远不会重新获取控制终端
    match unsafe { libc::fork() } {
        -1 => {
            eprintln!("守护进程化失败：第二次 fork 出错");
            std::process::exit(1);
        }
        0 => {
            // 孙进程：继续执行
        }
        _ => {
            // 第一个子进程：退出
            std::process::exit(0);
        }
    }

    // 重定向标准输入/输出/错误到 /dev/null。
    // 注意：不执行 chdir("/")，因为用户可能指定了相对路径的
    // store.odb，切换根目录后会导致文件无处可写。
    let dev_null = unsafe { libc::open(b"/dev/null\0".as_ptr() as *const libc::c_char, libc::O_RDWR) };
    if dev_null >= 0 {
        unsafe {
            libc::dup2(dev_null, 0);  // stdin
            libc::dup2(dev_null, 1);  // stdout
            libc::dup2(dev_null, 2);  // stderr
            if dev_null > 2 { libc::close(dev_null); }
        }
    }
}

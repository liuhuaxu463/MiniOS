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
    info!("正在启动 MiniOS 服务器...");
    info!("  版本: {}", env!("CARGO_PKG_VERSION"));
    info!("  数据文件: {}", args.store_path);
    info!("  Socket:   {}", args.socket_path);
    info!("  共享内存: {} ({} 字节)", args.shm_name, args.shm_size);

    // 保存 pid_file 路径（args 将被移动）
    let pid_file = args.pid_file.clone();

    // 写入 PID 文件
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

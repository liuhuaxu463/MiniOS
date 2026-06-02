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

fn main() {
    let args = CliArgs::parse();

    // Initialize logging
    let log_level = match args.log_level.to_lowercase().as_str() {
        "trace" => LevelFilter::Trace,
        "debug" => LevelFilter::Debug,
        "info" => LevelFilter::Info,
        "warn" => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        _ => LevelFilter::Info,
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

    // Determine mode: server or client
    if args.server_mode {
        run_server(args);
    } else if let Some(cmd) = args.command.clone() {
        run_client_command(args, &cmd);
    } else {
        eprintln!("MiniOS - Mini Object Storage Service");
        eprintln!();
        eprintln!("Usage:");
        eprintln!("  Server mode:  minios --server [OPTIONS]");
        eprintln!("  Client mode:  minios <COMMAND> [OPTIONS]");
        eprintln!();
        eprintln!("Commands:");
        eprintln!("  put     Upload an object");
        eprintln!("  get     Download an object");
        eprintln!("  delete  Delete an object");
        eprintln!("  list    List all objects");
        eprintln!("  status  Show server status");
        eprintln!("  start   Start the server");
        eprintln!("  stop    Stop the server");
        eprintln!();
        eprintln!("Run 'minios --help' for full options.");
        std::process::exit(1);
    }
}

fn run_server(args: CliArgs) {
    info!("Starting MiniOS server...");
    info!("  Version: {}", env!("CARGO_PKG_VERSION"));
    info!("  Store:   {}", args.store_path);
    info!("  Socket:  {}", args.socket_path);
    info!("  SHM:     {} ({} bytes)", args.shm_name, args.shm_size);

    // Save pid_file path before args is moved into Server::new()
    let pid_file = args.pid_file.clone();

    // Write PID file
    if let Err(e) = server::write_pid_file(&pid_file) {
        error!("Could not write PID file: {}", e);
    }

    // Register signal handlers for graceful shutdown
    let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        info!("Received interrupt signal, shutting down...");
        r.store(false, std::sync::atomic::Ordering::SeqCst);
    })
    .ok();

    match server::Server::new(args) {
        Ok(mut srv) => {
            if let Err(e) = srv.start() {
                error!("Failed to start server: {}", e);
                std::process::exit(1);
            }

            info!("Server is ready. Waiting for connections...");

            // Keep the main thread alive until stopped
            while srv.is_running() && running.load(std::sync::atomic::Ordering::SeqCst) {
                std::thread::sleep(std::time::Duration::from_millis(500));
            }

            if let Err(e) = srv.stop() {
                error!("Error during shutdown: {}", e);
            }

            info!("Server shutdown complete.");
        }
        Err(e) => {
            error!("Failed to create server: {}", e);
            std::process::exit(1);
        }
    }

    // Clean up PID file
    server::remove_pid_file(&pid_file);
}

fn run_client_command(args: CliArgs, cmd: &ClientCommand) {
    let client = client::Client::new(args);

    if let Err(e) = client.execute(cmd) {
        error!("{}", e);
        std::process::exit(1);
    }
}

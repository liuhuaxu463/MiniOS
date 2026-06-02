use crate::cache::ObjectCache;
use crate::shm::SharedMemory;
use crate::storage::SharedStorage;
use log::{error, info};
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

/// Prometheus metrics server.
///
/// Listens on a TCP port and serves a `/metrics` endpoint in
/// Prometheus text format, plus a simple HTML dashboard at `/`.
pub struct MetricsServer {
    port: u16,
    running: Arc<AtomicBool>,
}

impl MetricsServer {
    pub fn new(port: u16) -> Self {
        Self {
            port,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Start the metrics server in a background thread.
    pub fn start(
        &mut self,
        storage: SharedStorage,
        cache: Arc<ObjectCache>,
        shm: Arc<SharedMemory>,
        start_time: Instant,
    ) {
        if self.port == 0 {
            info!("Metrics server disabled (port=0)");
            return;
        }

        self.running.store(true, Ordering::SeqCst);
        let running = self.running.clone();
        let port = self.port;

        thread::spawn(move || {
            let addr = format!("0.0.0.0:{}", port);
            let listener = match TcpListener::bind(&addr) {
                Ok(l) => l,
                Err(e) => {
                    error!("Metrics server: cannot bind to {}: {}", addr, e);
                    return;
                }
            };

            info!("Prometheus metrics server listening on http://0.0.0.0:{}", port);

            for stream in listener.incoming() {
                if !running.load(Ordering::SeqCst) {
                    break;
                }
                match stream {
                    Ok(s) => {
                        let s2 = storage.clone();
                        let c2 = cache.clone();
                        let sh2 = shm.clone();
                        thread::spawn(move || handle_http(s, &s2, &c2, &sh2, start_time));
                    }
                    Err(e) => {
                        error!("Metrics accept error: {}", e);
                    }
                }
            }
        });
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
    }
}

fn handle_http(
    mut stream: TcpStream,
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
    shm: &Arc<SharedMemory>,
    start_time: Instant,
) {
    use std::io::Read;
    let mut buf = [0u8; 4096];
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).ok();
    let n = match stream.read(&mut buf) {
        Ok(n) if n > 0 => n,
        _ => return,
    };

    let req = String::from_utf8_lossy(&buf[..n]);
    let first_line = req.lines().next().unwrap_or("");

    let (status_line, content_type, body) = if first_line.starts_with("GET /metrics") {
        ("200 OK", "text/plain; version=0.0.4", build_metrics(storage, cache, shm, start_time))
    } else if first_line.starts_with("GET / ") || first_line == "GET /" || first_line.starts_with("GET / HTTP") {
        ("200 OK", "text/html; charset=utf-8", build_dashboard(storage, cache, shm, start_time))
    } else {
        ("404 Not Found", "text/plain", "404 Not Found\n".to_string())
    };

    let response = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status_line, content_type, body.len(), body,
    );

    let _ = stream.write_all(response.as_bytes());
}

// ============================================================================
// Prometheus Metrics
// ============================================================================

fn build_metrics(
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
    shm: &Arc<SharedMemory>,
    start_time: Instant,
) -> String {
    let mut out = String::new();

    let status = {
        let st = storage.lock().unwrap();
        st.status()
    };
    let cache_stats = cache.stats();
    let uptime = start_time.elapsed().as_secs();

    // HELP/TYPE lines for each metric
    out.push_str("# HELP minios_uptime_seconds Server uptime in seconds\n");
    out.push_str("# TYPE minios_uptime_seconds gauge\n");
    out.push_str(&format!("minios_uptime_seconds {}\n", uptime));

    out.push_str("# HELP minios_objects_total Total stored objects\n");
    out.push_str("# TYPE minios_objects_total gauge\n");
    out.push_str(&format!("minios_objects_total {}\n", status.object_count));

    out.push_str("# HELP minios_storage_blocks_total Total data blocks\n");
    out.push_str("# TYPE minios_storage_blocks_total gauge\n");
    out.push_str(&format!("minios_storage_blocks_total {}\n", status.total_blocks));

    out.push_str("# HELP minios_storage_blocks_used Used data blocks\n");
    out.push_str("# TYPE minios_storage_blocks_used gauge\n");
    out.push_str(&format!("minios_storage_blocks_used {}\n", status.used_blocks));

    out.push_str("# HELP minios_storage_blocks_free Free data blocks\n");
    out.push_str("# TYPE minios_storage_blocks_free gauge\n");
    out.push_str(&format!("minios_storage_blocks_free {}\n", status.free_blocks));

    out.push_str("# HELP minios_storage_bytes_total Total capacity in bytes\n");
    out.push_str("# TYPE minios_storage_bytes_total gauge\n");
    out.push_str(&format!("minios_storage_bytes_total {}\n", status.total_capacity));

    out.push_str("# HELP minios_storage_bytes_used Used capacity in bytes\n");
    out.push_str("# TYPE minios_storage_bytes_used gauge\n");
    out.push_str(&format!("minios_storage_bytes_used {}\n", status.used_capacity));

    out.push_str("# HELP minios_cache_hits_total Total cache hits\n");
    out.push_str("# TYPE minios_cache_hits_total counter\n");
    out.push_str(&format!("minios_cache_hits_total {}\n", cache_stats.hits));

    out.push_str("# HELP minios_cache_misses_total Total cache misses\n");
    out.push_str("# TYPE minios_cache_misses_total counter\n");
    out.push_str(&format!("minios_cache_misses_total {}\n", cache_stats.misses));

    out.push_str("# HELP minios_cache_evictions_total Total cache evictions\n");
    out.push_str("# TYPE minios_cache_evictions_total counter\n");
    out.push_str(&format!("minios_cache_evictions_total {}\n", cache_stats.evictions));

    out.push_str("# HELP minios_cache_size Current number of cached entries\n");
    out.push_str("# TYPE minios_cache_size gauge\n");
    out.push_str(&format!("minios_cache_size {}\n", cache_stats.size));

    out.push_str("# HELP minios_cache_capacity Max cache capacity\n");
    out.push_str("# TYPE minios_cache_capacity gauge\n");
    out.push_str(&format!("minios_cache_capacity {}\n", cache_stats.capacity));

    out.push_str("# HELP minios_cache_hit_rate_percent Cache hit rate percentage\n");
    out.push_str("# TYPE minios_cache_hit_rate_percent gauge\n");
    out.push_str(&format!("minios_cache_hit_rate_percent {:.2}\n", cache_stats.hit_rate()));

    out.push_str(&format!("# HELP minios_cache_algorithm_info Cache algorithm in use\n"));
    out.push_str(&format!("# TYPE minios_cache_algorithm_info gauge\n"));
    out.push_str(&format!("minios_cache_algorithm_info{{algorithm=\"{}\"}} 1\n", cache_stats.algorithm));

    out.push_str("# HELP minios_shm_pages_total Total shared memory pages\n");
    out.push_str("# TYPE minios_shm_pages_total gauge\n");
    out.push_str(&format!("minios_shm_pages_total {}\n", shm.num_pages()));

    out.push_str("# HELP minios_shm_pages_free Free shared memory pages\n");
    out.push_str("# TYPE minios_shm_pages_free gauge\n");
    out.push_str(&format!("minios_shm_pages_free {}\n", shm.free_page_count()));

    out
}

// ============================================================================
// Simple HTML Dashboard
// ============================================================================

fn build_dashboard(
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
    shm: &Arc<SharedMemory>,
    start_time: Instant,
) -> String {
    let status = {
        let st = storage.lock().unwrap();
        st.status()
    };
    let cache_stats = cache.stats();
    let uptime = start_time.elapsed().as_secs();
    let uptime_m = uptime / 60;
    let uptime_s = uptime % 60;

    format!(
        r#"<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="5">
<title>MiniOS Dashboard</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         max-width: 800px; margin: 2em auto; padding: 0 1em;
         background: #f5f5f5; color: #333; }}
  h1 {{ color: #1a5276; border-bottom: 2px solid #1a5276; padding-bottom: 0.3em; }}
  .card {{ background: white; border-radius: 8px; padding: 1em 1.5em;
           margin: 1em 0; box-shadow: 0 1px 3px rgba(0,0,0,0.12); }}
  .card h2 {{ margin-top: 0; color: #2c3e50; font-size: 1.1em; }}
  table {{ width: 100%; border-collapse: collapse; }}
  td {{ padding: 4px 8px; }} td:first-child {{ color: #666; width: 40%; }}
  .bar {{ background: #e0e0e0; border-radius: 4px; height: 12px; overflow: hidden; }}
  .bar-fill {{ background: #27ae60; height: 100%; border-radius: 4px; transition: width .5s; }}
  .footer {{ text-align: center; color: #999; font-size: 0.85em; margin-top: 2em; }}
</style>
</head>
<body>
<h1>MiniOS Dashboard</h1>

<div class="card">
  <h2>📊 Overview</h2>
  <table>
    <tr><td>Uptime</td><td>{}m {}s</td></tr>
    <tr><td>Objects</td><td>{} / {}</td></tr>
  </table>
</div>

<div class="card">
  <h2>💾 Storage</h2>
  <table>
    <tr><td>Used</td><td>{} / {} ({:.1}%)</td></tr>
    <tr><td colspan="2"><div class="bar"><div class="bar-fill" style="width:{:.1}%"></div></div></td></tr>
    <tr><td>Data blocks</td><td>{} / {} ({} each)</td></tr>
  </table>
</div>

<div class="card">
  <h2>🗄️ Cache ({})</h2>
  <table>
    <tr><td>Entries</td><td>{} / {}</td></tr>
    <tr><td>Hits / Misses</td><td>{} / {}</td></tr>
    <tr><td>Evictions</td><td>{}</td></tr>
    <tr><td>Hit rate</td><td><strong>{:.2}%</strong></td></tr>
  </table>
</div>

<div class="card">
  <h2>🧠 Shared Memory</h2>
  <table>
    <tr><td>Pages free / total</td><td>{} / {}</td></tr>
  </table>
</div>

<div class="footer">
  MiniOS v{} · <a href="/metrics">Prometheus metrics</a>
</div>
</body>
</html>"#,
        uptime_m, uptime_s,
        status.object_count, status.max_objects,
        fmt_bytes(status.used_capacity), fmt_bytes(status.total_capacity),
        pct(status.used_capacity, status.total_capacity),
        pct(status.used_capacity, status.total_capacity),
        status.used_blocks, status.total_blocks, fmt_bytes(status.block_size as u64),
        cache_stats.algorithm,
        cache_stats.size, cache_stats.capacity,
        cache_stats.hits, cache_stats.misses,
        cache_stats.evictions,
        cache_stats.hit_rate(),
        shm.free_page_count(), shm.num_pages(),
        env!("CARGO_PKG_VERSION"),
    )
}

fn fmt_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB"];
    let mut v = bytes as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 { v /= 1024.0; u += 1; }
    format!("{:.2} {}", v, UNITS[u])
}

fn pct(part: u64, total: u64) -> f64 {
    if total == 0 { 0.0 } else { (part as f64 / total as f64) * 100.0 }
}

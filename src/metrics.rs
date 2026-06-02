use crate::cache::{CachedObject, ObjectCache, CacheAlgorithmType};
use crate::shm::SharedMemory;
use crate::storage::SharedStorage;
use log::{error, info};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

/// Prometheus metrics + interactive Web management server.
pub struct MetricsServer {
    port: u16,
    running: Arc<AtomicBool>,
}

impl MetricsServer {
    pub fn new(port: u16) -> Self {
        Self { port, running: Arc::new(AtomicBool::new(false)) }
    }

    pub fn start(
        &mut self,
        storage: SharedStorage,
        cache: Arc<ObjectCache>,
        shm: Arc<SharedMemory>,
        start_time: Instant,
    ) {
        if self.port == 0 { info!("Web server disabled (port=0)"); return; }
        self.running.store(true, Ordering::SeqCst);
        let running = self.running.clone();
        let port = self.port;
        thread::spawn(move || {
            let addr = format!("0.0.0.0:{}", port);
            let listener = match TcpListener::bind(&addr) {
                Ok(l) => l,
                Err(e) => { error!("Web server: cannot bind {}: {}", addr, e); return; }
            };
            info!("Web management UI: http://0.0.0.0:{}", port);
            for stream in listener.incoming() {
                if !running.load(Ordering::SeqCst) { break; }
                match stream {
                    Ok(s) => {
                        let st = storage.clone();
                        let ca = cache.clone();
                        let sh = shm.clone();
                        thread::spawn(move || dispatch(s, &st, &ca, &sh, start_time));
                    }
                    Err(e) => error!("Web accept error: {}", e),
                }
            }
        });
    }

    pub fn stop(&mut self) { self.running.store(false, Ordering::SeqCst); }
}

// ============================================================================
// Request routing
// ============================================================================

fn dispatch(
    mut stream: TcpStream,
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
    shm: &Arc<SharedMemory>,
    start_time: Instant,
) {
    stream.set_read_timeout(Some(std::time::Duration::from_secs(10))).ok();
    let mut buf = [0u8; 16384];
    let n = match stream.read(&mut buf) { Ok(n) if n > 0 => n, _ => return, };
    let req = String::from_utf8_lossy(&buf[..n]);
    let first_line = req.lines().next().unwrap_or("");
    let (method, path) = parse_first(first_line);

    let body_start = req.find("\r\n\r\n").map(|i| i + 4).unwrap_or(req.len());
    let body = &req[body_start..];

    match (method, path) {
        ("GET", "/metrics") => respond(&mut stream, "200 OK", "text/plain; version=0.0.4",
            &build_metrics(storage, cache, shm, start_time)),
        ("GET", "/") => respond(&mut stream, "200 OK", "text/html; charset=utf-8",
            &build_dashboard(storage, cache, shm, start_time)),
        ("GET", "/manage") => respond(&mut stream, "200 OK", "text/html; charset=utf-8",
            &build_manage_page(storage, cache)),
        ("POST", "/api/put") => handle_web_put(&mut stream, body, storage, cache),
        ("GET", "/api/get") => handle_web_get(&mut stream, &req, storage),
        ("GET", "/api/delete") => handle_web_delete(&mut stream, &req, storage, cache),
        ("POST", "/api/resize") => handle_web_resize(&mut stream, body, cache),
        ("GET", "/api/benchmark") => handle_web_benchmark(&mut stream, storage, cache),
        _ => respond(&mut stream, "404 Not Found", "text/plain", "404 Not Found\n"),
    }
}

fn parse_first(line: &str) -> (&str, &str) {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() >= 2 { (parts[0], parts[1]) } else { ("GET", "/") }
}

fn respond(stream: &mut TcpStream, status: &str, content_type: &str, body: &str) {
    let resp = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status, content_type, body.len(), body,
    );
    let _ = stream.write_all(resp.as_bytes());
}

fn get_query_param(req: &str, key: &str) -> Option<String> {
    let path = req.lines().next().unwrap_or("");
    let query_start = path.find('?').map(|i| i + 1).unwrap_or(0);
    if query_start == 0 { return None; }
    let query = &path[query_start..];
    let query = query.split_whitespace().next().unwrap_or("");
    let query = query.split(' ').next().unwrap_or(query); // trim HTTP version
    for pair in query.split('&') {
        let mut kv = pair.splitn(2, '=');
        let k = kv.next().unwrap_or("");
        let v = kv.next().unwrap_or("");
        if k == key { return Some(url_decode(v)); }
    }
    None
}

fn url_decode(s: &str) -> String {
    let mut out = String::new();
    let mut i = 0;
    let bytes = s.as_bytes();
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                if let Ok(hex) = u8::from_str_radix(&s[i+1..i+3], 16) {
                    out.push(hex as char);
                    i += 3;
                    continue;
                }
            }
            b'+' => { out.push(' '); i += 1; continue; }
            b => { out.push(b as char); }
        }
        i += 1;
    }
    out
}

fn get_form_field(body: &str, field: &str) -> Option<String> {
    for pair in body.split('&') {
        let mut kv = pair.splitn(2, '=');
        let k = kv.next().unwrap_or("");
        let v = kv.next().unwrap_or("");
        if k == field && !v.is_empty() {
            return Some(url_decode(v));
        }
    }
    None
}

// ============================================================================
// Web API handlers (call storage/cache directly, no Unix socket needed)
// ============================================================================

fn handle_web_put(
    stream: &mut TcpStream,
    body: &str,
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
) {
    let name = match get_form_field(body, "name") {
        Some(n) => n,
        None => { respond(stream, "400", "text/html", &page("Error", "<p>Missing 'name' field</p><a href='/manage'>Back</a>")); return; }
    };
    let content = match get_form_field(body, "content") {
        Some(c) => c,
        None => { respond(stream, "400", "text/html", &page("Error", "<p>Missing 'content' field</p><a href='/manage'>Back</a>")); return; }
    };
    let ctype = get_form_field(body, "type").unwrap_or_else(|| "text/plain".to_string());
    let tags = get_form_field(body, "tags").unwrap_or_else(|| "{}".to_string());

    let data = content.as_bytes();
    let mut st = storage.lock().unwrap();
    match st.put(&name, data, &ctype, &tags) {
        Ok(info) => {
            let cached = CachedObject {
                uuid: info.uuid.clone(), data: data.to_vec(), name: info.name.clone(),
                content_type: info.content_type.clone(), size: info.size, tags: info.tags.clone(),
            };
            cache.put(&info.uuid, cached);
            info!("Web PUT: name='{}' uuid={} size={}", name, info.uuid, info.size);
            respond(stream, "200 OK", "text/html",
                &page("Upload OK", &format!(
                    "<p style='color:green'>✓ Object stored!</p>
                     <table><tr><td>Name</td><td>{}</td></tr>
                     <tr><td>UUID</td><td>{}</td></tr>
                     <tr><td>Size</td><td>{} bytes</td></tr>
                     <tr><td>Type</td><td>{}</td></tr></table>
                     <p><a href='/manage'>← Back to Management</a></p>",
                    info.name, info.uuid, info.size, info.content_type)));
        }
        Err(e) => {
            error!("Web PUT failed: {}", e);
            respond(stream, "500", "text/html",
                &page("Error", &format!("<p style='color:red'>Failed: {}</p><a href='/manage'>Back</a>", e)));
        }
    }
}

fn handle_web_get(
    stream: &mut TcpStream,
    req: &str,
    storage: &SharedStorage,
) {
    let key = match get_query_param(req, "key") {
        Some(k) => k,
        None => {
            // List all objects as clickable links
            let mut st = storage.lock().unwrap();
            let objects = st.list().unwrap_or_default();
            let mut rows = String::new();
            for o in &objects {
                rows.push_str(&format!(
                    "<tr><td><a href='/api/get?key={}'>{}</a></td><td>{}</td><td>{} B</td><td>{}</td></tr>",
                    o.uuid, o.name, o.uuid, o.size, o.created_at
                ));
            }
            let html = format!(
                "<h2>Download Object</h2><p>Click a name to download:</p>
                 <table><tr><th>Name</th><th>UUID</th><th>Size</th><th>Created</th></tr>{}</table>
                 <p><a href='/manage'>← Back</a></p>", rows);
            respond(stream, "200 OK", "text/html", &page("Get Object", &html));
            return;
        }
    };

    let mut st = storage.lock().unwrap();
    match st.get(&key) {
        Ok((info, data)) => {
            // Serve as download
            let hdr = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Disposition: attachment; filename=\"{}\"\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                info.content_type, info.name, data.len()
            );
            let _ = stream.write_all(hdr.as_bytes());
            let _ = stream.write_all(&data);
        }
        Err(e) => {
            respond(stream, "404", "text/html",
                &page("Not Found", &format!("<p style='color:red'>Object not found: {}</p><a href='/api/get'>Back</a>", e)));
        }
    }
}

fn handle_web_delete(
    stream: &mut TcpStream,
    req: &str,
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
) {
    let key = match get_query_param(req, "key") {
        Some(k) => k,
        None => {
            let mut st = storage.lock().unwrap();
            let objects = st.list().unwrap_or_default();
            let mut rows = String::new();
            for o in &objects {
                rows.push_str(&format!(
                    "<tr><td>{}</td><td>{}</td><td>{} B</td><td><a href='/api/delete?key={}' onclick=\"return confirm('Delete {}?')\" style='color:red'>Delete</a></td></tr>",
                    o.name, o.uuid, o.size, o.uuid, o.name
                ));
            }
            respond(stream, "200 OK", "text/html",
                &page("Delete Object", &format!(
                    "<h2>Delete Object</h2><table><tr><th>Name</th><th>UUID</th><th>Size</th><th></th></tr>{}</table><p><a href='/manage'>← Back</a></p>", rows)));
            return;
        }
    };

    // Get UUID for cache eviction
    let uuid = {
        let mut st = storage.lock().unwrap();
        match st.find_info(&key) {
            Ok(info) => info.uuid,
            Err(e) => {
                respond(stream, "404", "text/html",
                    &page("Not Found", &format!("<p style='color:red'>{}</p><a href='/api/delete'>Back</a>", e)));
                return;
            }
        }
    };

    let mut st = storage.lock().unwrap();
    match st.delete(&key) {
        Ok(()) => {
            cache.remove(&uuid);
            info!("Web DELETE: key='{}'", key);
            respond(stream, "200 OK", "text/html",
                &page("Deleted", &format!("<p style='color:green'>✓ Object '{}' deleted.</p><p><a href='/manage'>← Back</a></p>", key)));
        }
        Err(e) => {
            respond(stream, "500", "text/html",
                &page("Error", &format!("<p style='color:red'>Failed: {}</p><a href='/api/delete'>Back</a>", e)));
        }
    }
}

fn handle_web_resize(
    stream: &mut TcpStream,
    body: &str,
    cache: &Arc<ObjectCache>,
) {
    let cap_str = get_form_field(body, "capacity").unwrap_or_default();
    match cap_str.parse::<usize>() {
        Ok(cap) if cap > 0 => {
            let old = cache.capacity();
            cache.resize(cap);
            info!("Web resize: {} -> {}", old, cache.capacity());
            respond(stream, "200 OK", "text/html",
                &page("Cache Resized", &format!(
                    "<p style='color:green'>✓ Cache resized from {} to {}.</p>
                     <p>Current stats: {}/{} entries, {:.2}% hit rate</p>
                     <p><a href='/manage'>← Back</a></p>",
                    old, cache.capacity(), cache.len(), cache.capacity(), cache.stats().hit_rate())));
        }
        _ => {
            respond(stream, "400", "text/html",
                &page("Error", "<p style='color:red'>Invalid capacity value.</p><a href='/manage'>Back</a>"));
        }
    }
}

fn handle_web_benchmark(
    stream: &mut TcpStream,
    storage: &SharedStorage,
    cache: &Arc<ObjectCache>,
) {
    let object_uuids: Vec<String> = {
        let mut st = storage.lock().unwrap();
        st.list().unwrap_or_default().into_iter().map(|o| o.uuid).collect()
    };
    if object_uuids.is_empty() {
        respond(stream, "200 OK", "text/html",
            &page("Benchmark", "<p>No objects stored. Upload some first.</p><a href='/manage'>Back</a>"));
        return;
    }

    let n = object_uuids.len();
    let iterations = 200;
    let workload: Vec<String> = (0..iterations).map(|i| object_uuids[i % n].clone()).collect();
    let cap = cache.capacity().min(n).max(2);

    let mut rows = String::new();
    for alg in CacheAlgorithmType::all() {
        let bench_cache = ObjectCache::new(*alg, cap);
        let r = bench_cache.benchmark_run(&workload, &[]);
        rows.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{:.2}%</td></tr>",
            alg.as_str(), r.hits, r.misses, r.hit_rate
        ));
    }

    respond(stream, "200 OK", "text/html",
        &page("Benchmark Result", &format!(
            "<h2>Cache Benchmark ({} iters, {} objects)</h2>
             <table><tr><th>Algorithm</th><th>Hits</th><th>Misses</th><th>Hit Rate</th></tr>{}</table>
             <p>Capacity used: {}</p>
             <p><a href='/manage'>← Back</a></p>",
            iterations, n, rows, cap)));
}

// ============================================================================
// Web pages
// ============================================================================

fn page(title: &str, body: &str) -> String {
    format!(r#"<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MiniOS - {}</title>
<style>
* {{ box-sizing:border-box; }}
body {{ font-family: -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
       max-width:900px; margin:2em auto; padding:0 1em; background:#f0f2f5; color:#333; }}
h1 {{ color:#1a5276; }} h2 {{ color:#2c3e50; }}
.card {{ background:white; border-radius:8px; padding:1em 1.5em; margin:1em 0; box-shadow:0 1px 3px rgba(0,0,0,0.1); }}
.nav {{ display:flex; gap:0.5em; flex-wrap:wrap; margin:1em 0; }}
.nav a {{ background:#1a5276; color:white; padding:0.5em 1em; border-radius:4px; text-decoration:none; }}
.nav a:hover {{ background:#2e86c1; }}
table {{ width:100%; border-collapse:collapse; margin:0.5em 0; }}
th,td {{ padding:6px 10px; text-align:left; border-bottom:1px solid #eee; }}
th {{ background:#f8f9fa; }}
input,textarea,select {{ width:100%; padding:8px; margin:4px 0 12px; border:1px solid #ddd; border-radius:4px; font-size:1em; }}
button {{ background:#27ae60; color:white; border:none; padding:10px 24px; border-radius:4px; cursor:pointer; font-size:1em; }}
button:hover {{ background:#2ecc71; }}
button.danger {{ background:#e74c3c; }} button.danger:hover {{ background:#c0392b; }}
.footer {{ text-align:center; color:#999; font-size:0.85em; margin-top:2em; }}
</style></head><body>
<h1>MiniOS</h1>
<div class="nav">
 <a href="/">Dashboard</a> <a href="/manage">Upload</a>
 <a href="/api/get">Download</a> <a href="/api/delete">Delete</a>
 <a href="/metrics">Metrics</a>
</div>
<div class="card">{}</div>
<div class="footer">MiniOS v{} · Web Management Interface</div>
</body></html>"#, title, body, env!("CARGO_PKG_VERSION"))
}

fn build_manage_page(storage: &SharedStorage, cache: &Arc<ObjectCache>) -> String {
    let mut st = storage.lock().unwrap();
    let objects = st.list().unwrap_or_default();
    let mut table = String::new();
    for o in &objects {
        table.push_str(&format!(
            "<tr><td>{}</td><td style='font-size:0.85em;color:#666'>{}</td><td>{} B</td><td>{}</td><td>{}</td></tr>",
            o.name, o.uuid, o.size, o.content_type, o.created_at
        ));
    }

    let status = st.status();
    drop(st);
    let cap = cache.capacity();

    format!(
        r#"<h2>📤 Upload Object</h2>
<form method="POST" action="/api/put">
  <label>Name:</label><input name="name" required placeholder="object-name">
  <label>Type:</label><input name="type" value="text/plain">
  <label>Tags (JSON):</label><input name="tags" value='{{}}'>
  <label>Content:</label><textarea name="content" rows="8" required placeholder="File content here..."></textarea>
  <button type="submit">Upload</button>
</form>

<h2>🗄️ Cache Control</h2>
<form method="POST" action="/api/resize">
  <label>Current capacity: {} | New capacity:</label>
  <input name="capacity" type="number" min="1" value="{}" required>
  <button type="submit">Resize</button>
</form>
<p><a href="/api/benchmark">▶ Run Benchmark</a></p>

<h2>📦 Objects ({})</h2>
<table><tr><th>Name</th><th>UUID</th><th>Size</th><th>Type</th><th>Created</th></tr>{}</table>
<p>Total: {} objects, {} / {} blocks used</p>"#,
        cap, cap.max(2), status.object_count, table,
        status.object_count, status.used_blocks, status.total_blocks)
}

// ============================================================================
// Dashboard (read-only status)
// ============================================================================

fn build_dashboard(storage: &SharedStorage, cache: &Arc<ObjectCache>, shm: &Arc<SharedMemory>, start_time: Instant) -> String {
    let st = storage.lock().unwrap();
    let status = st.status();
    drop(st);
    let cs = cache.stats();
    let uptime = start_time.elapsed().as_secs();
    let up_m = uptime / 60; let up_s = uptime % 60;

    page("Dashboard", &format!(
        r#"<h2>📊 Overview</h2>
<table><tr><td>Uptime</td><td>{}m {}s</td></tr><tr><td>Objects</td><td>{} / {}</td></tr></table>

<h2>💾 Storage</h2>
<table>
<tr><td>Used</td><td>{} / {} ({:.1}%)</td></tr>
<tr><td colspan="2"><div style="background:#e0e0e0;border-radius:4px;height:12px"><div style="background:#27ae60;height:100%;border-radius:4px;width:{:.1}%"></div></div></td></tr>
<tr><td>Blocks</td><td>{} used / {} total ({} each)</td></tr></table>

<h2>🗄️ Cache ({})</h2>
<table>
<tr><td>Entries / Cap</td><td>{} / {}</td></tr>
<tr><td>Hits / Misses</td><td>{} / {}</td></tr>
<tr><td>Evictions</td><td>{}</td></tr>
<tr><td>Hit rate</td><td><strong>{:.2}%</strong></td></tr></table>

<h2>🧠 Shared Memory</h2>
<table><tr><td>Pages free / total</td><td>{} / {}</td></tr></table>"#,
        up_m, up_s, status.object_count, status.max_objects,
        fmt_bytes(status.used_capacity), fmt_bytes(status.total_capacity),
        pct(status.used_capacity, status.total_capacity),
        pct(status.used_capacity, status.total_capacity),
        status.used_blocks, status.total_blocks, fmt_bytes(status.block_size as u64),
        cs.algorithm, cs.size, cs.capacity, cs.hits, cs.misses, cs.evictions, cs.hit_rate(),
        shm.free_page_count(), shm.num_pages(),
    ))
}

// ============================================================================
// Prometheus metrics
// ============================================================================

fn build_metrics(storage: &SharedStorage, cache: &Arc<ObjectCache>, shm: &Arc<SharedMemory>, start_time: Instant) -> String {
    let st = storage.lock().unwrap();
    let status = st.status();
    drop(st);
    let cs = cache.stats();
    let uptime = start_time.elapsed().as_secs();
    let mut m = String::new();
    m.push_str(&format!("# HELP minios_uptime_seconds Server uptime in seconds\n# TYPE minios_uptime_seconds gauge\nminios_uptime_seconds {}\n", uptime));
    m.push_str(&format!("# HELP minios_objects_total Total stored objects\n# TYPE minios_objects_total gauge\nminios_objects_total {}\n", status.object_count));
    m.push_str(&format!("# HELP minios_storage_blocks_total Total data blocks\n# TYPE minios_storage_blocks_total gauge\nminios_storage_blocks_total {}\n", status.total_blocks));
    m.push_str(&format!("# HELP minios_storage_blocks_used Used data blocks\n# TYPE minios_storage_blocks_used gauge\nminios_storage_blocks_used {}\n", status.used_blocks));
    m.push_str(&format!("# HELP minios_storage_blocks_free Free data blocks\n# TYPE minios_storage_blocks_free gauge\nminios_storage_blocks_free {}\n", status.free_blocks));
    m.push_str(&format!("# HELP minios_storage_bytes_total Total capacity in bytes\n# TYPE minios_storage_bytes_total gauge\nminios_storage_bytes_total {}\n", status.total_capacity));
    m.push_str(&format!("# HELP minios_storage_bytes_used Used capacity in bytes\n# TYPE minios_storage_bytes_used gauge\nminios_storage_bytes_used {}\n", status.used_capacity));
    m.push_str(&format!("# HELP minios_cache_hits_total Total cache hits\n# TYPE minios_cache_hits_total counter\nminios_cache_hits_total {}\n", cs.hits));
    m.push_str(&format!("# HELP minios_cache_misses_total Total cache misses\n# TYPE minios_cache_misses_total counter\nminios_cache_misses_total {}\n", cs.misses));
    m.push_str(&format!("# HELP minios_cache_evictions_total Total cache evictions\n# TYPE minios_cache_evictions_total counter\nminios_cache_evictions_total {}\n", cs.evictions));
    m.push_str(&format!("# HELP minios_cache_size Current cached entries\n# TYPE minios_cache_size gauge\nminios_cache_size {}\n", cs.size));
    m.push_str(&format!("# HELP minios_cache_capacity Max cache capacity\n# TYPE minios_cache_capacity gauge\nminios_cache_capacity {}\n", cs.capacity));
    m.push_str(&format!("# HELP minios_cache_hit_rate_percent Hit rate percentage\n# TYPE minios_cache_hit_rate_percent gauge\nminios_cache_hit_rate_percent {:.2}\n", cs.hit_rate()));
    m.push_str(&format!("# HELP minios_cache_algorithm_info Cache algorithm in use\n# TYPE minios_cache_algorithm_info gauge\nminios_cache_algorithm_info{{algorithm=\"{}\"}} 1\n", cs.algorithm));
    m.push_str(&format!("# HELP minios_shm_pages_total Total shared memory pages\n# TYPE minios_shm_pages_total gauge\nminios_shm_pages_total {}\n", shm.num_pages()));
    m.push_str(&format!("# HELP minios_shm_pages_free Free shared memory pages\n# TYPE minios_shm_pages_free gauge\nminios_shm_pages_free {}\n", shm.free_page_count()));
    m
}

fn fmt_bytes(bytes: u64) -> String {
    const U: &[&str] = &["B","KB","MB","GB"];
    let (mut v, mut i) = (bytes as f64, 0);
    while v >= 1024.0 && i < U.len()-1 { v /= 1024.0; i += 1; }
    format!("{:.2} {}", v, U[i])
}

fn pct(part: u64, total: u64) -> f64 {
    if total == 0 { 0.0 } else { (part as f64 / total as f64) * 100.0 }
}

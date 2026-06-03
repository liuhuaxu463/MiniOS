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

pub struct MetricsServer {
    port: u16,
    running: Arc<AtomicBool>,
}

impl MetricsServer {
    pub fn new(port: u16) -> Self { Self { port, running: Arc::new(AtomicBool::new(false)) } }

    pub fn start(&mut self, storage: SharedStorage, cache: Arc<ObjectCache>,
                 shm: Arc<SharedMemory>, start_time: Instant) {
        if self.port == 0 { info!("Web 管理界面已禁用 (port=0)"); return; }
        self.running.store(true, Ordering::SeqCst);
        let running = self.running.clone();
        let port = self.port;
        thread::spawn(move || {
            let addr = format!("0.0.0.0:{}", port);
            let listener = match TcpListener::bind(&addr) {
                Ok(l) => l,
                Err(e) => { error!("Web 服务绑定失败 {}: {}", addr, e); return; }
            };
            info!("Web 管理界面: http://0.0.0.0:{}", port);
            for stream in listener.incoming() {
                if !running.load(Ordering::SeqCst) { break; }
                match stream {
                    Ok(s) => { let st=storage.clone(); let ca=cache.clone(); let sh=shm.clone();
                        thread::spawn(move || dispatch(s, &st, &ca, &sh, start_time)); }
                    Err(e) => error!("Web accept 错误: {}", e),
                }
            }
        });
    }
    pub fn stop(&mut self) { self.running.store(false, Ordering::SeqCst); }
}

// ============================================================================
// HTTP parsing
// ============================================================================

fn read_request(stream: &mut TcpStream) -> Option<(String, String, String)> {
    stream.set_read_timeout(Some(std::time::Duration::from_secs(30))).ok();
    stream.set_nonblocking(false).ok();
    let mut buf = vec![0u8; 65536];
    let mut total = 0;

    // Phase 1: read headers until \r\n\r\n
    loop {
        if total >= buf.len() { buf.resize(buf.len() * 2, 0); }
        match stream_read(stream, &mut buf[total..total+1]) {
            Some(n) if n > 0 => { total += n; }
            _ => break,
        }
        if total >= 4 {
            let peek = String::from_utf8_lossy(&buf[..total]);
            if let Some(hdr_end) = peek.find("\r\n\r\n") {
                let hdr_str = peek[..hdr_end].to_string();
                let body_offset = hdr_end + 4;

                // Phase 2: read full body based on Content-Length
                let body_len: usize = hdr_str.lines()
                    .find(|l| l.to_lowercase().starts_with("content-length:"))
                    .and_then(|l| l.split(':').nth(1))
                    .and_then(|s| s.trim().parse().ok())
                    .unwrap_or(0);

                let needed = body_offset + body_len;
                while total < needed {
                    if total >= buf.len() { buf.resize(buf.len() * 2, 0); }
                    match stream_read(stream, &mut buf[total..total+1]) {
                        Some(n) if n > 0 => { total += n; }
                        _ => break,
                    }
                }

                let body = String::from_utf8_lossy(&buf[body_offset..total]).to_string();
                return Some((hdr_str, body, String::from_utf8_lossy(&buf[..total]).to_string()));
            }
        }
    }
    if total == 0 { return None; }
    let raw = String::from_utf8_lossy(&buf[..total]);
    let hdr_end = raw.find("\r\n\r\n").map(|i| i + 4).unwrap_or(raw.len());
    let headers = raw[..hdr_end.saturating_sub(4)].to_string();
    let body = raw[hdr_end..].to_string();
    Some((headers, body, raw.to_string()))
}

fn stream_read(stream: &mut TcpStream, buf: &mut [u8]) -> Option<usize> {
    match stream.read(buf) {
        Ok(n) if n > 0 => Some(n),
        Ok(_) => None,
        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            std::thread::sleep(std::time::Duration::from_millis(50));
            match stream.read(buf) { Ok(n) if n > 0 => Some(n), _ => None }
        }
        Err(_) => None,
    }
}

fn dispatch(mut stream: TcpStream, storage: &SharedStorage, cache: &Arc<ObjectCache>,
            shm: &Arc<SharedMemory>, start_time: Instant) {
    let (headers, body, raw_req) = match read_request(&mut stream) {
        Some(v) => v,
        None => return,
    };
    let first_line = headers.lines().next().unwrap_or("");
    let (method, raw_path) = parse_first(first_line);
    let path = raw_path.split('?').next().unwrap_or(raw_path);

    // Determine content type
    let ct_lower = headers.lines()
        .find(|l| l.to_lowercase().starts_with("content-type:"))
        .map(|l| l.split(':').nth(1).unwrap_or("").trim().to_lowercase())
        .unwrap_or_default();

    match (method, path) {
        ("GET", "/metrics") =>
            respond_ok(&mut stream, "text/plain; version=0.0.4; charset=utf-8",
                       &build_metrics(storage, cache, shm, start_time)),
        ("GET", "/") =>
            respond_ok(&mut stream, "text/html; charset=utf-8",
                       &build_dashboard(storage, cache, shm, start_time)),
        ("GET", "/manage") =>
            respond_ok(&mut stream, "text/html; charset=utf-8",
                       &build_manage_page(storage, cache)),
        ("POST", "/api/put") =>
            handle_web_put(&mut stream, &headers, &body, &ct_lower, storage, cache),
        ("GET", "/api/get") =>
            handle_web_get(&mut stream, &raw_req, storage),
        ("GET", "/api/delete") =>
            handle_web_delete(&mut stream, &raw_req, storage, cache),
        ("POST", "/api/resize") =>
            handle_web_resize(&mut stream, &body, cache),
        ("GET", "/api/benchmark") =>
            handle_web_benchmark(&mut stream, storage, cache),
        _ => respond(&mut stream, "404 Not Found", "text/plain", "404\n"),
    }
}

fn parse_first(line: &str) -> (&str, &str) {
    let p: Vec<&str> = line.split_whitespace().collect();
    if p.len() >= 2 { (p[0], p[1]) } else { ("GET", "/") }
}

fn respond(stream: &mut TcpStream, status: &str, ct: &str, body: &str) {
    let b = body.as_bytes();
    let r = format!("HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    status, ct, b.len());
    let _ = stream.write_all(r.as_bytes());
    let _ = stream.write_all(b);
}
fn respond_ok(stream: &mut TcpStream, ct: &str, body: &str) {
    respond(stream, "200 OK", ct, body);
}

fn get_query_param(req: &str, key: &str) -> Option<String> {
    let path = req.lines().next().unwrap_or("").split_whitespace().nth(1).unwrap_or("");
    let qs = path.find('?').map(|i| &path[i+1..]).unwrap_or("");
    for pair in qs.split('&') {
        let mut kv = pair.splitn(2, '=');
        if kv.next() == Some(key) {
            if let Some(v) = kv.next() { return Some(url_decode(v)); }
        }
    }
    None
}

fn url_decode(s: &str) -> String {
    let mut r = String::new(); let b = s.as_bytes(); let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i+2 < b.len() => {
                if let Ok(h) = u8::from_str_radix(&s[i+1..i+3], 16) { r.push(h as char); i+=3; continue; }
            }
            b'+' => { r.push(' '); i+=1; continue; }
            x => { r.push(x as char); }
        }
        i += 1;
    }
    r
}

// ============================================================================
// Multipart form data parser
// ============================================================================

struct MultipartField { name: String, data: Vec<u8> }

/// Parse multipart/form-data body. Uses string splitting on the boundary
/// (ASCII-only, safe for UTF-8 body content). Returns fields keyed by name.
fn parse_multipart(headers: &str, body: &str) -> Vec<MultipartField> {
    let ct = headers.lines()
        .find(|l| l.to_lowercase().starts_with("content-type:"))
        .unwrap_or("");
    let boundary = ct.split("boundary=").nth(1).map(|b| b.trim().trim_matches('"')).unwrap_or("");
    if boundary.is_empty() { return vec![]; }

    // Multipart body format:
    //   --boundary\r\n  (first, no leading \r\n)
    //   \r\n--boundary\r\n  (subsequent)
    //   \r\n--boundary--  (ends with --)
    let boundary_marker = format!("--{}", boundary);
    let bm = boundary_marker.as_bytes();
    let body_bytes = body.as_bytes();
    let mut fields = Vec::new();

    // Find all boundary positions
    let mut positions: Vec<usize> = Vec::new();
    let mut search_from = 0;
    while search_from < body_bytes.len() {
        if let Some(pos) = find_bytes(&body_bytes[search_from..], bm) {
            let abs_pos = search_from + pos;
            // Must be at start of line (preceded by \r\n, or at position 0)
            let is_start_of_line = abs_pos == 0
                || (abs_pos >= 2 && &body_bytes[abs_pos-2..abs_pos] == b"\r\n");
            // Check it's not the end boundary (next two chars are --)
            let is_end = abs_pos + bm.len() + 2 <= body_bytes.len()
                && &body_bytes[abs_pos + bm.len()..abs_pos + bm.len() + 2] == b"--";

            if is_start_of_line && !is_end {
                positions.push(abs_pos);
            }
            search_from = abs_pos + bm.len();
        } else {
            break;
        }
    }

    for i in 0..positions.len() {
        let section_start = positions[i] + bm.len();
        // Skip \r\n right after boundary
        let section_start = if section_start + 2 <= body_bytes.len()
            && &body_bytes[section_start..section_start+2] == b"\r\n" {
            section_start + 2 } else { section_start };
        let section_end = if i + 1 < positions.len() {
            positions[i + 1] - 2  // -2 to skip the \r\n before next boundary
        } else {
            body_bytes.len()
        };

        let section = &body_bytes[section_start..section_end];
        if let Some(field) = parse_field_section(section) {
            fields.push(field);
        }
    }
    fields
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn parse_field_section(data: &[u8]) -> Option<MultipartField> {
    // Find header/body split: \r\n\r\n
    let split = data.windows(4).position(|w| w == b"\r\n\r\n");
    let body_start = split.map(|s| s + 4).unwrap_or(0);
    let headers = std::str::from_utf8(&data[..body_start.saturating_sub(4)]).unwrap_or("");
    let mut content = &data[body_start..];
    // Only trim trailing CRLF, not leading (content may start with spaces etc.)
    while content.ends_with(b"\r\n") { content = &content[..content.len()-2]; }
    while content.ends_with(b"\n") { content = &content[..content.len()-1]; }

    let cd = headers.lines().find(|l| l.to_lowercase().starts_with("content-disposition:")).unwrap_or("");
    let name = cd.split("name=\"").nth(1).and_then(|s| s.split('"').next());

    name.map(|n| MultipartField { name: n.to_string(), data: content.to_vec() })
}

fn get_form_field_urlencoded(body: &str, field: &str) -> Option<String> {
    for pair in body.split('&') {
        let mut kv = pair.splitn(2, '=');
        if kv.next() == Some(field) {
            if let Some(v) = kv.next() { if !v.is_empty() { return Some(url_decode(v)); } }
        }
    }
    None
}

// ============================================================================
// Handlers
// ============================================================================

fn handle_web_put(stream: &mut TcpStream, headers: &str, body: &str, ct: &str,
                  storage: &SharedStorage, cache: &Arc<ObjectCache>) {
    let (name, content, ctype, tags) = if ct.contains("multipart/form-data") {
        let fields = parse_multipart(headers, body);
        let name = fields.iter().find(|f| f.name=="name").map(|f| String::from_utf8_lossy(&f.data).to_string());
        // Use file data only if non-empty; otherwise fall back to textarea content
        let file_data = fields.iter().find(|f| f.name=="file")
            .filter(|f| !f.data.is_empty()).map(|f| f.data.clone());
        let text_data = fields.iter().find(|f| f.name=="content")
            .filter(|f| !f.data.is_empty()).map(|f| f.data.clone());
        let content_data = file_data.or(text_data).unwrap_or_default();
        let ct_val = fields.iter().find(|f| f.name=="type").map(|f| String::from_utf8_lossy(&f.data).to_string());
        let tags_val = fields.iter().find(|f| f.name=="tags").map(|f| String::from_utf8_lossy(&f.data).to_string());
        (name.unwrap_or_default(), content_data, ct_val.unwrap_or_else(|| "application/octet-stream".to_string()), tags_val.unwrap_or_else(|| "{}".to_string()))
    } else {
        // URL-encoded form
        let n = get_form_field_urlencoded(body, "name").unwrap_or_default();
        let c = get_form_field_urlencoded(body, "content").unwrap_or_default();
        let t = get_form_field_urlencoded(body, "type").unwrap_or_else(|| "text/plain".to_string());
        let g = get_form_field_urlencoded(body, "tags").unwrap_or_else(|| "{}".to_string());
        (n, c.into_bytes(), t, g)
    };

    if name.is_empty() || content.is_empty() {
        respond_ok(stream, "text/html; charset=utf-8",
            &page("上传失败", "<p class='error'>对象名称和内容不能为空</p><a class='btn-back' href='/manage'>返回</a>"));
        return;
    }

    let mut st = storage.lock().unwrap();
    match st.put(&name, &content, &ctype, &tags) {
        Ok(info) => {
            cache.put(&info.uuid, CachedObject {
                uuid: info.uuid.clone(), data: content.clone(), name: info.name.clone(),
                content_type: info.content_type.clone(), size: info.size, tags: info.tags.clone(),
            });
            info!("Web 上传: 名称='{}' uuid={} 大小={}", name, info.uuid, info.size);
            respond_ok(stream, "text/html; charset=utf-8", &page("上传成功",
                &format!("<div class='success'>对象上传成功</div>
                <table class='info-table'><tr><th>名称</th><td>{}</td></tr>
                <tr><th>UUID</th><td style='font-family:monospace;font-size:0.9em'>{}</td></tr>
                <tr><th>大小</th><td>{} 字节</td></tr>
                <tr><th>类型</th><td>{}</td></tr></table>
                <a class='btn-back' href='/manage'>返回管理页面</a>",
                info.name, info.uuid, info.size, info.content_type)));
        }
        Err(e) => {
            error!("Web 上传失败: {}", e);
            respond_ok(stream, "text/html; charset=utf-8", &page("上传失败",
                &format!("<p class='error'>上传失败：{}</p><a class='btn-back' href='/manage'>返回</a>", e)));
        }
    }
}

fn handle_web_get(stream: &mut TcpStream, req: &str, storage: &SharedStorage) {
    if let Some(key) = get_query_param(req, "key") {
        let mut st = storage.lock().unwrap();
        match st.get(&key) {
            Ok((info, data)) => {
                let hdr = format!("HTTP/1.1 200 OK\r\nContent-Type: {}\r\n\
                    Content-Disposition: attachment; filename=\"{}\"\r\nContent-Length: {}\r\n\
                    Connection: close\r\n\r\n", info.content_type, info.name, data.len());
                let _ = stream.write_all(hdr.as_bytes());
                let _ = stream.write_all(&data);
                return;
            }
            Err(e) => {
                respond_ok(stream, "text/html; charset=utf-8", &page("未找到",
                    &format!("<p class='error'>对象未找到：{}</p><a class='btn-back' href='/api/get'>返回</a>", e)));
                return;
            }
        }
    }

    // No key — list objects
    let mut st = storage.lock().unwrap();
    let objects = st.list().unwrap_or_default();
    let mut rows = String::new();
    for o in &objects {
        rows.push_str(&format!(
            "<tr><td><a class='obj-link' href='/api/get?key={}'>{}</a></td>\
             <td style='font-family:monospace;font-size:0.82em;color:#8590a6'>{}</td>\
             <td>{} 字节</td><td>{}</td></tr>",
            o.uuid, o.name, o.uuid, o.size, o.created_at));
    }
    let h = if objects.is_empty() {
        "<div class='empty'>暂无对象，请先上传</div>".to_string()
    } else {
        format!("<p>点击文件名即可下载：</p>\
            <table><tr><th>名称</th><th>UUID</th><th>大小</th><th>创建时间</th></tr>{}</table>", rows)
    };
    respond_ok(stream, "text/html; charset=utf-8",
        &page_tab("下载对象", &format!("{}<a class='btn-back' href='/manage'>返回</a>", h), "download"));
}

fn handle_web_delete(stream: &mut TcpStream, req: &str, storage: &SharedStorage, cache: &Arc<ObjectCache>) {
    if let Some(key) = get_query_param(req, "key") {
        let uuid = {
            let mut st = storage.lock().unwrap();
            match st.find_info(&key) {
                Ok(info) => info.uuid,
                Err(e) => {
                    respond_ok(stream, "text/html; charset=utf-8", &page("未找到",
                        &format!("<p class='error'>{}</p><a class='btn-back' href='/api/delete'>返回</a>", e)));
                    return;
                }
            }
        };
        let mut st = storage.lock().unwrap();
        match st.delete(&key) {
            Ok(()) => {
                cache.remove(&uuid);
                respond_ok(stream, "text/html; charset=utf-8", &page("删除成功",
                    &format!("<div class='success'>对象「{}」已成功删除</div><a class='btn-back' href='/manage'>返回管理页面</a>", key)));
            }
            Err(e) => {
                respond_ok(stream, "text/html; charset=utf-8", &page("删除失败",
                    &format!("<p class='error'>删除失败：{}</p><a class='btn-back' href='/api/delete'>返回</a>", e)));
            }
        }
        return;
    }

    // No key — list objects with delete links
    let mut st = storage.lock().unwrap();
    let objects = st.list().unwrap_or_default();
    let mut rows = String::new();
    if objects.is_empty() {
        rows = "<tr><td colspan='4' class='empty'>暂无对象可删除</td></tr>".to_string();
    } else {
        for o in &objects {
            rows.push_str(&format!(
                "<tr><td><strong>{}</strong></td><td style='font-family:monospace;font-size:0.82em;color:#8590a6'>{}</td>\
                 <td>{} 字节</td>\
                 <td><a class='btn-delete' href='/api/delete?key={}' \
                 onclick=\"return confirm('确定要删除「{}」吗？')\">删除</a></td></tr>",
                o.name, o.uuid, o.size, o.uuid, o.name));
        }
    }
    respond_ok(stream, "text/html; charset=utf-8", &page_tab("删除对象",
        &format!("<table><tr><th>名称</th><th>UUID</th><th>大小</th><th>操作</th></tr>{}</table>\
            <a class='btn-back' href='/manage'>返回</a>", rows), "delete"));
}

fn handle_web_resize(stream: &mut TcpStream, body: &str, cache: &Arc<ObjectCache>) {
    let cap_str = get_form_field_urlencoded(body, "capacity").unwrap_or_default();
    match cap_str.parse::<usize>() {
        Ok(cap) if cap > 0 => {
            let old = cache.capacity();
            cache.resize(cap);
            let cs = cache.stats();
            respond_ok(stream, "text/html; charset=utf-8", &page("缓存调整成功",
                &format!("<div class='success'>缓存容量已从 {} 调整到 {}</div>
                <table class='info-table'><tr><th>当前条目</th><td>{} / {}</td></tr>
                <tr><th>命中率</th><td>{:.2}%</td></tr></table>
                <a class='btn-back' href='/manage'>返回</a>", old, cap, cs.size, cs.capacity, cs.hit_rate())));
        }
        _ => { respond_ok(stream, "text/html; charset=utf-8", &page("参数错误",
            "<p class='error'>无效的容量值</p><a class='btn-back' href='/manage'>返回</a>")); }
    }
}

fn handle_web_benchmark(stream: &mut TcpStream, storage: &SharedStorage, cache: &Arc<ObjectCache>) {
    let object_uuids: Vec<String> = {
        storage.lock().unwrap().list().unwrap_or_default().into_iter().map(|o| o.uuid).collect()
    };
    if object_uuids.is_empty() {
        respond_ok(stream, "text/html; charset=utf-8", &page("性能测试",
            "<div class='empty'>暂无对象，请先上传一些文件后再进行测试</div><a class='btn-back' href='/manage'>返回</a>"));
        return;
    }
    let n = object_uuids.len();
    let iterations = 200;
    let workload: Vec<String> = (0..iterations).map(|i| object_uuids[i % n].clone()).collect();

    // Use configured cache capacity, capped at number of objects
    let real_cap = cache.capacity();
    let cap = real_cap.min(n).max(1);

    // Preload benchmark caches with actual storage data so cache hits can occur
    let mut preloaded: Vec<(String, CachedObject)> = Vec::new();
    {
        let mut st = storage.lock().unwrap();
        for uuid in object_uuids.iter().take(cap.min(50)) {
            if let Ok((_info, data)) = st.get(uuid) {
                preloaded.push((uuid.clone(), CachedObject {
                    uuid: uuid.clone(), data, name: "bench".to_string(),
                    content_type: "octet-stream".to_string(), size: 0, tags: "{}".to_string(),
                }));
            }
        }
    }

    let mut rows = String::new();
    let mut best = ("", 0.0);
    for alg in CacheAlgorithmType::all() {
        let bc = ObjectCache::new(*alg, cap);
        let r = bc.benchmark_run(&workload, &preloaded);
        if r.hit_rate > best.1 { best = (alg.as_str(), r.hit_rate); }
        rows.push_str(&format!("<tr><td><strong>{}</strong></td><td>{}</td><td>{}</td><td>{:.2}%</td></tr>",
                               alg.as_str(), r.hits, r.misses, r.hit_rate));
    }

    respond_ok(stream, "text/html; charset=utf-8", &page("性能测试结果",
        &format!("<h2>缓存算法对比</h2>
        <table><tr><th>算法</th><th>命中</th><th>未命中</th><th>命中率</th></tr>{}</table>
        <p style='margin-top:1em'><strong>最优算法：{}</strong>（{:.2}% 命中率）</p>
        <p style='color:#8590a6'>测试条件：{} 个对象，{} 次迭代，缓存容量 {}（预加载 {} 条）</p>
        <a class='btn-back' href='/manage'>返回</a>",
        rows, best.0, best.1, n, iterations, cap, preloaded.len())));
}

// ============================================================================
// Page builders
// ============================================================================

fn page(title: &str, body: &str) -> String { page_with_refresh(title, body, false, "") }
fn page_tab(title: &str, body: &str, tab: &str) -> String { page_with_refresh(title, body, false, tab) }

fn page_with_refresh(title: &str, body: &str, auto_refresh: bool, active_tab: &str) -> String {
    let refresh = if auto_refresh { "<meta http-equiv=\"refresh\" content=\"5\">" } else { "" };
    let (a0,a1,a2,a3) = match active_tab {
        "overview" => (" class='active'","","",""),
        "manage"   => (""," class='active'","",""),
        "download" => ("",""," class='active'",""),
        "delete"   => ("","",""," class='active'"),
        _ => ("","","",""),
    };
    format!(r##"<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
{}<title>MiniOS - {}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'PingFang SC','Hiragino Sans GB','Microsoft YaHei','Segoe UI',sans-serif;background:#f6f6f6;color:#1a1a1a;line-height:1.7;min-height:100vh}}
.header{{background:#fff;box-shadow:0 1px 3px rgba(18,18,18,.08);position:sticky;top:0;z-index:100}}
.header-inner{{max-width:1000px;margin:0 auto;padding:0 20px;display:flex;align-items:center;height:56px}}
.logo{{font-size:1.3em;font-weight:700;color:#06f;margin-right:32px;text-decoration:none;letter-spacing:-.5px}}
.logo span{{color:#333;font-weight:400}}
.nav{{display:flex;gap:4px}}
.nav a{{padding:8px 16px;border-radius:6px;color:#555;text-decoration:none;font-size:.95em;transition:all .15s;font-weight:500}}
.nav a:hover,.nav a.active{{background:#06f1a;color:#06f}}
.main{{max-width:1000px;margin:24px auto;padding:0 20px}}
.card{{background:#fff;border-radius:8px;padding:24px;margin-bottom:16px;box-shadow:0 1px 3px rgba(18,18,18,.06)}}
.card h2{{font-size:1.15em;font-weight:600;color:#1a1a1a;margin-bottom:16px;padding-bottom:12px;border-bottom:1px solid #f0f0f0}}
table{{width:100%;border-collapse:collapse}}
th,td{{padding:10px 14px;text-align:left;border-bottom:1px solid #f0f0f0}}
th{{background:#fafafa;color:#8590a6;font-weight:500;font-size:.9em}}
tr:hover{{background:#fafafa}}
label{{display:block;font-weight:500;color:#444;margin:16px 0 6px;font-size:.95em}}
input,textarea,select{{width:100%;padding:10px 14px;border:1px solid #e0e0e0;border-radius:6px;font-size:.95em;font-family:inherit;transition:border-color .15s;background:#fafafa}}
input:focus,textarea:focus{{outline:none;border-color:#06f;background:#fff}}
textarea{{resize:vertical;min-height:120px}}
input[type=file]{{padding:8px}}
.btn{{display:inline-block;padding:10px 24px;border:none;border-radius:6px;font-size:.95em;font-weight:500;cursor:pointer;text-decoration:none;transition:all .15s;margin:8px 8px 0 0}}
.btn-primary{{background:#06f;color:#fff}}
.btn-primary:hover{{background:#05c}}
.btn-danger{{background:#fff;color:#e74c3c;border:1px solid #e74c3c}}
.btn-danger:hover{{background:#e74c3c;color:#fff}}
.btn-back{{display:inline-block;margin-top:16px;color:#8590a6;text-decoration:none;font-size:.95em}}
.btn-back:hover{{color:#06f}}
.obj-link{{color:#06f;text-decoration:none;font-weight:500}}
.obj-link:hover{{text-decoration:underline}}
.btn-delete{{color:#e74c3c;text-decoration:none;font-weight:500;font-size:.9em}}
.btn-delete:hover{{text-decoration:underline}}
.stats-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin:16px 0}}
.stat-item{{text-align:center;padding:16px;background:#fafafa;border-radius:8px}}
.stat-value{{font-size:1.8em;font-weight:700;color:#06f}}
.stat-label{{font-size:.85em;color:#8590a6;margin-top:4px}}
.success{{color:#00a854;font-weight:500;font-size:1.05em;margin:8px 0}}
.error{{color:#e74c3c;font-weight:500;margin:8px 0}}
.empty{{color:#8590a6;text-align:center;padding:32px 0}}
.info-table td,.info-table th{{border:none}}
.info-table td:first-child,.info-table th:first-child{{width:80px;color:#8590a6}}
.footer{{text-align:center;color:#8590a6;font-size:.8em;padding:24px 0;border-top:1px solid #f0f0f0;margin-top:32px}}
.bar{{height:8px;border-radius:4px;background:#f0f0f0;overflow:hidden;margin:8px 0}}
.bar-fill{{height:100%;border-radius:4px;transition:width .5s}}
.bar-green{{background:linear-gradient(90deg,#06f,#00a854)}}
.form-row{{display:flex;gap:12px;flex-wrap:wrap}}
.form-row>div{{flex:1;min-width:200px}}
</style></head><body>
<div class="header"><div class="header-inner">
<a class="logo" href="/">Mini<span>OS</span></a>
<div class="nav">
<a href="/"{}>总览</a><a href="/manage"{}>管理</a><a href="/api/get"{}>下载</a><a href="/api/delete"{}>删除</a><a href="/metrics">监控</a>
</div></div></div>
<div class="main"><div class="card"><h2>{}</h2>{}</div></div>
<div class="footer">MiniOS v{} · 轻量级对象存储服务 · Web 管理控制台</div>
</body></html>"##,
    refresh, title, a0,a1,a2,a3, title, body, env!("CARGO_PKG_VERSION"))
}

fn build_manage_page(storage: &SharedStorage, cache: &Arc<ObjectCache>) -> String {
    let mut st = storage.lock().unwrap();
    let objects = st.list().unwrap_or_default();
    let mut table = String::new();
    if objects.is_empty() {
        table = "<tr><td colspan='5' class='empty'>暂无对象，请使用上方表单上传</td></tr>".to_string();
    } else {
        for o in &objects {
            table.push_str(&format!(
                "<tr><td><strong>{}</strong></td>\
                 <td style='font-family:monospace;font-size:.82em;color:#8590a6'>{}</td>\
                 <td>{} 字节</td><td>{}</td><td>{}</td></tr>",
                o.name, o.uuid, o.size, o.content_type, o.created_at));
        }
    }
    let status = st.status();
    drop(st);
    let cap = cache.capacity();
    let cs = cache.stats();

    page_tab("对象管理", &format!(
        r##"<div class="stats-grid">
<div class="stat-item"><div class="stat-value">{}</div><div class="stat-label">已存对象</div></div>
<div class="stat-item"><div class="stat-value">{:.1}%</div><div class="stat-label">容量使用率</div></div>
<div class="stat-item"><div class="stat-value">{}</div><div class="stat-label">缓存容量</div></div>
<div class="stat-item"><div class="stat-value">{:.1}%</div><div class="stat-label">命中率</div></div>
</div>

<h2 style="margin-top:24px">上传对象</h2>
<form method="POST" action="/api/put" enctype="multipart/form-data" autocomplete="off">
  <div class="form-row">
    <div><label>对象名称</label><input name="name" required placeholder="my-document.txt" autocomplete="off"></div>
    <div><label>内容类型</label><input name="type" value="text/plain" list="mime-types" autocomplete="off">
    <datalist id="mime-types">
      <option value="text/plain">
      <option value="text/html">
      <option value="application/json">
      <option value="application/octet-stream">
      <option value="image/png">
      <option value="image/jpeg">
      <option value="application/pdf">
      <option value="text/css">
      <option value="application/javascript">
    </datalist></div>
  </div>
  <div class="form-row">
    <div><label>标签（JSON 格式）</label><input name="tags" value='{{{{}}}}' placeholder='{{"author":"me"}}' autocomplete="off"></div>
  </div>
  <label>选择文件（从本地上传）</label>
  <input type="file" name="file" autocomplete="off">
  <label>或直接粘贴文件内容</label>
  <textarea name="content" placeholder="在此粘贴文件内容（如不选择文件则使用此内容）" autocomplete="off"></textarea>
  <button class="btn btn-primary" type="submit">上传</button>
</form>

<h2 style="margin-top:24px">缓存控制</h2>
<form method="POST" action="/api/resize">
  <div class="form-row">
    <div><label>缓存容量（当前 {}）</label><input name="capacity" type="number" min="1" value="{}" required></div>
    <div style="display:flex;align-items:flex-end"><button class="btn btn-primary" type="submit">调整容量</button></div>
  </div>
</form>
<a class="btn btn-primary" href="/api/benchmark">运行性能测试</a>

<h2 style="margin-top:24px">对象列表（共 {} 个）</h2>
<table><tr><th>名称</th><th>UUID</th><th>大小</th><th>类型</th><th>创建时间</th></tr>{}</table>
<p style="color:#8590a6;margin-top:8px">存储用量：{} / {} 块（{} 每块）</p>"##,
        status.object_count, pct(status.used_capacity, status.total_capacity),
        cap, cs.hit_rate(),
        cap, cap.max(2),
        status.object_count, table,
        status.used_blocks, status.total_blocks, fmt_bytes(status.block_size as u64),
    ), "manage")
}

fn build_dashboard(storage: &SharedStorage, cache: &Arc<ObjectCache>, shm: &Arc<SharedMemory>, start_time: Instant) -> String {
    let st = storage.lock().unwrap();
    let status = st.status();
    drop(st);
    let cs = cache.stats();
    let uptime = start_time.elapsed().as_secs();
    page_with_refresh("系统总览", &format!(
        r##"<div class="stats-grid">
<div class="stat-item"><div class="stat-value">{}</div><div class="stat-label">运行时间</div></div>
<div class="stat-item"><div class="stat-value">{}</div><div class="stat-label">对象总数</div></div>
<div class="stat-item"><div class="stat-value">{:.1}%</div><div class="stat-label">命中率</div></div>
<div class="stat-item"><div class="stat-value">{}</div><div class="stat-label">缓存算法</div></div>
</div>
<h2>存储</h2>
<table>
<tr><td>已用容量</td><td>{} / {}（{:.1}%）</td></tr>
<tr><td colspan="2"><div class="bar"><div class="bar-fill bar-green" style="width:{:.1}%"></div></div></td></tr>
<tr><td>数据块</td><td>{} 已用 / {} 总计（{} 每块）</td></tr>
<tr><td>对象数</td><td>{} / {}（上限）</td></tr>
</table>
<h2>缓存 — {} 算法</h2>
<table>
<tr><td>当前条目 / 容量</td><td>{} / {}</td></tr>
<tr><td>命中 / 未命中</td><td>{} / {}</td></tr>
<tr><td>淘汰次数</td><td>{}</td></tr>
<tr><td>命中率</td><td><strong>{:.2}%</strong></td></tr>
</table>
<h2>共享内存</h2>
<table><tr><td>空闲页 / 总页数</td><td>{} / {}</td></tr><tr><td>页大小</td><td>4 KB</td></tr></table>"##,
        fmt_uptime(uptime), status.object_count, cs.hit_rate(), cs.algorithm,
        fmt_bytes(status.used_capacity), fmt_bytes(status.total_capacity),
        pct(status.used_capacity, status.total_capacity),
        pct(status.used_capacity, status.total_capacity),
        status.used_blocks, status.total_blocks, fmt_bytes(status.block_size as u64),
        status.object_count, status.max_objects,
        cs.algorithm, cs.size, cs.capacity, cs.hits, cs.misses,
        cs.evictions, cs.hit_rate(),
        shm.free_page_count(), shm.num_pages(),
    ), true, "overview")
}

// ============================================================================
// Prometheus metrics
// ============================================================================

fn build_metrics(storage: &SharedStorage, cache: &Arc<ObjectCache>, shm: &Arc<SharedMemory>, start_time: Instant) -> String {
    let st = storage.lock().unwrap(); let status = st.status(); drop(st);
    let cs = cache.stats(); let uptime = start_time.elapsed().as_secs();
    let mut m = String::new();
    m.push_str(&format!("# HELP minios_uptime_seconds Server uptime\n# TYPE minios_uptime_seconds gauge\nminios_uptime_seconds {}\n", uptime));
    m.push_str(&format!("# HELP minios_objects_total Stored objects\n# TYPE minios_objects_total gauge\nminios_objects_total {}\n", status.object_count));
    m.push_str(&format!("# HELP minios_storage_blocks_total Total data blocks\n# TYPE minios_storage_blocks_total gauge\nminios_storage_blocks_total {}\n", status.total_blocks));
    m.push_str(&format!("# HELP minios_storage_blocks_used Used data blocks\n# TYPE minios_storage_blocks_used gauge\nminios_storage_blocks_used {}\n", status.used_blocks));
    m.push_str(&format!("# HELP minios_storage_blocks_free Free data blocks\n# TYPE minios_storage_blocks_free gauge\nminios_storage_blocks_free {}\n", status.free_blocks));
    m.push_str(&format!("# HELP minios_storage_bytes_total Total capacity\n# TYPE minios_storage_bytes_total gauge\nminios_storage_bytes_total {}\n", status.total_capacity));
    m.push_str(&format!("# HELP minios_storage_bytes_used Used capacity\n# TYPE minios_storage_bytes_used gauge\nminios_storage_bytes_used {}\n", status.used_capacity));
    m.push_str(&format!("# HELP minios_cache_hits_total Cache hits\n# TYPE minios_cache_hits_total counter\nminios_cache_hits_total {}\n", cs.hits));
    m.push_str(&format!("# HELP minios_cache_misses_total Cache misses\n# TYPE minios_cache_misses_total counter\nminios_cache_misses_total {}\n", cs.misses));
    m.push_str(&format!("# HELP minios_cache_evictions_total Cache evictions\n# TYPE minios_cache_evictions_total counter\nminios_cache_evictions_total {}\n", cs.evictions));
    m.push_str(&format!("# HELP minios_cache_size Current cached entries\n# TYPE minios_cache_size gauge\nminios_cache_size {}\n", cs.size));
    m.push_str(&format!("# HELP minios_cache_capacity Max cache capacity\n# TYPE minios_cache_capacity gauge\nminios_cache_capacity {}\n", cs.capacity));
    m.push_str(&format!("# HELP minios_cache_hit_rate_percent Hit rate\n# TYPE minios_cache_hit_rate_percent gauge\nminios_cache_hit_rate_percent {:.2}\n", cs.hit_rate()));
    m.push_str(&format!("# HELP minios_cache_algorithm_info Cache algorithm\n# TYPE minios_cache_algorithm_info gauge\nminios_cache_algorithm_info{{algorithm=\"{}\"}} 1\n", cs.algorithm));
    m.push_str(&format!("# HELP minios_shm_pages_total SHM total pages\n# TYPE minios_shm_pages_total gauge\nminios_shm_pages_total {}\n", shm.num_pages()));
    m.push_str(&format!("# HELP minios_shm_pages_free SHM free pages\n# TYPE minios_shm_pages_free gauge\nminios_shm_pages_free {}\n", shm.free_page_count()));
    m
}

fn fmt_bytes(bytes: u64) -> String {
    let u = ["B","KB","MB","GB"]; let (mut v,mut i)=(bytes as f64,0);
    while v>=1024.0 && i<u.len()-1 { v/=1024.0; i+=1; }
    format!("{:.2} {}", v, u[i])
}
fn pct(part: u64, total: u64) -> f64 { if total==0 {0.0} else {part as f64/total as f64*100.0} }
fn fmt_uptime(s: u64) -> String {
    if s<60 {format!("{} 秒",s)} else if s<3600 {format!("{} 分 {} 秒",s/60,s%60)}
    else {format!("{} 时 {} 分",s/3600,(s%3600)/60)}
}

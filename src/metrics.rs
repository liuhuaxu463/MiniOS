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
    pub fn new(port: u16) -> Self {
        Self { port, running: Arc::new(AtomicBool::new(false)) }
    }

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
                    Ok(s) => {
                        let st = storage.clone(); let ca = cache.clone();
                        let sh = shm.clone();
                        thread::spawn(move || dispatch(s, &st, &ca, &sh, start_time));
                    }
                    Err(e) => error!("Web accept 错误: {}", e),
                }
            }
        });
    }

    pub fn stop(&mut self) { self.running.store(false, Ordering::SeqCst); }
}

fn dispatch(mut stream: TcpStream, storage: &SharedStorage, cache: &Arc<ObjectCache>,
            shm: &Arc<SharedMemory>, start_time: Instant) {
    stream.set_read_timeout(Some(std::time::Duration::from_secs(10))).ok();
    let mut buf = [0u8; 16384];
    let n = match stream.read(&mut buf) { Ok(n) if n > 0 => n, _ => return, };
    let req = String::from_utf8_lossy(&buf[..n]);
    let first_line = req.lines().next().unwrap_or("");
    let (method, path) = parse_first(first_line);
    let body_start = req.find("\r\n\r\n").map(|i| i + 4).unwrap_or(req.len());
    let body = &req[body_start..];

    match (method, path) {
        ("GET", "/metrics") =>
            respond(&mut stream, "200 OK", "text/plain; version=0.0.4; charset=utf-8",
                    &build_metrics(storage, cache, shm, start_time)),
        ("GET", "/") =>
            respond(&mut stream, "200 OK", "text/html; charset=utf-8",
                    &build_dashboard(storage, cache, shm, start_time)),
        ("GET", "/manage") =>
            respond(&mut stream, "200 OK", "text/html; charset=utf-8",
                    &build_manage_page(storage, cache)),
        ("POST", "/api/put") =>
            handle_web_put(&mut stream, body, storage, cache),
        ("GET", "/api/get") =>
            handle_web_get(&mut stream, &req, storage),
        ("GET", "/api/delete") =>
            handle_web_delete(&mut stream, &req, storage, cache),
        ("POST", "/api/resize") =>
            handle_web_resize(&mut stream, body, cache),
        ("GET", "/api/benchmark") =>
            handle_web_benchmark(&mut stream, storage, cache),
        _ => respond(&mut stream, "404 Not Found", "text/plain", "404 页面未找到\n"),
    }
}

fn parse_first(line: &str) -> (&str, &str) {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() >= 2 { (parts[0], parts[1]) } else { ("GET", "/") }
}

fn respond(stream: &mut TcpStream, status: &str, ct: &str, body: &str) {
    let r = format!("HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status, ct, body.len(), body);
    let _ = stream.write_all(r.as_bytes());
}

fn get_query_param(req: &str, key: &str) -> Option<String> {
    let path = req.lines().next().unwrap_or("").split_whitespace().next().unwrap_or("");
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

fn get_form_field(body: &str, field: &str) -> Option<String> {
    for pair in body.split('&') {
        let mut kv = pair.splitn(2, '=');
        if kv.next() == Some(field) {
            if let Some(v) = kv.next() { if !v.is_empty() { return Some(url_decode(v)); } }
        }
    }
    None
}

// ============================================================================
// Web API handlers
// ============================================================================

fn handle_web_put(stream: &mut TcpStream, body: &str, storage: &SharedStorage, cache: &Arc<ObjectCache>) {
    let name = match get_form_field(body, "name") {
        Some(n) => n,
        None => { respond(stream, "400", "text/html", &page("上传失败", "<p class='error'>缺少「对象名称」字段</p><a href='/manage'>← 返回</a>")); return; }
    };
    let content = match get_form_field(body, "content") {
        Some(c) => c,
        None => { respond(stream, "400", "text/html", &page("上传失败", "<p class='error'>缺少「内容」字段</p><a href='/manage'>← 返回</a>")); return; }
    };
    let ctype = get_form_field(body, "type").unwrap_or_else(|| "text/plain".to_string());
    let tags = get_form_field(body, "tags").unwrap_or_else(|| "{}".to_string());

    let data = content.as_bytes();
    let mut st = storage.lock().unwrap();
    match st.put(&name, data, &ctype, &tags) {
        Ok(info) => {
            cache.put(&info.uuid, CachedObject {
                uuid: info.uuid.clone(), data: data.to_vec(), name: info.name.clone(),
                content_type: info.content_type.clone(), size: info.size, tags: info.tags.clone(),
            });
            info!("Web 上传: 名称='{}' uuid={} 大小={}", name, info.uuid, info.size);
            respond(stream, "200 OK", "text/html", &page("上传成功",
                &format!(r#"<div class="success">✓ 对象上传成功！</div>
                <table class="info-table"><tr><th>名称</th><td>{}</td></tr>
                <tr><th>UUID</th><td style="font-family:monospace;font-size:0.9em">{}</td></tr>
                <tr><th>大小</th><td>{} 字节</td></tr>
                <tr><th>类型</th><td>{}</td></tr></table>
                <a class="btn-back" href='/manage'>← 返回管理页面</a>"#,
                info.name, info.uuid, info.size, info.content_type)));
        }
        Err(e) => {
            error!("Web 上传失败: {}", e);
            respond(stream, "500", "text/html", &page("上传失败",
                &format!("<p class='error'>上传失败：{}</p><a class='btn-back' href='/manage'>← 返回</a>", e)));
        }
    }
}

fn handle_web_get(stream: &mut TcpStream, req: &str, storage: &SharedStorage) {
    let key = match get_query_param(req, "key") {
        Some(k) => k,
        None => {
            let mut st = storage.lock().unwrap();
            let objects = st.list().unwrap_or_default();
            let mut rows = String::new();
            for o in &objects {
                rows.push_str(&format!(
                    "<tr><td><a class='obj-link' href='/api/get?key={}'>{}</a></td>\
                     <td style='font-family:monospace;font-size:0.85em;color:#8590a6'>{}</td>\
                     <td>{} 字节</td><td>{}</td></tr>",
                    o.uuid, o.name, o.uuid, o.size, o.created_at
                ));
            }
            let h = if objects.is_empty() {
                "<div class='empty'>暂无对象，请先上传</div>".to_string()
            } else {
                format!("<p>点击文件名即可下载：</p>\
                    <table><tr><th>名称</th><th>UUID</th><th>大小</th><th>创建时间</th></tr>{}</table>", rows)
            };
            respond(stream, "200 OK", "text/html", &page("下载对象",
                &format!("{}<a class='btn-back' href='/manage'>← 返回</a>", h)));
            return;
        }
    };
    let mut st = storage.lock().unwrap();
    match st.get(&key) {
        Ok((info, data)) => {
            let hdr = format!("HTTP/1.1 200 OK\r\nContent-Type: {}\r\n\
                Content-Disposition: attachment; filename=\"{}\"\r\nContent-Length: {}\r\n\
                Connection: close\r\n\r\n", info.content_type, info.name, data.len());
            let _ = stream.write_all(hdr.as_bytes());
            let _ = stream.write_all(&data);
        }
        Err(e) => {
            respond(stream, "404", "text/html", &page("未找到",
                &format!("<p class='error'>对象未找到：{}</p><a class='btn-back' href='/api/get'>← 返回</a>", e)));
        }
    }
}

fn handle_web_delete(stream: &mut TcpStream, req: &str, storage: &SharedStorage, cache: &Arc<ObjectCache>) {
    let key = match get_query_param(req, "key") {
        Some(k) => k,
        None => {
            let mut st = storage.lock().unwrap();
            let objects = st.list().unwrap_or_default();
            let mut rows = String::new();
            if objects.is_empty() {
                rows = "<tr><td colspan='4' class='empty'>暂无对象可删除</td></tr>".to_string();
            } else {
                for o in &objects {
                    rows.push_str(&format!(
                        "<tr><td><strong>{}</strong></td><td style='font-family:monospace;font-size:0.85em;color:#8590a6'>{}</td>\
                         <td>{} 字节</td>\
                         <td><a class='btn-delete' href='/api/delete?key={}' \
                         onclick=\"return confirm('确定要删除「{}」吗？此操作不可撤销。')\">删除</a></td></tr>",
                        o.name, o.uuid, o.size, o.uuid, o.name));
                }
            }
            respond(stream, "200 OK", "text/html", &page("删除对象",
                &format!("<table><tr><th>名称</th><th>UUID</th><th>大小</th><th>操作</th></tr>{}</table>\
                    <a class='btn-back' href='/manage'>← 返回</a>", rows)));
            return;
        }
    };
    let uuid = {
        let mut st = storage.lock().unwrap();
        match st.find_info(&key) {
            Ok(info) => info.uuid,
            Err(e) => {
                respond(stream, "404", "text/html", &page("未找到",
                    &format!("<p class='error'>{}</p><a class='btn-back' href='/api/delete'>← 返回</a>", e)));
                return;
            }
        }
    };
    let mut st = storage.lock().unwrap();
    match st.delete(&key) {
        Ok(()) => {
            cache.remove(&uuid);
            respond(stream, "200 OK", "text/html", &page("删除成功",
                &format!("<div class='success'>✓ 对象「{}」已成功删除</div><a class='btn-back' href='/manage'>← 返回管理页面</a>", key)));
        }
        Err(e) => {
            respond(stream, "500", "text/html", &page("删除失败",
                &format!("<p class='error'>删除失败：{}</p><a class='btn-back' href='/api/delete'>← 返回</a>", e)));
        }
    }
}

fn handle_web_resize(stream: &mut TcpStream, body: &str, cache: &Arc<ObjectCache>) {
    let cap_str = get_form_field(body, "capacity").unwrap_or_default();
    match cap_str.parse::<usize>() {
        Ok(cap) if cap > 0 => {
            let old = cache.capacity();
            cache.resize(cap);
            let cs = cache.stats();
            respond(stream, "200 OK", "text/html", &page("缓存调整成功",
                &format!("<div class='success'>✓ 缓存容量已从 {} 调整到 {}</div>
                <table class='info-table'><tr><th>当前条目</th><td>{} / {}</td></tr>
                <tr><th>命中率</th><td>{:.2}%</td></tr></table>
                <a class='btn-back' href='/manage'>← 返回</a>", old, cap, cs.size, cs.capacity, cs.hit_rate())));
        }
        _ => { respond(stream, "400", "text/html", &page("参数错误", "<p class='error'>无效的容量值</p><a class='btn-back' href='/manage'>← 返回</a>")); }
    }
}

fn handle_web_benchmark(stream: &mut TcpStream, storage: &SharedStorage, cache: &Arc<ObjectCache>) {
    let object_uuids: Vec<String> = {
        storage.lock().unwrap().list().unwrap_or_default().into_iter().map(|o| o.uuid).collect()
    };
    if object_uuids.is_empty() {
        respond(stream, "200 OK", "text/html", &page("性能测试",
            "<div class='empty'>暂无对象，请先上传一些文件后再进行测试</div><a class='btn-back' href='/manage'>← 返回</a>"));
        return;
    }
    let n = object_uuids.len();
    let iterations = 200;
    let workload: Vec<String> = (0..iterations).map(|i| object_uuids[i % n].clone()).collect();
    let cap = cache.capacity().min(n).max(2);
    let mut rows = String::new();
    let mut best = ("", 0.0);
    for alg in CacheAlgorithmType::all() {
        let bc = ObjectCache::new(*alg, cap);
        let r = bc.benchmark_run(&workload, &[]);
        if r.hit_rate > best.1 { best = (alg.as_str(), r.hit_rate); }
        rows.push_str(&format!("<tr><td><strong>{}</strong></td><td>{}</td><td>{}</td><td>{:.2}%</td></tr>",
                               alg.as_str(), r.hits, r.misses, r.hit_rate));
    }
    respond(stream, "200 OK", "text/html", &page("性能测试结果",
        &format!("<table><tr><th>算法</th><th>命中</th><th>未命中</th><th>命中率</th></tr>{}</table>
        <p style='margin-top:1em'><strong>最优算法：{}</strong>（{:.2}% 命中率）</p>
        <p style='color:#8590a6'>测试条件：{} 个对象，{} 次迭代，缓存容量 {} </p>
        <a class='btn-back' href='/manage'>← 返回</a>",
        rows, best.0, best.1, n, iterations, cap)));
}

// ============================================================================
// Page template — Zhihu-inspired design, all Chinese
// ============================================================================

fn page(title: &str, body: &str) -> String {
    page_with_refresh(title, body, false)
}

fn page_with_refresh(title: &str, body: &str, auto_refresh: bool) -> String {
    let refresh_tag = if auto_refresh { "<meta http-equiv=\"refresh\" content=\"5\">" } else { "" };
    format!(r##"<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
{}<title>MiniOS - {}</title>
<style>
/* === Reset & Base === */
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, 'PingFang SC', 'Hiragino Sans GB',
               'Microsoft YaHei', 'Segoe UI', sans-serif;
  background: #f6f6f6; color: #1a1a1a; line-height: 1.7; min-height: 100vh;
}}

/* === Header === */
.header {{
  background: #fff; box-shadow: 0 1px 3px rgba(18,18,18,0.08);
  position: sticky; top: 0; z-index: 100;
}}
.header-inner {{
  max-width: 1000px; margin: 0 auto; padding: 0 20px;
  display: flex; align-items: center; height: 56px;
}}
.logo {{
  font-size: 1.3em; font-weight: 700; color: #0066ff; margin-right: 32px;
  text-decoration: none; letter-spacing: -0.5px;
}}
.logo span {{ color: #333; font-weight: 400; }}

/* === Nav === */
.nav {{ display: flex; gap: 4px; }}
.nav a {{
  padding: 8px 16px; border-radius: 6px; color: #555; text-decoration: none;
  font-size: 0.95em; transition: all 0.15s; font-weight: 500;
}}
.nav a:hover, .nav a.active {{ background: #0066ff10; color: #0066ff; }}

/* === Main === */
.main {{ max-width: 1000px; margin: 24px auto; padding: 0 20px; }}

/* === Cards === */
.card {{
  background: #fff; border-radius: 8px; padding: 24px;
  margin-bottom: 16px; box-shadow: 0 1px 3px rgba(18,18,18,0.06);
}}
.card h2 {{
  font-size: 1.15em; font-weight: 600; color: #1a1a1a;
  margin-bottom: 16px; padding-bottom: 12px; border-bottom: 1px solid #f0f0f0;
  display: flex; align-items: center; gap: 8px;
}}
.card h2 .icon {{ font-size: 1.2em; }}

/* === Tables === */
table {{ width: 100%; border-collapse: collapse; }}
th, td {{ padding: 10px 14px; text-align: left; border-bottom: 1px solid #f0f0f0; }}
th {{ background: #fafafa; color: #8590a6; font-weight: 500; font-size: 0.9em; }}
tr:hover {{ background: #fafafa; }}

/* === Forms === */
label {{ display: block; font-weight: 500; color: #444; margin: 16px 0 6px; font-size: 0.95em; }}
input, textarea, select {{
  width: 100%; padding: 10px 14px; border: 1px solid #e0e0e0; border-radius: 6px;
  font-size: 0.95em; font-family: inherit; transition: border-color 0.15s;
  background: #fafafa;
}}
input:focus, textarea:focus {{ outline: none; border-color: #0066ff; background: #fff; }}
textarea {{ resize: vertical; min-height: 120px; }}

/* === Buttons === */
.btn {{
  display: inline-block; padding: 10px 24px; border: none; border-radius: 6px;
  font-size: 0.95em; font-weight: 500; cursor: pointer; text-decoration: none;
  transition: all 0.15s; margin: 8px 8px 0 0;
}}
.btn-primary {{ background: #0066ff; color: #fff; }}
.btn-primary:hover {{ background: #0052cc; }}
.btn-danger {{ background: #fff; color: #e74c3c; border: 1px solid #e74c3c; }}
.btn-danger:hover {{ background: #e74c3c; color: #fff; }}
.btn-back {{
  display: inline-block; margin-top: 16px; color: #8590a6; text-decoration: none;
  font-size: 0.95em;
}}
.btn-back:hover {{ color: #0066ff; }}
.obj-link {{ color: #0066ff; text-decoration: none; font-weight: 500; }}
.obj-link:hover {{ text-decoration: underline; }}
.btn-delete {{ color: #e74c3c; text-decoration: none; font-weight: 500; font-size: 0.9em; }}
.btn-delete:hover {{ text-decoration: underline; }}

/* === Stats Grid === */
.stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin: 16px 0; }}
.stat-item {{ text-align: center; padding: 16px; background: #fafafa; border-radius: 8px; }}
.stat-value {{ font-size: 1.8em; font-weight: 700; color: #0066ff; }}
.stat-label {{ font-size: 0.85em; color: #8590a6; margin-top: 4px; }}

/* === Utilities === */
.success {{ color: #00a854; font-weight: 500; font-size: 1.05em; margin: 8px 0; }}
.error {{ color: #e74c3c; font-weight: 500; margin: 8px 0; }}
.empty {{ color: #8590a6; text-align: center; padding: 32px 0; }}
.info-table td, .info-table th {{ border: none; }}
.info-table td:first-child, .info-table th:first-child {{ width: 80px; color: #8590a6; }}
.footer {{
  text-align: center; color: #8590a6; font-size: 0.8em;
  padding: 24px 0; border-top: 1px solid #f0f0f0; margin-top: 32px;
}}

/* === Progress Bar === */
.bar {{ height: 8px; border-radius: 4px; background: #f0f0f0; overflow: hidden; margin: 8px 0; }}
.bar-fill {{ height: 100%; border-radius: 4px; transition: width .5s; }}
.bar-green {{ background: linear-gradient(90deg, #0066ff, #00a854); }}
</style>
</head>
<body>

<div class="header">
  <div class="header-inner">
    <a class="logo" href="/">Mini<span>OS</span></a>
    <div class="nav">
      <a href="/" {}>总览</a>
      <a href="/manage" {}>管理</a>
      <a href="/api/get">下载</a>
      <a href="/api/delete">删除</a>
      <a href="/metrics">监控</a>
    </div>
  </div>
</div>

<div class="main">
  <div class="card">
    <h2>{}</h2>
    {}
  </div>
</div>

<div class="footer">
  MiniOS v{} · 轻量级对象存储服务 · Web 管理控制台
</div>

</body>
</html>"##,
    refresh_tag,
    title,
    if title.contains("总览") {{ "class='active'" }} else {{ "" }},
    if title.contains("管理") || title.contains("上传") || title.contains("下载") || title.contains("删除") || title.contains("性能") || title.contains("缓存") {{ "class='active'" }} else {{ "" }},
    title, body, env!("CARGO_PKG_VERSION"))
}

// ============================================================================
// Page builders
// ============================================================================

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
                 <td style='font-family:monospace;font-size:0.82em;color:#8590a6'>{}</td>\
                 <td>{} 字节</td><td>{}</td><td>{}</td></tr>",
                o.name, o.uuid, o.size, o.content_type, o.created_at));
        }
    }
    let status = st.status();
    drop(st);
    let cap = cache.capacity();
    let cs = cache.stats();

    page("对象管理", &format!(
        r##"<div class="stats-grid">
<div class="stat-item"><div class="stat-value">{}</div><div class="stat-label">已存对象</div></div>
<div class="stat-item"><div class="stat-value">{:.1}%</div><div class="stat-label">容量使用率</div></div>
<div class="stat-item"><div class="stat-value">{}</div><div class="stat-label">缓存容量</div></div>
<div class="stat-item"><div class="stat-value">{:.1}%</div><div class="stat-label">命中率</div></div>
</div>

<h2 style="margin-top:24px">上传对象</h2>
<form method="POST" action="/api/put">
  <label>对象名称</label>
  <input name="name" required placeholder="例如：my-document.txt">
  <label>内容类型</label>
  <input name="type" value="text/plain" placeholder="text/plain">
  <label>标签（JSON 格式）</label>
  <input name="tags" value='{{{{}}}}' placeholder='{{{{"author":"me"}}}}'>
  <label>内容</label>
  <textarea name="content" required placeholder="在此粘贴文件内容..."></textarea>
  <button class="btn btn-primary" type="submit">上传</button>
</form>

<h2 style="margin-top:24px">缓存控制</h2>
<form method="POST" action="/api/resize" style="display:flex;align-items:flex-end;gap:12px;flex-wrap:wrap">
  <div style="flex:1;min-width:200px">
    <label>缓存容量（当前 {}）</label>
    <input name="capacity" type="number" min="1" value="{}" required>
  </div>
  <button class="btn btn-primary" type="submit" style="flex-shrink:0">调整容量</button>
</form>
<a class="btn btn-primary" href="/api/benchmark">运行性能测试</a>

<h2 style="margin-top:24px">对象列表（共 {} 个）</h2>
<table><tr><th>名称</th><th>UUID</th><th>大小</th><th>类型</th><th>创建时间</th></tr>{}</table>
<p style="color:#8590a6;margin-top:8px">存储用量：{} / {} 块（{} 每块）</p>"##,
        status.object_count,
        pct(status.used_capacity, status.total_capacity),
        cap,
        cs.hit_rate(),
        cap, cap.max(2),
        status.object_count, table,
        status.used_blocks, status.total_blocks, fmt_bytes(status.block_size as u64),
    ))
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
<table>
<tr><td>空闲页 / 总页数</td><td>{} / {}</td></tr>
<tr><td>页大小</td><td>4 KB</td></tr>
</table>"##,
        fmt_uptime(uptime),
        status.object_count,
        cs.hit_rate(),
        cs.algorithm,
        fmt_bytes(status.used_capacity), fmt_bytes(status.total_capacity),
        pct(status.used_capacity, status.total_capacity),
        pct(status.used_capacity, status.total_capacity),
        status.used_blocks, status.total_blocks, fmt_bytes(status.block_size as u64),
        status.object_count, status.max_objects,
        cs.algorithm, cs.size, cs.capacity, cs.hits, cs.misses,
        cs.evictions, cs.hit_rate(),
        shm.free_page_count(), shm.num_pages(),
    ), true)
}

// ============================================================================
// Prometheus metrics (unchanged)
// ============================================================================

fn build_metrics(storage: &SharedStorage, cache: &Arc<ObjectCache>, shm: &Arc<SharedMemory>, start_time: Instant) -> String {
    let st = storage.lock().unwrap();
    let status = st.status();
    drop(st);
    let cs = cache.stats();
    let uptime = start_time.elapsed().as_secs();
    let mut m = String::new();
    m.push_str(&format!("# HELP minios_uptime_seconds 服务运行时间（秒）\n# TYPE minios_uptime_seconds gauge\nminios_uptime_seconds {}\n", uptime));
    m.push_str(&format!("# HELP minios_objects_total 已存储对象总数\n# TYPE minios_objects_total gauge\nminios_objects_total {}\n", status.object_count));
    m.push_str(&format!("# HELP minios_storage_blocks_total 数据块总数\n# TYPE minios_storage_blocks_total gauge\nminios_storage_blocks_total {}\n", status.total_blocks));
    m.push_str(&format!("# HELP minios_storage_blocks_used 已用数据块\n# TYPE minios_storage_blocks_used gauge\nminios_storage_blocks_used {}\n", status.used_blocks));
    m.push_str(&format!("# HELP minios_storage_blocks_free 空闲数据块\n# TYPE minios_storage_blocks_free gauge\nminios_storage_blocks_free {}\n", status.free_blocks));
    m.push_str(&format!("# HELP minios_storage_bytes_total 总容量（字节）\n# TYPE minios_storage_bytes_total gauge\nminios_storage_bytes_total {}\n", status.total_capacity));
    m.push_str(&format!("# HELP minios_storage_bytes_used 已用容量（字节）\n# TYPE minios_storage_bytes_used gauge\nminios_storage_bytes_used {}\n", status.used_capacity));
    m.push_str(&format!("# HELP minios_cache_hits_total 缓存命中次数\n# TYPE minios_cache_hits_total counter\nminios_cache_hits_total {}\n", cs.hits));
    m.push_str(&format!("# HELP minios_cache_misses_total 缓存未命中次数\n# TYPE minios_cache_misses_total counter\nminios_cache_misses_total {}\n", cs.misses));
    m.push_str(&format!("# HELP minios_cache_evictions_total 缓存淘汰次数\n# TYPE minios_cache_evictions_total counter\nminios_cache_evictions_total {}\n", cs.evictions));
    m.push_str(&format!("# HELP minios_cache_size 当前缓存条目数\n# TYPE minios_cache_size gauge\nminios_cache_size {}\n", cs.size));
    m.push_str(&format!("# HELP minios_cache_capacity 缓存最大容量\n# TYPE minios_cache_capacity gauge\nminios_cache_capacity {}\n", cs.capacity));
    m.push_str(&format!("# HELP minios_cache_hit_rate_percent 命中率百分比\n# TYPE minios_cache_hit_rate_percent gauge\nminios_cache_hit_rate_percent {:.2}\n", cs.hit_rate()));
    m.push_str(&format!("# HELP minios_cache_algorithm_info 当前缓存算法\n# TYPE minios_cache_algorithm_info gauge\nminios_cache_algorithm_info{{algorithm=\"{}\"}} 1\n", cs.algorithm));
    m.push_str(&format!("# HELP minios_shm_pages_total 共享内存总页数\n# TYPE minios_shm_pages_total gauge\nminios_shm_pages_total {}\n", shm.num_pages()));
    m.push_str(&format!("# HELP minios_shm_pages_free 共享内存空闲页数\n# TYPE minios_shm_pages_free gauge\nminios_shm_pages_free {}\n", shm.free_page_count()));
    m
}

fn fmt_bytes(bytes: u64) -> String {
    let u = ["B","KB","MB","GB"];
    let (mut v, mut i) = (bytes as f64, 0);
    while v >= 1024.0 && i < u.len()-1 { v /= 1024.0; i += 1; }
    format!("{:.2} {}", v, u[i])
}
fn pct(part: u64, total: u64) -> f64 {
    if total == 0 { 0.0 } else { part as f64 / total as f64 * 100.0 }
}
fn fmt_uptime(s: u64) -> String {
    if s < 60 { format!("{} 秒", s) }
    else if s < 3600 { format!("{} 分 {} 秒", s/60, s%60) }
    else { format!("{} 时 {} 分", s/3600, (s%3600)/60) }
}

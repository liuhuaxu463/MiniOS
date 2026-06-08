use crate::access_log::AccessLog;
use crate::cache::{CachedObject, ObjectCache, CacheAlgorithmType, generate_weighted_workload};
use crate::shm::SharedMemory;
use crate::storage::SharedStorage;
use log::{debug, error, info};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

/// 指标服务器，提供 Web 管理界面和 Prometheus 监控端点。
pub struct MetricsServer {
    port: u16,
    running: Arc<AtomicBool>,
}

impl MetricsServer {
    /// 创建一个新的 `MetricsServer` 实例，绑定到指定端口。
    pub fn new(port: u16) -> Self { Self { port, running: Arc::new(AtomicBool::new(false)) } }

    /// 启动指标服务器，在后台线程中监听 TCP 连接。
    /// `port=0` 表示禁用 Web 管理界面。
    /// 每个连接都会生成一个新线程来处理，通过 `dispatch` 函数路由请求。
    pub fn start(&mut self, storage: SharedStorage, cache: Arc<ObjectCache>,
                 shm: Arc<SharedMemory>, access_log: Arc<AccessLog>, start_time: Instant) {
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
                    Ok(s) => { let st=storage.clone(); let ca=cache.clone(); let sh=shm.clone(); let al=access_log.clone();
                        thread::spawn(move || dispatch(s, &st, &ca, &sh, &al, start_time)); }
                    Err(e) => error!("Web accept 错误: {}", e),
                }
            }
        });
    }

    /// 停止指标服务器，通知后台线程退出 accept 循环。
    pub fn stop(&mut self) { self.running.store(false, Ordering::SeqCst); }
}

// ============================================================================
// HTTP 解析
// ============================================================================

/// 读取 HTTP 请求。返回 `(头部字符串, 原始正文字节)`。
/// 头部保证为 ASCII 编码，正文保留为原始 `Vec<u8>` 以避免
/// 二进制数据（PNG、JPEG 等）损坏。
fn read_request(stream: &mut TcpStream) -> Option<(String, Vec<u8>)> {
    stream.set_read_timeout(Some(std::time::Duration::from_secs(30))).ok();
    stream.set_nonblocking(false).ok();
    let mut buf = vec![0u8; 65536];
    let mut total = 0;

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

                // 将正文复制为原始字节 —— 不进行字符串转换
                let body_bytes = buf[body_offset..total].to_vec();
                return Some((hdr_str, body_bytes));
            }
        }
    }
    if total == 0 { return None; }
    let raw = String::from_utf8_lossy(&buf[..total]);
    let hdr_end = raw.find("\r\n\r\n").map(|i| i + 4).unwrap_or(raw.len());
    let headers = raw[..hdr_end.saturating_sub(4)].to_string();
    let body_bytes = buf[hdr_end..total].to_vec();
    Some((headers, body_bytes))
}

/// 从 TCP 流中读取数据。处理 `WouldBlock` 错误时自动等待并重试一次。
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

/// 解析 HTTP 请求并分发到对应的处理器。
/// 根据请求方法和路径路由到不同的 handler 函数：
/// - `/metrics` 返回 Prometheus 指标
/// - `/` 返回系统仪表盘
/// - `/manage` 返回管理页面
/// - `/api/put`、`/api/get`、`/api/delete` 等为 API 端点
fn dispatch(mut stream: TcpStream, storage: &SharedStorage, cache: &Arc<ObjectCache>,
            shm: &Arc<SharedMemory>, access_log: &Arc<AccessLog>, start_time: Instant) {
    let (headers, body_bytes) = match read_request(&mut stream) {
        Some(v) => v,
        None => return,
    };
    let first_line = headers.lines().next().unwrap_or("");
    let (method, raw_path) = parse_first(first_line);
    let path = raw_path.split('?').next().unwrap_or(raw_path);

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
            handle_web_put(&mut stream, &headers, &body_bytes, &ct_lower, storage, cache, access_log),
        ("GET", "/api/get") =>
            handle_web_get(&mut stream, first_line, storage, cache, access_log),
        ("GET", "/api/delete") =>
            handle_web_delete(&mut stream, first_line, storage, cache, access_log),
        ("POST", "/api/resize") =>
            handle_web_resize(&mut stream, &body_bytes, cache),
        ("GET", "/api/benchmark") =>
            handle_web_benchmark(&mut stream, storage, cache),
        ("GET", "/api/search") =>
            handle_web_search(&mut stream, first_line, storage),
        _ => respond(&mut stream, "404 Not Found", "text/plain", "404\n"),
    }
}

/// 解析 HTTP 请求的第一行，提取出 `(方法, 路径)`。
fn parse_first(line: &str) -> (&str, &str) {
    let p: Vec<&str> = line.split_whitespace().collect();
    if p.len() >= 2 { (p[0], p[1]) } else { ("GET", "/") }
}

/// 向客户端发送 HTTP 响应，包含状态码、内容类型和正文。
fn respond(stream: &mut TcpStream, status: &str, ct: &str, body: &str) {
    let b = body.as_bytes();
    let r = format!("HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    status, ct, b.len());
    let _ = stream.write_all(r.as_bytes());
    let _ = stream.write_all(b);
}

/// 向客户端发送 200 OK HTTP 响应。
fn respond_ok(stream: &mut TcpStream, ct: &str, body: &str) {
    respond(stream, "200 OK", ct, body);
}

/// 从 HTTP 请求的 URL 查询字符串中提取指定键的值。
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

/// 对 URL 编码的字符串进行解码（处理 `%XX` 和 `+` 字符）。
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
// Multipart 表单数据解析
// ============================================================================

struct MultipartField { name: String, data: Vec<u8> }

/// 解析 multipart/form-data 正文。接收原始字节以保留二进制
/// 文件内容（PNG、JPEG 等），避免数据损坏。
fn parse_multipart(headers: &str, body: &[u8]) -> Vec<MultipartField> {
    let ct = headers.lines()
        .find(|l| l.to_lowercase().starts_with("content-type:"))
        .unwrap_or("");
    let boundary = ct.split("boundary=").nth(1).map(|b| b.trim().trim_matches('"')).unwrap_or("");
    if boundary.is_empty() { return vec![]; }

    let boundary_marker = format!("--{}", boundary);
    let bm = boundary_marker.as_bytes();
    let body_bytes = body;
    let mut fields = Vec::new();

    // 查找所有边界标记的位置
    let mut positions: Vec<usize> = Vec::new();
    let mut search_from = 0;
    while search_from < body_bytes.len() {
        if let Some(pos) = find_bytes(&body_bytes[search_from..], bm) {
            let abs_pos = search_from + pos;
            // 必须位于行首（前面是 \r\n，或者在位置 0）
            let is_start_of_line = abs_pos == 0
                || (abs_pos >= 2 && &body_bytes[abs_pos-2..abs_pos] == b"\r\n");
            // 检查不是结束边界（接下来的两个字符是 --）
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
        // 跳过紧接在边界后面的 \r\n
        let section_start = if section_start + 2 <= body_bytes.len()
            && &body_bytes[section_start..section_start+2] == b"\r\n" {
            section_start + 2 } else { section_start };
        let section_end = if i + 1 < positions.len() {
            positions[i + 1] - 2  // -2 是为了跳过一个边界前面的 \r\n
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

/// 在字节数组中查找子序列，返回第一次出现的位置。
fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// 解析单个 multipart 表单字段区域，提取字段名称和数据内容。
fn parse_field_section(data: &[u8]) -> Option<MultipartField> {
    // 查找头部/正文分隔符：\r\n\r\n
    let split = data.windows(4).position(|w| w == b"\r\n\r\n");
    let body_start = split.map(|s| s + 4).unwrap_or(0);
    let headers = std::str::from_utf8(&data[..body_start.saturating_sub(4)]).unwrap_or("");
    let mut content = &data[body_start..];
    // 只修剪尾部的 CRLF，不修剪头部（内容可能以空格等字符开头）
    while content.ends_with(b"\r\n") { content = &content[..content.len()-2]; }
    while content.ends_with(b"\n") { content = &content[..content.len()-1]; }

    let cd = headers.lines().find(|l| l.to_lowercase().starts_with("content-disposition:")).unwrap_or("");
    let name = cd.split("name=\"").nth(1).and_then(|s| s.split('"').next());

    name.map(|n| MultipartField { name: n.to_string(), data: content.to_vec() })
}

/// 从原始字节中解析 URL 编码的表单正文。仅对 ASCII 的 key=value 对
/// 进行字符串转换以完成解析 —— 字段值会进行 URL 解码。
fn get_form_field_urlencoded(body: &[u8], field: &str) -> Option<String> {
    let bstr = String::from_utf8_lossy(body);
    for pair in bstr.split('&') {
        let mut kv = pair.splitn(2, '=');
        if kv.next() == Some(field) {
            if let Some(v) = kv.next() { if !v.is_empty() { return Some(url_decode(v)); } }
        }
    }
    None
}

// ============================================================================
// 请求处理器
// ============================================================================

/// 处理 Web 上传请求（POST /api/put）。
/// 支持 multipart/form-data 和 URL 编码两种表单格式。
/// 将对象写入存储并更新缓存。
fn handle_web_put(stream: &mut TcpStream, headers: &str, body: &[u8], ct: &str,
                  storage: &SharedStorage, cache: &Arc<ObjectCache>, access_log: &Arc<AccessLog>) {
    let (name, content, ctype, tags) = if ct.contains("multipart/form-data") {
        let fields = parse_multipart(headers, body);
        let name = fields.iter().find(|f| f.name=="name").map(|f| String::from_utf8_lossy(&f.data).to_string());
        // 仅在文件数据非空时使用文件数据；否则回退到文本域内容
        let file_data = fields.iter().find(|f| f.name=="file")
            .filter(|f| !f.data.is_empty()).map(|f| f.data.clone());
        let text_data = fields.iter().find(|f| f.name=="content")
            .filter(|f| !f.data.is_empty()).map(|f| f.data.clone());
        let content_data = file_data.or(text_data).unwrap_or_default();
        let ct_val = fields.iter().find(|f| f.name=="type").map(|f| String::from_utf8_lossy(&f.data).to_string());
        let tags_val = fields.iter().find(|f| f.name=="tags").map(|f| String::from_utf8_lossy(&f.data).to_string());
        (name.unwrap_or_default(), content_data, ct_val.unwrap_or_else(|| "application/octet-stream".to_string()), tags_val.unwrap_or_else(|| "{}".to_string()))
    } else {
        // URL 编码表单
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

    let mut st = storage.write().unwrap();
    match st.put(&name, &content, &ctype, &tags) {
        Ok(info) => {
            cache.put(&info.uuid, CachedObject {
                uuid: info.uuid.clone(), data: content.clone(), name: info.name.clone(),
                content_type: info.content_type.clone(), size: info.size, tags: info.tags.clone(),
            });
            info!("Web 上传: 名称='{}' uuid={} 大小={}", name, info.uuid, info.size);
            access_log.record("PUT", &info.name, &info.uuid, info.size, &info.content_type, &info.tags);
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

/// 处理 Web 下载/列出对象请求（GET /api/get）。
/// 携带 `?key=` 参数时下载指定对象（优先从缓存读取），
/// 不带参数时展示所有已存储对象的可下载列表。
fn handle_web_get(stream: &mut TcpStream, first_line: &str, storage: &SharedStorage, cache: &Arc<ObjectCache>, access_log: &Arc<AccessLog>) {
    if let Some(key) = get_query_param(first_line, "key") {
        // 第一步：将 key 解析为 UUID（find_info 只读取元数据，不读取数据块）
        let info = match storage.read().unwrap().find_info(&key) {
            Ok(info) => info,
            Err(e) => {
                respond_ok(stream, "text/html; charset=utf-8", &page("未找到",
                    &format!("<p class='error'>对象未找到：{}</p><a class='btn-back' href='/api/get'>返回</a>", e)));
                return;
            }
        };

        // 第二步：先按 UUID 查询缓存（逻辑与 CLI 的 handle_get 相同）
        let data = if let Some(cached) = cache.get(&info.uuid) {
            debug!("Web GET cache HIT for uuid={}", info.uuid);
            cached.data
        } else {
            debug!("Web GET cache MISS, reading from disk");
            let st = storage.read().unwrap();
            match st.get(&key) {
                Ok((_info, storage_data)) => {
                    // 按 UUID 更新缓存，以便后续 GET 能命中
                    cache.put(&info.uuid, CachedObject {
                        uuid: info.uuid.clone(), data: storage_data.clone(),
                        name: info.name.clone(), content_type: info.content_type.clone(),
                        size: info.size, tags: info.tags.clone(),
                    });
                    storage_data
                }
                Err(e) => {
                    respond_ok(stream, "text/html; charset=utf-8", &page("未找到",
                        &format!("<p class='error'>{}<a class='btn-back' href='/api/get'>返回</a>", e)));
                    return;
                }
            }
        };

        // 以文件下载方式发送
        let hdr = format!("HTTP/1.1 200 OK\r\nContent-Type: {}\r\n\
            Content-Disposition: attachment; filename=\"{}\"\r\nContent-Length: {}\r\n\
            Connection: close\r\n\r\n", info.content_type, info.name, data.len());
        access_log.record("GET", &info.name, &info.uuid, info.size, &info.content_type, &info.tags);
        let _ = stream.write_all(hdr.as_bytes());
        let _ = stream.write_all(&data);
        return;
    }

    // 未提供 key —— 列出所有对象
    let st = storage.read().unwrap();
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
        format!("<input type='text' id='searchBox' placeholder='输入关键词实时筛选...' \
            style='width:100%;padding:8px 12px;margin-bottom:12px;border:1px solid #e0e0e0;\
            border-radius:6px;font-size:.95em' oninput=\"\
            var v=this.value.toLowerCase();\
            document.querySelectorAll('#objTable tbody tr').forEach(function(r){{ \
              r.style.display=r.textContent.toLowerCase().indexOf(v)>=0?'':'none'\
            }});\">\
            <p>点击文件名即可下载：</p>\
            <table id='objTable'><tr><th>名称</th><th>UUID</th><th>大小</th><th>创建时间</th></tr>{}</table>", rows)
    };
    respond_ok(stream, "text/html; charset=utf-8",
        &page_tab("下载对象", &format!("{}<a class='btn-back' href='/manage'>返回</a>", h), "download"));
}

/// 处理 Web 删除请求（GET /api/delete）。
/// 携带 `?key=` 参数时删除指定对象并从缓存中移除，
/// 不带参数时展示所有可删除对象的列表（含确认删除链接）。
fn handle_web_delete(stream: &mut TcpStream, first_line: &str, storage: &SharedStorage, cache: &Arc<ObjectCache>, access_log: &Arc<AccessLog>) {
    if let Some(key) = get_query_param(first_line, "key") {
        let uuid = {
            let st = storage.read().unwrap();
            match st.find_info(&key) {
                Ok(info) => info.uuid,
                Err(e) => {
                    respond_ok(stream, "text/html; charset=utf-8", &page("未找到",
                        &format!("<p class='error'>{}</p><a class='btn-back' href='/api/delete'>返回</a>", e)));
                    return;
                }
            }
        };
        let mut st = storage.write().unwrap();
        match st.delete(&key) {
            Ok(()) => {
                cache.remove(&uuid);
                access_log.record("DELETE", &key, &uuid, 0, "", "{}");
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

    // 未提供 key —— 列出带有删除链接的对象
    let st = storage.read().unwrap();
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
        &format!("<input type='text' id='searchBoxDel' placeholder='输入关键词实时筛选...' \
            style='width:100%;padding:8px 12px;margin-bottom:12px;border:1px solid #e0e0e0;\
            border-radius:6px;font-size:.95em' oninput=\"\
            var v=this.value.toLowerCase();\
            document.querySelectorAll('#delTable tbody tr').forEach(function(r){{ \
              r.style.display=r.textContent.toLowerCase().indexOf(v)>=0?'':'none'\
            }});\">\
            <table id='delTable'><tr><th>名称</th><th>UUID</th><th>大小</th><th>操作</th></tr>{}</table>\
            <a class='btn-back' href='/manage'>返回</a>", rows), "delete"));
}

/// 处理缓存容量调整请求（POST /api/resize）。
/// 从表单中读取新的容量值并调用 `cache.resize()`。
fn handle_web_resize(stream: &mut TcpStream, body: &[u8], cache: &Arc<ObjectCache>) {
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

/// 处理对象搜索请求（GET /api/search）。
/// 支持按名称、标签、类型关键词模糊搜索，以及按创建时间范围（after/before）筛选。
fn handle_web_search(stream: &mut TcpStream, first_line: &str, storage: &SharedStorage) {
    let name = get_query_param(first_line, "name");
    let tag = get_query_param(first_line, "tag");
    let ctype = get_query_param(first_line, "type");
    let after = get_query_param(first_line, "after");
    let before = get_query_param(first_line, "before");

    let all = {
        let st = storage.read().unwrap();
        st.list().unwrap_or_default()
    };

    let filtered: Vec<_> = all.into_iter().filter(|o| {
        if let Some(ref n) = name {
            if !o.name.to_lowercase().contains(&n.to_lowercase()) { return false; }
        }
        if let Some(ref t) = tag {
            if !o.tags.to_lowercase().contains(&t.to_lowercase()) { return false; }
        }
        if let Some(ref ct) = ctype {
            if !o.content_type.to_lowercase().contains(&ct.to_lowercase()) { return false; }
        }
        // after/before —— 对 created_at 的日期部分进行简单字符串前缀匹配
        if let Some(ref a) = after {
            if &o.created_at[..a.len().min(10)] < &a[..a.len().min(10)] { return false; }
        }
        if let Some(ref b) = before {
            if &o.created_at[..b.len().min(10)] > &b[..b.len().min(10)] { return false; }
        }
        true
    }).collect();

    let mut rows = String::new();
    if filtered.is_empty() {
        rows = "<tr><td colspan='5' class='empty'>无匹配结果，换个关键词试试</td></tr>".to_string();
    } else {
        for o in &filtered {
            rows.push_str(&format!(
                "<tr><td><strong>{}</strong></td>\
                 <td style='font-family:monospace;font-size:.82em;color:#8590a6'>{}</td>\
                 <td>{} 字节</td><td>{}</td><td>{}</td></tr>",
                o.name, o.uuid, o.size, o.content_type, o.created_at));
        }
    }

    let query_desc = format!(
        "name={} tag={} type={}",
        name.as_deref().unwrap_or("—"),
        tag.as_deref().unwrap_or("—"),
        ctype.as_deref().unwrap_or("—"),
    );

    respond_ok(stream, "text/html; charset=utf-8", &page_tab("搜索结果",
        &format!("<form method='GET' action='/api/search' style='margin-bottom:16px'>\
        <div class='form-row'>\
        <div><label>名称</label><input name='name' value='{}' autocomplete='off'></div>\
        <div><label>标签</label><input name='tag' value='{}' autocomplete='off'></div>\
        <div><label>类型</label><input name='type' value='{}' autocomplete='off'></div>\
        </div>\
        <button class='btn btn-primary' type='submit' style='margin-top:8px'>搜索</button>\
        </form>\
        <p style='color:#8590a6;margin-bottom:12px'>搜索条件：{} | 找到 {} 个结果</p>\
        <table><tr><th>名称</th><th>UUID</th><th>大小</th><th>类型</th><th>创建时间</th></tr>{}</table>\
        <a class='btn-back' href='/manage'>← 返回管理页面</a>",
        name.as_deref().unwrap_or(""), tag.as_deref().unwrap_or(""), ctype.as_deref().unwrap_or(""),
        query_desc, filtered.len(), rows), "manage"));
}

/// 处理缓存算法性能基准测试请求（GET /api/benchmark）。
/// 对所有缓存算法运行相同的工作负载（基于实际下载频率加权生成），
/// 以冷启动方式对比各算法的命中率，展示最优算法。
fn handle_web_benchmark(stream: &mut TcpStream, storage: &SharedStorage, cache: &Arc<ObjectCache>) {
    let object_uuids: Vec<String> = {
        storage.read().unwrap().list().unwrap_or_default().into_iter().map(|o| o.uuid).collect()
    };
    if object_uuids.is_empty() {
        respond_ok(stream, "text/html; charset=utf-8", &page("性能测试",
            "<div class='empty'>暂无对象，请先上传一些文件后再进行测试</div><a class='btn-back' href='/manage'>返回</a>"));
        return;
    }
    let n = object_uuids.len();
    let iterations = 200;
    let freqs = cache.get_access_frequencies();
    let workload = generate_weighted_workload(&object_uuids, iterations, &freqs);
    let real_cap = cache.capacity();
    let cap = real_cap.max(1);

    // 显示每个对象的实际下载次数
    let mut freq: Vec<(u64, &str)> = object_uuids.iter()
        .map(|u| (freqs.get(u).copied().unwrap_or(0), &u[..u.len().min(8)]))
        .collect();
    freq.sort_by(|a, b| b.0.cmp(&a.0));
    let freq_str = freq.iter()
        .map(|(cnt, short)| format!("{}...(下载 {} 次)", short, cnt))
        .collect::<Vec<_>>().join(", ");

    let mut rows = String::new();
    let mut best = ("", 0.0);
    for alg in CacheAlgorithmType::all() {
        let bc = ObjectCache::new(*alg, cap);
        // 冷启动无预加载：所有缓存从空开始，让每种算法的淘汰策略
        // 自然决定哪些对象保留在缓存中。
        let r = bc.benchmark_run(&workload, &[]);
        if r.hit_rate > best.1 { best = (alg.as_str(), r.hit_rate); }
        rows.push_str(&format!("<tr><td><strong>{}</strong></td><td>{}</td><td>{}</td><td>{:.2}%</td></tr>",
                               alg.as_str(), r.hits, r.misses, r.hit_rate));
    }

    respond_ok(stream, "text/html; charset=utf-8", &page("性能测试结果",
        &format!("<h2>缓存算法对比</h2>
        <table><tr><th>算法</th><th>命中</th><th>未命中</th><th>命中率</th></tr>{}</table>
        <p style='margin-top:1em'><strong>最优算法：{}</strong>（{:.2}% 命中率）</p>
        <p style='color:#8590a6;font-size:.85em'>
        测试条件：{} 个对象，{} 次迭代，缓存容量 {}，冷启动（无预加载）<br>
        下载记录：{}<br>
        说明：权重 = 实际下载次数 + 1，你越常下载的文件在测试中被访问的次数就越多
        </p>
        <a class='btn-back' href='/manage'>返回</a>",
        rows, best.0, best.1, n, iterations, cap, freq_str)));
}

// ============================================================================
// 页面构建器
// ============================================================================

/// 构建指定标题的基础 HTML 页面（无自动刷新，无标签页激活状态）。
fn page(title: &str, body: &str) -> String { page_with_refresh(title, body, false, "") }

/// 构建指定标题和标签页激活状态的 HTML 页面（无自动刷新）。
fn page_tab(title: &str, body: &str, tab: &str) -> String { page_with_refresh(title, body, false, tab) }

/// 构建完整的 HTML 页面，支持自动刷新和导航标签页高亮。
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

/// 构建对象管理页面（/manage 路由）。
/// 包含存储和缓存概览统计、上传表单、缓存容量控制、
/// 搜索引擎入口以及完整对象列表。
fn build_manage_page(storage: &SharedStorage, cache: &Arc<ObjectCache>) -> String {
    let st = storage.read().unwrap();
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

<h2 style="margin-top:24px">搜索对象</h2>
<form method="GET" action="/api/search" style="margin-bottom:8px">
  <div class="form-row">
    <div><label>名称</label><input name="name" placeholder="模糊搜索" autocomplete="off"></div>
    <div><label>标签</label><input name="tag" placeholder='如 author=me' autocomplete="off"></div>
    <div><label>类型</label><input name="type" placeholder='如 image' autocomplete="off"></div>
    <div style="display:flex;align-items:flex-end"><button class="btn btn-primary" type="submit">搜索</button></div>
  </div>
</form>

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

/// 构建系统总览仪表盘页面（/ 路由）。
/// 展示运行时间、对象总数、缓存命中率、存储使用率、
/// 共享内存状态等信息，并每 5 秒自动刷新。
fn build_dashboard(storage: &SharedStorage, cache: &Arc<ObjectCache>, shm: &Arc<SharedMemory>, start_time: Instant) -> String {
    let st = storage.write().unwrap();
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
// Prometheus 监控指标
// ============================================================================

/// 构建 Prometheus 文本格式的监控指标输出（/metrics 端点）。
/// 包含存储、缓存和共享内存的各项 gauge 和 counter 指标。
fn build_metrics(storage: &SharedStorage, cache: &Arc<ObjectCache>, shm: &Arc<SharedMemory>, start_time: Instant) -> String {
    let st = storage.write().unwrap(); let status = st.status(); drop(st);
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

/// 将字节数格式化为人类可读的字符串（如 `1.50 KB`、`3.00 MB`）。
fn fmt_bytes(bytes: u64) -> String {
    let u = ["B","KB","MB","GB"]; let (mut v,mut i)=(bytes as f64,0);
    while v>=1024.0 && i<u.len()-1 { v/=1024.0; i+=1; }
    format!("{:.2} {}", v, u[i])
}

/// 计算百分比：`(part / total) * 100`。当 total 为 0 时返回 0.0。
fn pct(part: u64, total: u64) -> f64 { if total==0 {0.0} else {part as f64/total as f64*100.0} }

/// 将秒数格式化为人类可读的运行时间字符串（如 `30 秒`、`5 分 30 秒`、`2 时 15 分`）。
fn fmt_uptime(s: u64) -> String {
    if s<60 {format!("{} 秒",s)} else if s<3600 {format!("{} 分 {} 秒",s/60,s%60)}
    else {format!("{} 时 {} 分",s/3600,(s%3600)/60)}
}

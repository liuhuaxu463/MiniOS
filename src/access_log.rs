//! 访问日志模块。
//!
//! 以 JSON 行格式记录每一次 PUT / GET / DELETE 操作，
//! 包含时间戳、操作类型、对象名称、UUID 和大小等信息。

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::Mutex;
use log::error;

/// 线程安全的访问日志写入器。
///
/// 每次对象存储操作（上传/下载/删除）成功后，将一条 JSON
/// 格式的日志行追加到指定文件中，用于审计和统计分析。
pub struct AccessLog {
    /// 受互斥锁保护的文件句柄。None 表示日志功能已禁用。
    file: Mutex<Option<File>>,
}

impl AccessLog {
    /// 创建新的访问日志写入器。
    ///
    /// `path` 为空字符串时，日志功能禁用；否则以追加模式打开文件。
    pub fn new(path: &str) -> Self {
        let file = if path.is_empty() {
            None
        } else {
            match OpenOptions::new().create(true).append(true).open(path) {
                Ok(f) => Some(f),
                Err(e) => {
                    error!("无法打开访问日志文件 '{}': {}", path, e);
                    None
                }
            }
        };
        Self { file: Mutex::new(file) }
    }

    /// 记录一次对象操作。
    ///
    /// 参数说明：
    /// - `op`: 操作类型（"PUT"、"GET"、"DELETE"）
    /// - `name`: 对象名称
    /// - `uuid`: 对象的全局唯一标识符
    /// - `size`: 对象大小（字节）
    /// - `content_type`: 内容类型（MIME）
    /// - `tags`: 用户自定义标签（JSON 字符串）
    pub fn record(&self, op: &str, name: &str, uuid: &str, size: u64, content_type: &str, tags: &str) {
        let now = chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f");
        let line = format!(r#"{{"ts":"{}","op":"{}","name":"{}","uuid":"{}","size":{},"type":"{}","tags":{}}}"#,
            now, op, escape_json(name), uuid, size, escape_json(content_type), tags);
        let mut guard = self.file.lock().unwrap();
        if let Some(ref mut f) = *guard {
            let _ = writeln!(f, "{}", line);
        }
    }
}

/// 对 JSON 字符串中的特殊字符进行转义处理。
///
/// 将反斜杠和双引号分别转义为 `\\` 和 `\"`，
/// 确保嵌入 JSON 值时不会破坏 JSON 结构。
fn escape_json(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

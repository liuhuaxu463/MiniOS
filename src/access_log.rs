use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use log::error;

/// Thread-safe access log writer.
///
/// Every GET / PUT / DELETE operation is appended as a JSON line
/// with timestamp, operation type, object name, UUID, and size.
pub struct AccessLog {
    file: Mutex<Option<File>>,
}

impl AccessLog {
    pub fn new(path: &str) -> Self {
        let file = if path.is_empty() {
            None
        } else {
            match OpenOptions::new().create(true).append(true).open(path) {
                Ok(f) => Some(f),
                Err(e) => {
                    error!("Cannot open access log '{}': {}", path, e);
                    None
                }
            }
        };
        Self { file: Mutex::new(file) }
    }

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

fn escape_json(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tracing::{Event, Subscriber};
use tracing_subscriber::{
    layer::{Context, SubscriberExt},
    Layer,
};

#[derive(Serialize, Clone, Debug)]
pub struct CollectedLogEntry {
    pub timestamp: String,
    pub level: String,
    pub message: String,
    pub target: String,
    pub file: Option<String>,
    pub line: Option<u32>,
    pub thread_id: Option<String>,
}

#[derive(Clone)]
pub struct LogCollector {
    logs: Arc<Mutex<VecDeque<CollectedLogEntry>>>,
    max_logs: usize,
}

impl LogCollector {
    pub fn new(max_logs: usize) -> Self {
        Self {
            logs: Arc::new(Mutex::new(VecDeque::with_capacity(max_logs))),
            max_logs,
        }
    }

    pub fn get_logs(&self) -> Vec<CollectedLogEntry> {
        let logs = self.logs.lock().unwrap();
        logs.iter().cloned().collect()
    }

    pub fn get_recent_logs(&self, count: usize) -> Vec<CollectedLogEntry> {
        let logs = self.logs.lock().unwrap();
        logs.iter()
            .rev()
            .take(count)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    pub fn clear_logs(&self) {
        let mut logs = self.logs.lock().unwrap();
        logs.clear();
    }

    fn add_log(&self, entry: CollectedLogEntry) {
        let mut logs = self.logs.lock().unwrap();
        if logs.len() >= self.max_logs {
            logs.pop_front();
        }
        logs.push_back(entry);
    }

    /// 判断是否应该过滤掉某个target的日志
    fn should_filter_target(&self, target: &str) -> bool {
        // 过滤掉这些底层库的日志
        let filtered_targets = [
            "hyper",
            "reqwest",
            "h2",
            "tower",
            "tracing",
            "tokio",
            "pool",
            "call",
            "reqwest_transport",
            "want",
            "mio",
            "runtime",
        ];

        filtered_targets
            .iter()
            .any(|&filtered| target.contains(filtered))
    }
}

impl<S> Layer<S> for LogCollector
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();

        // 提取消息内容
        let mut message = String::new();
        let mut visitor = MessageVisitor(&mut message);
        event.record(&mut visitor);

        // 如果消息为空，尝试从目标中提取信息
        if message.trim().is_empty() {
            message = format!("Log event from {}", metadata.target());
        }

        // 获取文件和行号信息
        let file = metadata.file().map(|f| {
            // 只取文件名，去掉路径
            f.split('\\')
                .last()
                .or_else(|| f.split('/').last())
                .unwrap_or(f)
                .to_string()
        });

        // 获取线程信息
        let thread_id = std::thread::current().id();
        let thread_id_str = format!("{:?}", thread_id);

        let entry = CollectedLogEntry {
            timestamp: chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string(),
            level: metadata.level().to_string().to_lowercase(),
            message: message.trim().to_string(),
            target: metadata.target().to_string(),
            file,
            line: metadata.line(),
            thread_id: Some(thread_id_str),
        };

        // 过滤掉底层库的日志，只保留应用层的日志
        let should_include = !entry.message.is_empty()
            && entry.message != "Log event from"
            && !self.should_filter_target(&entry.target);

        if should_include {
            self.add_log(entry);
        }
    }
}

struct MessageVisitor<'a>(&'a mut String);

impl<'a> tracing::field::Visit for MessageVisitor<'a> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.append_field(field.name(), &format!("{:?}", value));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.append_field(field.name(), value);
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.append_field(field.name(), &value.to_string());
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.append_field(field.name(), &value.to_string());
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.append_field(field.name(), &value.to_string());
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.append_field(field.name(), &value.to_string());
    }
}

impl<'a> MessageVisitor<'a> {
    fn append_field(&mut self, field_name: &str, value: &str) {
        if field_name == "message" {
            // 清理message字段的引号
            let clean_value = if value.starts_with('"') && value.ends_with('"') && value.len() > 1 {
                &value[1..value.len() - 1]
            } else {
                value
            };
            *self.0 = clean_value.to_string();
        } else {
            // 对于其他字段，如果是第一个字段且message为空，直接赋值
            // 否则追加到现有内容
            if self.0.is_empty() {
                *self.0 = value.to_string();
            } else {
                write!(self.0, " {}={}", field_name, value).unwrap();
            }
        }
    }
}

use std::fmt::Write;

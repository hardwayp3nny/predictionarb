use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone)]
pub struct AuditLogger {
    file_path: PathBuf,
}

impl AuditLogger {
    pub fn new(file_path: impl Into<PathBuf>) -> Self {
        Self {
            file_path: file_path.into(),
        }
    }

    pub async fn append(&self, record: &AuditRecord) -> eyre::Result<()> {
        if let Some(parent) = self.file_path.parent() {
            fs::create_dir_all(parent).await.ok();
        }
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file_path)
            .await?;
        let line = serde_json::to_string(record)? + "\n";
        f.write_all(line.as_bytes()).await?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ActionKind {
    Deposit,
    Withdraw,
    Buy,
    Sell,
    Split,
    Merge,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AuditRecord {
    pub ts: String,
    pub action: ActionKind,
    pub summary: String,
    pub details: serde_json::Value,
}

impl AuditRecord {
    pub fn now(action: ActionKind, summary: impl Into<String>, details: serde_json::Value) -> Self {
        Self {
            ts: chrono::Utc::now().to_rfc3339(),
            action,
            summary: summary.into(),
            details,
        }
    }
}

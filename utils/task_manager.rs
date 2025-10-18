use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskStatus {
    Running,
    Completed,
    Failed(String),
}

#[derive(Clone, Debug)]
pub struct TaskInfo {
    pub id: String,
    pub name: String,
    pub started_at: SystemTime,
    pub finished_at: Option<SystemTime>,
    pub status: TaskStatus,
}

#[derive(Default)]
pub struct TaskManagerInner {
    pub tasks: HashMap<String, TaskInfo>,
}

#[derive(Clone, Default)]
pub struct TaskManager {
    inner: Arc<RwLock<TaskManagerInner>>,
}

impl TaskManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn spawn<F>(&self, name: impl Into<String>, fut: F) -> String
    where
        F: std::future::Future<Output = eyre::Result<()>> + Send + 'static,
    {
        let id = Uuid::new_v4().to_string();
        let name = name.into();

        {
            let mut guard = self.inner.write().await;
            guard.tasks.insert(
                id.clone(),
                TaskInfo {
                    id: id.clone(),
                    name: name.clone(),
                    started_at: SystemTime::now(),
                    finished_at: None,
                    status: TaskStatus::Running,
                },
            );
        }

        let tm = self.clone();
        let id_for_task = id.clone();
        tokio::spawn(async move {
            let result = fut.await;
            let mut guard = tm.inner.write().await;
            if let Some(task) = guard.tasks.get_mut(&id_for_task) {
                task.finished_at = Some(SystemTime::now());
                task.status = match result {
                    Ok(()) => TaskStatus::Completed,
                    Err(e) => TaskStatus::Failed(e.to_string()),
                };
            }
        });

        id
    }

    pub async fn get(&self, id: &str) -> Option<TaskInfo> {
        let guard = self.inner.read().await;
        guard.tasks.get(id).cloned()
    }

    pub async fn list(&self) -> Vec<TaskInfo> {
        let guard = self.inner.read().await;
        guard.tasks.values().cloned().collect()
    }
}

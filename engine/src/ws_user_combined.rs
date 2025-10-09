use crate::{model::UserEvent, ports::UserStream};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

const DEDUP_WINDOW: usize = 2048;

#[derive(Default)]
struct DedupState {
    seen: HashSet<String>,
    order: VecDeque<String>,
}

impl DedupState {
    fn remember(&mut self, key: String) -> bool {
        if self.seen.contains(&key) {
            return false;
        }
        self.seen.insert(key.clone());
        self.order.push_back(key);
        if self.order.len() > DEDUP_WINDOW {
            if let Some(old) = self.order.pop_front() {
                self.seen.remove(&old);
            }
        }
        true
    }
}

struct TimingEntry {
    label: &'static str,
    time: Instant,
}

struct TimingState {
    pending: HashMap<String, TimingEntry>,
    window_count: u64,
    window_sum_abs_ms: f64,
    window_sum_signed_ms: f64,
    window_min_ms: f64,
    window_max_ms: f64,
    window_primary_first: u64,
    window_secondary_first: u64,
    window_index: u64,
    writer: Option<BufWriter<std::fs::File>>,
    log_path: PathBuf,
}

impl TimingState {
    fn new() -> Self {
        let log_dir = PathBuf::from("logs");
        if let Err(err) = std::fs::create_dir_all(&log_dir) {
            tracing::warn!(target: "ws_user_combined", ?err, "failed to create logs directory");
        }
        let log_path = log_dir.join("user_ws_timing.log");
        let writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map(|file| BufWriter::new(file))
            .map_err(|err| {
                tracing::warn!(
                    target: "ws_user_combined",
                    ?err,
                    path = %log_path.display(),
                    "failed to open timing log file"
                );
                err
            })
            .ok();
        let mut state = Self {
            pending: HashMap::new(),
            window_count: 0,
            window_sum_abs_ms: 0.0,
            window_sum_signed_ms: 0.0,
            window_min_ms: f64::MAX,
            window_max_ms: 0.0,
            window_primary_first: 0,
            window_secondary_first: 0,
            window_index: 0,
            writer,
            log_path,
        };
        state.reset_window_stats();
        state
    }

    async fn record_event(state: &Mutex<TimingState>, key: String, label: &'static str) {
        let now = Instant::now();
        let mut guard = state.lock().await;
        guard.record(key, label, now);
    }

    fn record(&mut self, key: String, label: &'static str, now: Instant) {
        if let Some(entry) = self.pending.remove(&key) {
            if entry.label == label {
                // Same stream repeated; keep the earliest timestamp for future comparison.
                let earliest = if now < entry.time { now } else { entry.time };
                self.pending.insert(
                    key,
                    TimingEntry {
                        label,
                        time: earliest,
                    },
                );
                return;
            }

            let delta_ms = now.duration_since(entry.time).as_secs_f64() * 1000.0;
            self.window_count += 1;
            self.window_sum_abs_ms += delta_ms;
            if entry.label == "primary" {
                self.window_sum_signed_ms += delta_ms;
                self.window_primary_first += 1;
            } else {
                self.window_sum_signed_ms -= delta_ms;
                self.window_secondary_first += 1;
            }
            if delta_ms < self.window_min_ms {
                self.window_min_ms = delta_ms;
            }
            if delta_ms > self.window_max_ms {
                self.window_max_ms = delta_ms;
            }

            if self.window_count >= 1_000 {
                self.flush_summary();
            }
        } else {
            self.pending.insert(key, TimingEntry { label, time: now });
        }
    }

    fn flush_summary(&mut self) {
        if self.window_count == 0 {
            return;
        }
        let avg_abs = self.window_sum_abs_ms / self.window_count as f64;
        let avg_signed = self.window_sum_signed_ms / self.window_count as f64;
        let summary = format!(
            "window={} events={} avg_abs_ms={:.3} avg_signed_ms={:.3} min_ms={:.3} max_ms={:.3} primary_first={} secondary_first={}",
            self.window_index,
            self.window_count,
            avg_abs,
            avg_signed,
            self.window_min_ms,
            self.window_max_ms,
            self.window_primary_first,
            self.window_secondary_first
        );
        tracing::info!(target: "ws_user_combined_timing", "{}", summary);
        if let Some(writer) = self.writer.as_mut() {
            if writeln!(writer, "{}", summary).is_err() {
                tracing::warn!(
                    target: "ws_user_combined",
                    path = %self.log_path.display(),
                    "failed to write timing summary"
                );
            } else if writer.flush().is_err() {
                tracing::warn!(
                    target: "ws_user_combined",
                    path = %self.log_path.display(),
                    "failed to flush timing summary"
                );
            }
        }
        self.window_index += 1;
        self.reset_window_stats();
    }

    fn reset_window_stats(&mut self) {
        self.window_count = 0;
        self.window_sum_abs_ms = 0.0;
        self.window_sum_signed_ms = 0.0;
        self.window_min_ms = f64::MAX;
        self.window_max_ms = 0.0;
        self.window_primary_first = 0;
        self.window_secondary_first = 0;
    }
}

impl Drop for TimingState {
    fn drop(&mut self) {
        if self.window_count > 0 {
            self.flush_summary();
        }
    }
}

pub struct CombinedUserStream<A, B>
where
    A: UserStream + 'static,
    B: UserStream + 'static,
{
    primary: Arc<A>,
    secondary: Arc<B>,
    tx: mpsc::Sender<UserEvent>,
    rx: Arc<Mutex<mpsc::Receiver<UserEvent>>>,
    dedup: Arc<Mutex<DedupState>>,
    bg_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    timing: Arc<Mutex<TimingState>>,
}

impl<A, B> CombinedUserStream<A, B>
where
    A: UserStream + 'static,
    B: UserStream + 'static,
{
    pub fn new(primary: Arc<A>, secondary: Arc<B>) -> Self {
        let (tx, rx) = mpsc::channel(10_000);
        Self {
            primary,
            secondary,
            tx,
            rx: Arc::new(Mutex::new(rx)),
            dedup: Arc::new(Mutex::new(DedupState::default())),
            bg_handles: Arc::new(Mutex::new(Vec::new())),
            timing: Arc::new(Mutex::new(TimingState::new())),
        }
    }

    fn event_key(ev: &UserEvent) -> String {
        match ev {
            UserEvent::Order(upd) => {
                format!("order:{}:{}:{:.6}", upd.id, upd.status, upd.size_matched)
            }
            UserEvent::Trade(tr) => format!("trade:{}:{}:{:.6}", tr.id, tr.status, tr.size),
        }
    }

    async fn reader_loop<T>(
        stream: Arc<T>,
        tx: mpsc::Sender<UserEvent>,
        dedup: Arc<Mutex<DedupState>>,
        timing: Arc<Mutex<TimingState>>,
        label: &'static str,
    ) where
        T: UserStream + 'static,
    {
        loop {
            match stream.next().await {
                Ok(Some(ev)) => {
                    let key = Self::event_key(&ev);
                    TimingState::record_event(&timing, key.clone(), label).await;
                    let mut allow = true;
                    {
                        let mut guard = dedup.lock().await;
                        if !guard.remember(key) {
                            allow = false;
                        }
                    }
                    if allow {
                        if tx.send(ev.clone()).await.is_err() {
                            tracing::warn!(target: "ws_user_combined", stream = label, "failed to forward user event, channel closed");
                            break;
                        }
                    } else {
                        tracing::debug!(target: "ws_user_combined", stream = label, "duplicate user event filtered");
                    }
                }
                Ok(None) => {
                    sleep(Duration::from_millis(50)).await;
                }
                Err(err) => {
                    tracing::warn!(target: "ws_user_combined", stream = label, ?err, "user stream error");
                    sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }
}

#[async_trait]
impl<A, B> UserStream for CombinedUserStream<A, B>
where
    A: UserStream + 'static,
    B: UserStream + 'static,
{
    async fn connect(&self) -> Result<()> {
        self.primary.connect().await?;
        self.secondary.connect().await?;
        let mut handles = self.bg_handles.lock().await;
        if handles.is_empty() {
            let primary = self.primary.clone();
            let secondary = self.secondary.clone();
            let tx_primary = self.tx.clone();
            let tx_secondary = self.tx.clone();
            let dedup_primary = self.dedup.clone();
            let dedup_secondary = self.dedup.clone();
            let timing_primary = self.timing.clone();
            let timing_secondary = self.timing.clone();
            let h1 = tokio::spawn(Self::reader_loop(
                primary,
                tx_primary,
                dedup_primary,
                timing_primary,
                "primary",
            ));
            let h2 = tokio::spawn(Self::reader_loop(
                secondary,
                tx_secondary,
                dedup_secondary,
                timing_secondary,
                "secondary",
            ));
            handles.push(h1);
            handles.push(h2);
        }
        Ok(())
    }

    async fn next(&self) -> Result<Option<UserEvent>> {
        let mut rx = self.rx.lock().await;
        Ok(rx.recv().await)
    }
}

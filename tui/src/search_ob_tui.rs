use anyhow::{anyhow, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use crossterm::{
    event::{Event, KeyCode, KeyEvent},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use engine::ports::{ActivityStream, UserStream};
use engine::{
    auth::{create_or_derive_api_creds, ApiCreds},
    http_exchange::PolymarketHttpExchange,
    http_pool::HttpPool,
    model::*,
    ws_activity::ActivityWs,
    ws_market_multi::MarketMultiWs,
    ws_user::UserWs,
    EngineConfig, ExchangeClient, MarketStream,
};
use futures::StreamExt;
use prometheus::Registry;
use ratatui::{
    backend::CrosstermBackend,
    buffer::Buffer,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, List, ListItem, Paragraph, Widget, Wrap},
    Terminal,
};
use reqwest::header::ACCEPT;
use rusqlite::{params, Connection};
use serde::{
    de::{self, Deserializer},
    Deserialize,
};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    fs, io,
    path::{Path, PathBuf},
    sync::{mpsc as std_mpsc, Arc},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{
    mpsc::{self, UnboundedSender},
    oneshot, RwLock,
};

use engine::eip712_sign::parse_pk;

mod errors;
mod local_store;
mod onboarding;
mod polymarket;
mod registration;
mod utils;

mod search;
mod tui;
use search::detail::{fetch_event_detail, EventDetail};
use search::{search_events, EventItem};
use tui::OrderBookState;

const ASCII_POLYMARKET: &[&str] = &[
    "__________      .__                              __           __   ",
    "\\______   \\____ |  | ___.__. _____ _____ _______|  | __ _____/  |_",
    " |     ___/  _ \\|  |<   |  |/     \\\\__  \\_  __ \\  |/ // __ \\   __\\",
    " |    |  (  <_> )  |_\\___  |  Y Y  \\__/ __ \\|  | \\/    <\\  ___/|  |  ",
    " |____|   \\____/|____/ ____|__|_|  (____  /__|  |__|_ \\\\___  >__|  ",
    "                     \\/          \\/     \\/           \\/    \\/       ",
];

const TICKER_SEPARATOR: &str = "   ";
const TICKER_MAX_ITEMS: usize = 100;
const TICKER_MAX_KEYS: usize = 512;
const MARKET_INFO_BATCH_LIMIT: usize = 40;
const RECENT_TRADES_PER_TOKEN: usize = 200;
const RECENT_TRADES_SCROLL_INTERVAL: u8 = 8;
const RECORDINGS_DIR: &str = "recordings";
const RECORDING_DB_EXTENSION: &str = "sqlite";

struct DepthColumn<'a> {
    title: &'a str,
    levels: &'a [(f64, f64)],
    is_bid: bool,
    highlights: &'a [f64],
}

impl<'a> DepthColumn<'a> {
    fn new(title: &'a str, levels: &'a [(f64, f64)], is_bid: bool, highlights: &'a [f64]) -> Self {
        Self {
            title,
            levels,
            is_bid,
            highlights,
        }
    }
}

impl<'a> Widget for DepthColumn<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.width == 0 || area.height == 0 {
            return;
        }

        for y in 0..area.height {
            for x in 0..area.width {
                let cell = buf.get_mut(area.x + x, area.y + y);
                cell.reset();
                cell.set_symbol(" ");
            }
        }

        let accent = if self.is_bid {
            Color::Green
        } else {
            Color::Red
        };
        let fill_color = if self.is_bid {
            Color::Rgb(70, 120, 70)
        } else {
            Color::Rgb(150, 70, 70)
        };
        let header_style = Style::default().fg(accent).add_modifier(Modifier::BOLD);
        let title_style = Style::default().fg(accent).add_modifier(Modifier::BOLD);
        let text_style = Style::default().fg(Color::White);

        write_line_with_style(
            buf,
            area.x,
            area.y,
            area.width as usize,
            self.title,
            title_style,
        );

        if area.height <= 1 {
            return;
        }

        let header = format!("{:>8} {:>8} {:>10}", "Px", "Sz", "USD");
        write_line_with_style(
            buf,
            area.x,
            area.y + 1,
            area.width as usize,
            &header,
            header_style,
        );

        let max_rows = area.height.saturating_sub(2) as usize;
        if max_rows == 0 {
            return;
        }
        let rows: Vec<&(f64, f64)> = self.levels.iter().take(max_rows).collect();
        if rows.is_empty() {
            return;
        }
        let values: Vec<f64> = rows.iter().map(|(p, s)| (*p) * (*s)).collect();
        let total: f64 = values.iter().copied().sum();
        let mut tail_sum = 0.0f64;
        let mut fills = vec![0.0; rows.len()];
        for i in (0..rows.len()).rev() {
            let fill = if total > 0.0 {
                1.0 - (tail_sum / total)
            } else {
                0.0
            };
            fills[i] = fill.clamp(0.0, 1.0);
            tail_sum += values[i];
        }

        for (idx, ((price, size), fill)) in rows.iter().zip(fills.iter()).enumerate() {
            let y = area.y + 2 + idx as u16;
            if y >= area.y + area.height {
                break;
            }
            let usd = price * size;
            let text = format!("{:>8.4} {:>8.2} {:>10.2}", price, size, usd);

            let highlight = self.highlights.iter().any(|h| (h - *price).abs() <= 1e-4);

            let mut fill_cols = ((*fill) * area.width as f64).round() as i32;
            if fill_cols < 0 {
                fill_cols = 0;
            }
            let fill_cols = fill_cols as u16;
            let fill_cols = fill_cols.min(area.width);

            let text_chars: Vec<char> = text.chars().collect();
            let width = area.width as usize;
            for i in 0..width {
                let ch = if i < text_chars.len() {
                    text_chars[i]
                } else {
                    ' '
                };
                let mut char_buf = [0u8; 4];
                let symbol = ch.encode_utf8(&mut char_buf);
                let cell = buf.get_mut(area.x + i as u16, y);
                cell.set_symbol(symbol);
                let mut style = text_style;
                if (i as u16) < fill_cols {
                    style = style.bg(fill_color).fg(Color::Black);
                }
                if highlight {
                    style = Style::default().bg(Color::Blue).fg(Color::White);
                }
                cell.set_style(style);
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, Copy)]
enum Focus {
    Search,
    Event,
    Book,
    Trade,
    Orders,
}

fn show_missing_credentials_screen(message: &str) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let result = (|| -> Result<()> {
        loop {
            terminal.draw(|f| draw_missing_credentials(f, message))?;
            if crossterm::event::poll(Duration::from_millis(200))? {
                match crossterm::event::read()? {
                    Event::Key(KeyEvent {
                        code: KeyCode::Esc, ..
                    })
                    | Event::Key(KeyEvent {
                        code: KeyCode::Char('q'),
                        ..
                    })
                    | Event::Key(KeyEvent {
                        code: KeyCode::Char('Q'),
                        ..
                    })
                    | Event::Key(KeyEvent {
                        code: KeyCode::Enter,
                        ..
                    }) => break,
                    _ => {}
                }
            }
        }
        Ok(())
    })();

    disable_raw_mode()?;
    drop(terminal);
    execute!(io::stdout(), LeaveAlternateScreen)?;
    result
}

fn draw_missing_credentials(f: &mut ratatui::Frame, message: &str) {
    let size = f.size();
    let block = Block::default()
        .title(Span::styled(
            "Polymarket Setup",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded);
    let inner = block.inner(size);
    f.render_widget(block, size);
    if inner.width == 0 || inner.height == 0 {
        return;
    }

    let mut lines: Vec<Line> = Vec::new();
    for row in ASCII_POLYMARKET {
        lines.push(Line::from(Span::styled(
            *row,
            Style::default().fg(Color::Blue),
        )));
    }
    lines.push(Line::from(""));
    lines.push(Line::from(vec![Span::styled(
        "No credentials detected.",
        Style::default().add_modifier(Modifier::BOLD),
    )]));
    lines.push(Line::from(vec![Span::raw(
        "Configure your private key in config.json or rerun setup.",
    )]));
    if !message.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled("Details: ", Style::default().fg(Color::Red)),
            Span::raw(message),
        ]));
    }
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::raw("Press "),
        Span::styled("Q", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(" or "),
        Span::styled("Esc", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(" to exit."),
    ]));

    let paragraph = Paragraph::new(lines)
        .wrap(Wrap { trim: true })
        .alignment(Alignment::Center);
    f.render_widget(paragraph, inner);
}

fn compute_highlight_prices(
    app: &AppState,
    current_token: Option<&str>,
    complement_token: Option<&str>,
) -> (Vec<f64>, Vec<f64>) {
    let mut bids = Vec::new();
    let mut asks = Vec::new();

    let Some(curr) = current_token else {
        return (bids, asks);
    };

    for order in app.orders_map.values() {
        if !matches!(order.status, OrderStatus::Live | OrderStatus::PendingNew) {
            continue;
        }

        if order.asset_id == curr {
            match order.side {
                Side::Buy => bids.push(order.price),
                Side::Sell => asks.push(order.price),
            }
        } else if complement_token
            .map(|token| token == order.asset_id)
            .unwrap_or(false)
        {
            let mapped_price = (1.0 - order.price).clamp(0.0, 1.0);
            match order.side {
                Side::Buy => asks.push(mapped_price),
                Side::Sell => bids.push(mapped_price),
            }
        }
    }

    bids.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    bids.dedup_by(|a, b| (*a - *b).abs() <= 1e-4);
    asks.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    asks.dedup_by(|a, b| (*a - *b).abs() <= 1e-4);

    (bids, asks)
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum TradeField {
    Price,
    Size,
    Side,
    Submit,
}

impl TradeField {
    fn next(self) -> Self {
        match self {
            TradeField::Price => TradeField::Size,
            TradeField::Size => TradeField::Side,
            TradeField::Side => TradeField::Submit,
            TradeField::Submit => TradeField::Price,
        }
    }

    fn prev(self) -> Self {
        match self {
            TradeField::Price => TradeField::Submit,
            TradeField::Size => TradeField::Price,
            TradeField::Side => TradeField::Size,
            TradeField::Submit => TradeField::Side,
        }
    }
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum ViewMode {
    Main,
    Positions,
    Orders,
    User,
}

struct TradeForm {
    price: String,
    size: String,
    side: Side,
    field: TradeField,
    status: Option<String>,
    last_order_id: Option<String>,
    enabled: bool,
}

impl TradeForm {
    fn new() -> Self {
        Self {
            price: String::new(),
            size: String::new(),
            side: Side::Buy,
            field: TradeField::Price,
            status: None,
            last_order_id: None,
            enabled: false,
        }
    }

    fn highlight_style(active: bool) -> Style {
        if active {
            Style::default()
                .fg(Color::Black)
                .bg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        }
    }

    fn toggle_side(&mut self) {
        self.side = match self.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum UserTab {
    Positions,
    Trades,
}

#[derive(Clone)]
struct ActivityEntry {
    tx_hash: Option<String>,
    title: String,
    timestamp: i64,
    slug: Option<String>,
    asset_id: Option<String>,
    outcome_index: Option<usize>,
    kind: ActivityKind,
}

#[derive(Clone)]
struct TokenMarketInfo {
    question: String,
    outcome: String,
    slug: Option<String>,
}

#[derive(Clone)]
struct RecentTrade {
    asset_id: String,
    side: Side,
    price: f64,
    size: f64,
    timestamp_secs: i64,
}

enum RecordingCommand {
    OrderBookSnapshot {
        asset_id: String,
        hashes: Vec<String>,
        ws_timestamp: i64,
        local_timestamp: i64,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>,
    },
    Trade {
        asset_id: String,
        outcome: String,
        size: f64,
        price: f64,
        side: Side,
        hash: Option<String>,
        ws_timestamp: i64,
        local_timestamp: i64,
    },
    Stop {
        reply: oneshot::Sender<RecordingSummary>,
    },
}

#[derive(Default)]
struct RecordingSummary {
    ws_start: Option<i64>,
    ws_end: Option<i64>,
    local_start: Option<i64>,
    local_end: Option<i64>,
    rows_written: u64,
}

struct RecordingState {
    sender: std_mpsc::Sender<RecordingCommand>,
    tmp_path: PathBuf,
    event_title: String,
    slug: Option<String>,
    start_local_ms: i64,
    start_ws_ms: Option<i64>,
    asset_ids: Vec<String>,
    join_handle: Option<thread::JoinHandle<()>>,
}

struct RecordingStopHandle {
    tmp_path: PathBuf,
    event_title: String,
    slug: Option<String>,
    start_local_ms: i64,
    join_handle: Option<thread::JoinHandle<()>>,
}

fn spawn_recording_worker(
    path: PathBuf,
) -> anyhow::Result<(std_mpsc::Sender<RecordingCommand>, thread::JoinHandle<()>)> {
    let (tx, rx) = std_mpsc::channel::<RecordingCommand>();
    let handle = thread::spawn(move || {
        let conn_result = Connection::open(&path);
        if let Err(err) = conn_result {
            tracing::error!(target: "recording", ?err, "failed to open recording database");
            drain_until_stop(rx, RecordingSummary::default());
            return;
        }
        let mut conn = conn_result.unwrap();
        if let Err(err) = init_recording_db(&mut conn) {
            tracing::error!(target: "recording", ?err, "failed to init recording database");
            drain_until_stop(rx, RecordingSummary::default());
            return;
        }
        let mut summary = RecordingSummary::default();
        while let Ok(cmd) = rx.recv() {
            match cmd {
                RecordingCommand::OrderBookSnapshot {
                    asset_id,
                    hashes,
                    ws_timestamp,
                    local_timestamp,
                    bids,
                    asks,
                } => {
                    if let Err(err) = insert_orderbook_snapshot(
                        &mut conn,
                        &asset_id,
                        &hashes,
                        ws_timestamp,
                        local_timestamp,
                        &bids,
                        &asks,
                    ) {
                        tracing::warn!(target: "recording", ?err, "failed to persist orderbook snapshot");
                    } else {
                        update_summary_times(&mut summary, ws_timestamp, local_timestamp);
                        summary.rows_written += 1;
                    }
                }
                RecordingCommand::Trade {
                    asset_id,
                    outcome,
                    size,
                    price,
                    side,
                    hash,
                    ws_timestamp,
                    local_timestamp,
                } => {
                    if let Err(err) = insert_trade(
                        &mut conn,
                        &asset_id,
                        &outcome,
                        size,
                        price,
                        side,
                        hash.as_deref(),
                        ws_timestamp,
                        local_timestamp,
                    ) {
                        tracing::warn!(target: "recording", ?err, "failed to persist trade" );
                    } else {
                        update_summary_times(&mut summary, ws_timestamp, local_timestamp);
                        summary.rows_written += 1;
                    }
                }
                RecordingCommand::Stop { reply } => {
                    let _ = reply.send(summary);
                    break;
                }
            }
        }
    });
    Ok((tx, handle))
}

fn drain_until_stop(mut rx: std_mpsc::Receiver<RecordingCommand>, summary: RecordingSummary) {
    while let Ok(cmd) = rx.recv() {
        if let RecordingCommand::Stop { reply } = cmd {
            let _ = reply.send(summary);
            break;
        }
    }
}

fn init_recording_db(conn: &mut Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        r#"
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        CREATE TABLE IF NOT EXISTS orderbook_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id TEXT NOT NULL,
            hashes TEXT NOT NULL,
            ws_timestamp INTEGER,
            local_timestamp INTEGER NOT NULL,
            bids TEXT NOT NULL,
            asks TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id TEXT NOT NULL,
            outcome TEXT,
            size REAL,
            price REAL,
            side TEXT,
            hash TEXT,
            ws_timestamp INTEGER,
            local_timestamp INTEGER NOT NULL
        );
        "#,
    )
}

fn insert_orderbook_snapshot(
    conn: &mut Connection,
    asset_id: &str,
    hashes: &[String],
    ws_timestamp: i64,
    local_timestamp: i64,
    bids: &[(f64, f64)],
    asks: &[(f64, f64)],
) -> rusqlite::Result<()> {
    let hashes_json = serde_json::to_string(hashes).unwrap_or_else(|_| "[]".to_string());
    let bids_json = serde_json::to_string(bids).unwrap_or_else(|_| "[]".to_string());
    let asks_json = serde_json::to_string(asks).unwrap_or_else(|_| "[]".to_string());
    conn.execute(
        "INSERT INTO orderbook_snapshots (asset_id, hashes, ws_timestamp, local_timestamp, bids, asks) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            asset_id,
            hashes_json,
            ws_timestamp,
            local_timestamp,
            bids_json,
            asks_json
        ],
    )?;
    Ok(())
}

fn insert_trade(
    conn: &mut Connection,
    asset_id: &str,
    outcome: &str,
    size: f64,
    price: f64,
    side: Side,
    hash: Option<&str>,
    ws_timestamp: i64,
    local_timestamp: i64,
) -> rusqlite::Result<()> {
    let side_label = match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    };
    conn.execute(
        "INSERT INTO trades (asset_id, outcome, size, price, side, hash, ws_timestamp, local_timestamp) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            asset_id,
            outcome,
            size,
            price,
            side_label,
            hash,
            ws_timestamp,
            local_timestamp
        ],
    )?;
    Ok(())
}

fn update_summary_times(summary: &mut RecordingSummary, ws_timestamp: i64, local_timestamp: i64) {
    summary.local_start = Some(match summary.local_start {
        Some(existing) => existing.min(local_timestamp),
        None => local_timestamp,
    });
    summary.local_end = Some(match summary.local_end {
        Some(existing) => existing.max(local_timestamp),
        None => local_timestamp,
    });
    if ws_timestamp > 0 {
        summary.ws_start = Some(match summary.ws_start {
            Some(existing) => existing.min(ws_timestamp),
            None => ws_timestamp,
        });
        summary.ws_end = Some(match summary.ws_end {
            Some(existing) => existing.max(ws_timestamp),
            None => ws_timestamp,
        });
    }
}

#[derive(Clone)]
enum ActivityKind {
    Trade {
        side: Side,
        size: f64,
        usdc_size: f64,
        price: f64,
    },
    Conversion {
        size: f64,
        usdc_size: f64,
    },
    Merge {
        size: f64,
        usdc_size: f64,
    },
    Split {
        size: f64,
        usdc_size: f64,
    },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ActivityRow {
    #[serde(rename = "type")]
    type_field: String,
    timestamp: i64,
    #[serde(default)]
    size: Option<f64>,
    #[serde(default)]
    #[serde(rename = "usdcSize")]
    usdc_size: Option<f64>,
    #[serde(default)]
    price: Option<f64>,
    #[serde(default)]
    side: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    #[serde(rename = "transactionHash")]
    transaction_hash: Option<String>,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    #[serde(rename = "eventSlug")]
    event_slug: Option<String>,
    #[serde(default)]
    asset: Option<String>,
    #[serde(default)]
    #[serde(rename = "outcomeIndex")]
    outcome_index: Option<i64>,
}

#[derive(Clone, Debug)]
struct TokenMarketEntry {
    token_id: String,
    question: String,
    outcome: String,
    slug: Option<String>,
}

enum MarketInfoUpdate {
    Loaded(Vec<TokenMarketEntry>),
    Failed(Vec<String>),
}

impl ActivityEntry {
    fn ty_label(&self) -> &'static str {
        match self.kind {
            ActivityKind::Trade { .. } => "TRADE",
            ActivityKind::Conversion { .. } => "CONVERSION",
            ActivityKind::Merge { .. } => "MERGE",
            ActivityKind::Split { .. } => "SPLIT",
        }
    }

    fn side_label(&self) -> String {
        match self.kind {
            ActivityKind::Trade { side, .. } => match side {
                Side::Buy => "BUY".into(),
                Side::Sell => "SELL".into(),
            },
            _ => String::new(),
        }
    }

    fn size(&self) -> f64 {
        match self.kind {
            ActivityKind::Trade { size, .. }
            | ActivityKind::Conversion { size, .. }
            | ActivityKind::Merge { size, .. }
            | ActivityKind::Split { size, .. } => size,
        }
    }

    fn usdc_size(&self) -> f64 {
        match self.kind {
            ActivityKind::Trade { usdc_size, .. }
            | ActivityKind::Conversion { usdc_size, .. }
            | ActivityKind::Merge { usdc_size, .. }
            | ActivityKind::Split { usdc_size, .. } => usdc_size,
        }
    }

    fn price(&self) -> Option<f64> {
        match self.kind {
            ActivityKind::Trade { price, .. } => Some(price),
            _ => None,
        }
    }

    fn from_row(row: ActivityRow) -> Option<Self> {
        let ty = row.type_field.to_uppercase();
        let title = row.title.unwrap_or_default();
        if title.is_empty() {
            return None;
        }
        let size = row.size.unwrap_or(0.0);
        let usdc_size = row.usdc_size.unwrap_or(0.0);
        let ts = row.timestamp;
        let tx_hash = row.transaction_hash;

        let slug = row
            .slug
            .or(row.event_slug)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let asset_id = row
            .asset
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let outcome_index =
            row.outcome_index
                .and_then(|idx| if idx >= 0 { Some(idx as usize) } else { None });

        let kind = match ty.as_str() {
            "TRADE" => {
                let side = row.side.as_ref()?.to_uppercase();
                let side = match side.as_str() {
                    "BUY" => Side::Buy,
                    "SELL" => Side::Sell,
                    _ => return None,
                };
                let price = row.price.unwrap_or(0.0);
                ActivityKind::Trade {
                    side,
                    size,
                    usdc_size,
                    price,
                }
            }
            "CONVERSION" => ActivityKind::Conversion { size, usdc_size },
            "MERGE" => ActivityKind::Merge { size, usdc_size },
            "SPLIT" => ActivityKind::Split { size, usdc_size },
            _ => return None,
        };

        Some(Self {
            tx_hash,
            title,
            timestamp: ts,
            slug,
            asset_id,
            outcome_index,
            kind,
        })
    }

    #[allow(dead_code)]
    fn from_event(evt: &ActivityEvent) -> Self {
        let kind = ActivityKind::Trade {
            side: evt.side,
            size: evt.size,
            usdc_size: evt.usdc_size.unwrap_or(0.0),
            price: evt.price,
        };
        Self {
            tx_hash: if evt.transaction_hash.is_empty() {
                None
            } else {
                Some(evt.transaction_hash.clone())
            },
            title: if evt.title.is_empty() {
                evt.slug.clone()
            } else {
                evt.title.clone()
            },
            timestamp: evt.timestamp,
            slug: if evt.slug.is_empty() {
                None
            } else {
                Some(evt.slug.clone())
            },
            asset_id: if evt.asset.is_empty() {
                None
            } else {
                Some(evt.asset.clone())
            },
            outcome_index: evt.outcome_index,
            kind,
        }
    }
}

#[derive(Clone)]
struct UserInfo {
    username: String,
    proxy_wallet: String,
    private_key: String,
    chain_id: u64,
    polygon_rpc_url: String,
}

struct AppState {
    focus: Focus,
    view_mode: ViewMode,
    // left: search
    search_input: String,
    results: Vec<EventItem>,
    sel_event_idx: usize,
    last_error: Option<String>,

    // middle: selected event detail and markets
    cache: HashMap<String, EventDetail>,
    loading_detail: bool,
    sel_market_idx: usize,
    sel_outcome_idx: usize, // 0 or 1

    // right: books (token_id -> orderbook)
    books: HashMap<String, OrderBookState>,

    trade: TradeForm,
    orders: Vec<OrderLogEntry>,
    orders_map: HashMap<String, OpenOrder>,
    sel_order_idx: usize,
    orders_rows_per_page: usize,
    orders_page: usize,
    // positions
    base_positions: HashMap<String, f64>,
    delta_positions: HashMap<String, f64>,
    order_matched_seen: HashMap<String, f64>,
    positions_initialized: bool,
    cash_usdce: f64,
    positions_value_sum: f64,
    positions: Vec<PositionInfo>,
    positions_sel: usize,
    positions_page: usize,
    positions_rows_per_page: usize,
    pending_slash: bool,
    orders_view_sel: usize,
    user_profile: Option<UserInfo>,
    user_status: Option<String>,
    user_tab: UserTab,
    trade_activities: Vec<ActivityEntry>,
    trades_sel: usize,
    trades_page: usize,
    trades_rows_per_page: usize,
    token_market_cache: HashMap<String, TokenMarketInfo>,
    token_market_pending: HashSet<String>,
    ticker_items: VecDeque<String>,
    ticker_recent_keys: VecDeque<String>,
    ticker_seen: HashSet<String>,
    ticker_chars: Vec<char>,
    ticker_scroll: usize,
    ticker_dirty: bool,
    recent_trades: HashMap<String, VecDeque<RecentTrade>>,
    recent_trades_offset: usize,
    recent_trades_visible_rows: usize,
    recent_trades_last_len: usize,
    recent_trades_active_tokens: Vec<String>,
    recent_trades_scroll_tick: u8,
    recording: Option<RecordingState>,
    recording_message: Option<String>,
}

impl AppState {
    fn new(user: Option<UserInfo>) -> Self {
        Self {
            focus: Focus::Search,
            view_mode: ViewMode::Main,
            search_input: String::new(),
            results: Vec::new(),
            sel_event_idx: 0,
            last_error: None,
            cache: HashMap::new(),
            loading_detail: false,
            sel_market_idx: 0,
            sel_outcome_idx: 0,
            books: HashMap::new(),
            trade: TradeForm::new(),
            orders: Vec::new(),
            orders_map: HashMap::new(),
            sel_order_idx: 0,
            orders_rows_per_page: 1,
            orders_page: 0,
            base_positions: HashMap::new(),
            delta_positions: HashMap::new(),
            order_matched_seen: HashMap::new(),
            positions_initialized: false,
            cash_usdce: 0.0,
            positions_value_sum: 0.0,
            positions: Vec::new(),
            positions_sel: 0,
            positions_page: 0,
            positions_rows_per_page: 1,
            pending_slash: false,
            orders_view_sel: 0,
            user_profile: user,
            user_status: None,
            user_tab: UserTab::Positions,
            trade_activities: Vec::new(),
            trades_sel: 0,
            trades_page: 0,
            trades_rows_per_page: 1,
            token_market_cache: HashMap::new(),
            token_market_pending: HashSet::new(),
            ticker_items: VecDeque::new(),
            ticker_recent_keys: VecDeque::new(),
            ticker_seen: HashSet::new(),
            ticker_chars: Vec::new(),
            ticker_scroll: 0,
            ticker_dirty: true,
            recent_trades: HashMap::new(),
            recent_trades_offset: 0,
            recent_trades_visible_rows: 0,
            recent_trades_last_len: 0,
            recent_trades_active_tokens: Vec::new(),
            recent_trades_scroll_tick: 0,
            recording: None,
            recording_message: None,
        }
    }
    fn current_event_slug(&self) -> Option<String> {
        self.results.get(self.sel_event_idx).map(|e| e.slug.clone())
    }
    fn current_event(&self) -> Option<&EventDetail> {
        self.current_event_slug().and_then(|s| self.cache.get(&s))
    }
    fn selected_market_and_token(&self) -> Option<(&search::detail::EventMarket, Option<String>)> {
        let ev = self.current_event()?;
        let m = ev.markets.get(self.sel_market_idx)?;
        let token = m.clobTokenIds.get(self.sel_outcome_idx).cloned();
        Some((m, token))
    }

    fn selected_market_title(&self) -> Option<String> {
        let ev = self.current_event()?;
        let m = ev.markets.get(self.sel_market_idx)?;
        if m.question.is_empty() {
            Some(m.slug.clone())
        } else {
            Some(m.question.clone())
        }
    }

    fn selected_outcome_name(&self) -> Option<String> {
        let ev = self.current_event()?;
        let m = ev.markets.get(self.sel_market_idx)?;
        m.outcomes.get(self.sel_outcome_idx).cloned()
    }

    fn current_market_tokens(&self) -> Vec<String> {
        self.current_event()
            .and_then(|ev| ev.markets.get(self.sel_market_idx))
            .map(|m| m.clobTokenIds.clone())
            .unwrap_or_default()
    }

    fn record_public_trade(&mut self, trade: &PublicTrade) {
        let ts = trade.timestamp;
        let ts_secs = if ts > 1_000_000_000_000 {
            ts / 1000
        } else {
            ts
        };
        let entry = RecentTrade {
            asset_id: trade.asset_id.clone(),
            side: trade.side,
            price: trade.price,
            size: trade.size,
            timestamp_secs: ts_secs,
        };
        let deque = self
            .recent_trades
            .entry(trade.asset_id.clone())
            .or_insert_with(VecDeque::new);
        deque.push_front(entry);
        while deque.len() > RECENT_TRADES_PER_TOKEN {
            deque.pop_back();
        }
        if self
            .recent_trades_active_tokens
            .iter()
            .any(|token| token == &trade.asset_id)
        {
            self.recent_trades_offset = 0;
            self.recent_trades_scroll_tick = 0;
        }
    }

    fn gather_recent_trades(&self, tokens: &[String]) -> Vec<RecentTrade> {
        let mut trades: Vec<RecentTrade> = Vec::new();
        for token in tokens {
            if let Some(entries) = self.recent_trades.get(token) {
                trades.extend(entries.iter().cloned());
            }
        }
        trades.sort_by(|a, b| b.timestamp_secs.cmp(&a.timestamp_secs));
        trades
    }

    fn advance_recent_trades(&mut self) {
        if self.recent_trades_visible_rows == 0
            || self.recent_trades_last_len <= self.recent_trades_visible_rows
        {
            self.recent_trades_offset = 0;
            self.recent_trades_scroll_tick = 0;
            return;
        }
        self.recent_trades_scroll_tick = self.recent_trades_scroll_tick.saturating_add(1);
        if self.recent_trades_scroll_tick < RECENT_TRADES_SCROLL_INTERVAL {
            return;
        }
        self.recent_trades_scroll_tick = 0;
        if self.recent_trades_last_len > 0 {
            self.recent_trades_offset =
                (self.recent_trades_offset + 1) % self.recent_trades_last_len.max(1);
        }
    }

    fn recording_assets(&self) -> Option<&[String]> {
        self.recording.as_ref().map(|rec| rec.asset_ids.as_slice())
    }

    fn recording_includes(&self, asset_id: &str) -> bool {
        self.recording_assets()
            .map(|tokens| tokens.iter().any(|t| t == asset_id))
            .unwrap_or(false)
    }

    fn start_recording_session(&mut self) -> Result<()> {
        if self.recording.is_some() {
            self.recording_message = Some("Recording already in progress".into());
            return Ok(());
        }
        let tokens = self.current_market_tokens();
        if tokens.is_empty() {
            self.recording_message = Some("Select a market before recording".into());
            return Ok(());
        }

        let event_title = self
            .current_event()
            .map(|d| d.title.clone())
            .filter(|s| !s.is_empty())
            .or_else(|| self.selected_market_title())
            .unwrap_or_else(|| "Untitled market".to_string());
        let event_slug = self.current_event().and_then(|d| {
            if d.slug.is_empty() {
                None
            } else {
                Some(d.slug.clone())
            }
        });

        fs::create_dir_all(RECORDINGS_DIR)?;
        let identifier = event_slug
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(sanitize_filename)
            .unwrap_or_else(|| sanitize_filename(&event_title));
        let start_local_ms = now_ts_ms();
        let tmp_name = format!("{}_{}.tmp", identifier, start_local_ms);
        let tmp_path = Path::new(RECORDINGS_DIR).join(tmp_name);

        let (sender, join_handle) = spawn_recording_worker(tmp_path.clone())?;

        self.recording = Some(RecordingState {
            sender,
            tmp_path,
            event_title: event_title.clone(),
            slug: event_slug,
            start_local_ms,
            start_ws_ms: None,
            asset_ids: tokens.clone(),
            join_handle: Some(join_handle),
        });

        self.recording_message = Some(format!("Recording {} (press r to stop)", event_title));

        let mut initial_hash = Vec::new();
        initial_hash.push("initial".to_string());
        for token in tokens {
            if let Some(book) = self.books.get(&token) {
                self.record_orderbook_snapshot(
                    &token,
                    initial_hash.clone(),
                    0,
                    book.bids.levels.clone(),
                    book.asks.levels.clone(),
                );
            }
        }

        Ok(())
    }

    fn prepare_stop_recording(
        &mut self,
    ) -> Result<Option<(oneshot::Receiver<RecordingSummary>, RecordingStopHandle)>> {
        let Some(mut state) = self.recording.take() else {
            self.recording_message = Some("No active recording".into());
            return Ok(None);
        };
        let (tx, rx) = oneshot::channel();
        if let Err(err) = state.sender.send(RecordingCommand::Stop { reply: tx }) {
            self.recording_message = Some("Failed to stop recording".into());
            tracing::warn!(target: "recording", ?err, "failed to send stop command");
            return Err(anyhow!("failed to stop recording"));
        }
        let handle = RecordingStopHandle {
            tmp_path: state.tmp_path.clone(),
            event_title: state.event_title.clone(),
            slug: state.slug.clone(),
            start_local_ms: state.start_local_ms,
            join_handle: state.join_handle.take(),
        };
        self.recording_message = Some("Stopping recording...".into());
        Ok(Some((rx, handle)))
    }

    fn finalize_recording(
        &mut self,
        mut handle: RecordingStopHandle,
        summary: RecordingSummary,
    ) -> Result<()> {
        if let Some(join) = handle.join_handle.take() {
            let _ = join.join();
        }

        let start_local = summary.local_start.unwrap_or(handle.start_local_ms);
        let end_local = summary.local_end.unwrap_or_else(|| now_ts_ms());
        let identifier = handle
            .slug
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(sanitize_filename)
            .unwrap_or_else(|| sanitize_filename(&handle.event_title));
        let start_seg = format_ms_for_filename(start_local);
        let end_seg = format_ms_for_filename(end_local);
        let final_name = format!(
            "{}_{}-{}.{}",
            identifier, start_seg, end_seg, RECORDING_DB_EXTENSION
        );
        let final_path = Path::new(RECORDINGS_DIR).join(final_name);
        if let Err(err) = fs::rename(&handle.tmp_path, &final_path) {
            tracing::warn!(target: "recording", ?err, "failed to rename recording file");
            self.recording_message = Some(format!(
                "Recording saved (rename failed). File: {}",
                handle.tmp_path.display()
            ));
            return Ok(());
        }
        self.recording_message = Some(format!(
            "Recording saved: {} ({} rows)",
            final_path.display(),
            summary.rows_written
        ));
        Ok(())
    }

    fn record_orderbook_snapshot(
        &mut self,
        asset_id: &str,
        hashes: Vec<String>,
        ws_timestamp: i64,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>,
    ) {
        let Some(rec) = self.recording.as_mut() else {
            return;
        };
        if !rec.asset_ids.iter().any(|t| t == asset_id) {
            return;
        }
        if rec.start_ws_ms.is_none() && ws_timestamp > 0 {
            rec.start_ws_ms = Some(ws_timestamp);
        }
        let local_ts = now_ts_ms();
        if let Err(err) = rec.sender.send(RecordingCommand::OrderBookSnapshot {
            asset_id: asset_id.to_string(),
            hashes,
            ws_timestamp,
            local_timestamp: local_ts,
            bids,
            asks,
        }) {
            tracing::warn!(target: "recording", ?err, "orderbook snapshot send failed");
            self.recording_message = Some("Recording aborted due to writer error".into());
            self.recording = None;
        }
    }

    fn record_trade_event(&mut self, trade: &PublicTrade, outcome: String) {
        let Some(rec) = self.recording.as_mut() else {
            return;
        };
        if !rec.asset_ids.iter().any(|t| t == &trade.asset_id) {
            return;
        }
        if rec.start_ws_ms.is_none() && trade.timestamp > 0 {
            rec.start_ws_ms = Some(trade.timestamp);
        }
        let local_ts = now_ts_ms();
        if let Err(err) = rec.sender.send(RecordingCommand::Trade {
            asset_id: trade.asset_id.clone(),
            outcome,
            size: trade.size,
            price: trade.price,
            side: trade.side,
            hash: trade.hash.clone(),
            ws_timestamp: trade.timestamp,
            local_timestamp: local_ts,
        }) {
            tracing::warn!(target: "recording", ?err, "trade send failed");
            self.recording_message = Some("Recording aborted due to writer error".into());
            self.recording = None;
        }
    }

    fn update_positions_rows_per_page(&mut self, rows: usize) {
        let rows = rows.max(1);
        if self.positions_rows_per_page != rows {
            self.positions_rows_per_page = rows;
            self.clamp_positions_state();
        }
    }

    fn clamp_positions_state(&mut self) {
        let rows_per_page = self.positions_rows_per_page.max(1);
        if self.positions.is_empty() {
            self.positions_page = 0;
            self.positions_sel = 0;
            return;
        }

        let total_pages = (self.positions.len() + rows_per_page - 1) / rows_per_page;
        if total_pages == 0 {
            self.positions_page = 0;
        } else {
            self.positions_page = self.positions_page.min(total_pages - 1);
        }

        let start = self.positions_page * rows_per_page;
        let end = (start + rows_per_page).min(self.positions.len());
        if self.positions_sel >= self.positions.len() {
            self.positions_sel = self.positions.len() - 1;
        }
        if self.positions_sel < start || self.positions_sel >= end {
            self.positions_sel = start;
        }
    }

    fn current_positions_slice(&self) -> (usize, usize) {
        if self.positions.is_empty() {
            return (0, 0);
        }
        let rows_per_page = self.positions_rows_per_page.max(1);
        let start = self.positions_page * rows_per_page;
        let start = start.min(self.positions.len().saturating_sub(1));
        let end = (start + rows_per_page).min(self.positions.len());
        (start, end)
    }

    fn update_trades_rows_per_page(&mut self, rows: usize) {
        let rows = rows.max(1);
        if self.trades_rows_per_page != rows {
            self.trades_rows_per_page = rows;
            self.clamp_trades_state();
        }
    }

    fn clamp_trades_state(&mut self) {
        let rows_per_page = self.trades_rows_per_page.max(1);
        if self.trade_activities.is_empty() {
            self.trades_page = 0;
            self.trades_sel = 0;
            return;
        }

        let total_pages = (self.trade_activities.len() + rows_per_page - 1) / rows_per_page;
        if total_pages == 0 {
            self.trades_page = 0;
        } else {
            self.trades_page = self.trades_page.min(total_pages - 1);
        }

        let start = self.trades_page * rows_per_page;
        let end = (start + rows_per_page).min(self.trade_activities.len());
        if self.trades_sel >= self.trade_activities.len() {
            self.trades_sel = self.trade_activities.len() - 1;
        }
        if self.trades_sel < start || self.trades_sel >= end {
            self.trades_sel = start;
        }
    }

    fn current_trades_slice(&self) -> (usize, usize) {
        if self.trade_activities.is_empty() {
            return (0, 0);
        }
        let rows_per_page = self.trades_rows_per_page.max(1);
        let start = self.trades_page * rows_per_page;
        let start = start.min(self.trade_activities.len().saturating_sub(1));
        let end = (start + rows_per_page).min(self.trade_activities.len());
        (start, end)
    }

    fn positions_total_pages(&self) -> usize {
        if self.positions.is_empty() {
            0
        } else {
            let rows = self.positions_rows_per_page.max(1);
            (self.positions.len() + rows - 1) / rows
        }
    }

    fn trades_total_pages(&self) -> usize {
        if self.trade_activities.is_empty() {
            0
        } else {
            let rows = self.trades_rows_per_page.max(1);
            (self.trade_activities.len() + rows - 1) / rows
        }
    }

    fn orders_total_pages(&self, total: usize) -> usize {
        if total == 0 {
            0
        } else {
            let rows = self.orders_rows_per_page.max(1);
            (total + rows - 1) / rows
        }
    }

    fn update_orders_rows_per_page(&mut self, rows: usize, total: usize) {
        let rows = rows.max(1);
        if self.orders_rows_per_page != rows {
            self.orders_rows_per_page = rows;
            self.clamp_orders_state(total);
        }
    }

    fn clamp_orders_state(&mut self, total: usize) {
        if total == 0 {
            self.orders_page = 0;
            self.orders_view_sel = 0;
            return;
        }
        let rows = self.orders_rows_per_page.max(1);
        let total_pages = (total + rows - 1) / rows;
        if total_pages == 0 {
            self.orders_page = 0;
        } else {
            self.orders_page = self.orders_page.min(total_pages - 1);
        }
        let start = self.orders_page * rows;
        let end = (start + rows).min(total);
        if self.orders_view_sel >= total {
            self.orders_view_sel = total - 1;
        }
        if self.orders_view_sel < start || self.orders_view_sel >= end {
            self.orders_view_sel = start;
        }
    }

    fn current_orders_slice(&self, total: usize) -> (usize, usize) {
        if total == 0 {
            return (0, 0);
        }
        let rows = self.orders_rows_per_page.max(1);
        let start = self.orders_page * rows;
        let start = start.min(total.saturating_sub(1));
        let end = (start + rows).min(total);
        (start, end)
    }

    fn handle_live_activity(&mut self, evt: &ActivityEvent) {
        self.push_ticker_event(evt);
    }

    fn push_ticker_event(&mut self, evt: &ActivityEvent) {
        let key = if !evt.transaction_hash.is_empty() {
            evt.transaction_hash.clone()
        } else {
            format!("{}:{}:{}", evt.timestamp, evt.proxy_wallet, evt.asset)
        };
        if self.ticker_seen.contains(&key) {
            return;
        }
        self.ticker_seen.insert(key.clone());
        self.ticker_recent_keys.push_back(key);
        if self.ticker_recent_keys.len() > TICKER_MAX_KEYS {
            if let Some(old) = self.ticker_recent_keys.pop_front() {
                self.ticker_seen.remove(&old);
            }
        }

        let side_str = match evt.side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        };
        let title_ref = if evt.title.is_empty() {
            evt.slug.as_str()
        } else {
            evt.title.as_str()
        };
        let mut item = format!("{} {}", evt.name, side_str);
        if !evt.outcome.is_empty() {
            item.push(' ');
            item.push_str(evt.outcome.as_str());
        }
        item.push_str(" @ ");
        item.push_str(&trim_to_width(title_ref, 40));

        self.ticker_items.push_back(item);
        if self.ticker_items.len() > TICKER_MAX_ITEMS {
            self.ticker_items.pop_front();
        }
        self.ticker_dirty = true;
    }

    fn rebuild_ticker_cache(&mut self) {
        if !self.ticker_dirty {
            return;
        }
        if self.ticker_items.is_empty() {
            self.ticker_chars.clear();
            self.ticker_scroll = 0;
            self.ticker_dirty = false;
            return;
        }

        let mut base = String::new();
        for (idx, item) in self.ticker_items.iter().enumerate() {
            if idx > 0 {
                base.push_str(TICKER_SEPARATOR);
            }
            base.push_str(item);
        }

        let mut extended = base.clone();
        extended.push_str(TICKER_SEPARATOR);
        extended.push_str(&base);

        self.ticker_chars = extended.chars().collect();
        if self.ticker_chars.is_empty() {
            self.ticker_chars = base.chars().collect();
        }

        if !self.ticker_chars.is_empty() {
            self.ticker_scroll %= self.ticker_chars.len();
        } else {
            self.ticker_scroll = 0;
        }
        self.ticker_dirty = false;
    }

    fn ticker_display(&mut self, width: usize) -> String {
        if width == 0 {
            return String::new();
        }
        self.rebuild_ticker_cache();
        if self.ticker_chars.is_empty() {
            return "No activity yet".to_string();
        }
        let len = self.ticker_chars.len();
        let mut out = String::with_capacity(width);
        for i in 0..width {
            let idx = (self.ticker_scroll + i) % len;
            out.push(self.ticker_chars[idx]);
        }
        out
    }

    fn advance_ticker(&mut self) {
        self.rebuild_ticker_cache();
        if self.ticker_chars.is_empty() {
            return;
        }
        self.ticker_scroll = (self.ticker_scroll + 1) % self.ticker_chars.len();
    }
}

#[derive(Clone)]
#[allow(dead_code)]
struct OrderLogEntry {
    token_id: String,
    side: Side,
    price: f64,
    size: f64,
    success: bool,
    order_id: Option<String>,
    error_message: Option<String>,
}

#[derive(Clone, Copy)]
struct TokenMeta {
    tick_size: f64,
    neg_risk: bool,
    min_order_size: Option<f64>,
}

struct TradingContext {
    cfg: EngineConfig,
    exchange: Arc<PolymarketHttpExchange>,
    api_creds: Arc<ApiCreds>,
    meta_cache: RwLock<HashMap<String, TokenMeta>>,
    wallet_addr: Option<String>,
    rpc_endpoints: Vec<String>,
    _chain_id: u64,
}

impl TradingContext {
    async fn new(cfg: EngineConfig, http_pool: Arc<HttpPool>, user: &UserInfo) -> Result<Self> {
        let signer = parse_pk(&user.private_key)?;
        let api_creds = Arc::new(create_or_derive_api_creds(&http_pool, &signer, None).await?);

        let chain_id = user.chain_id;
        let maker_override = std::env::var("SAFE_ADDRESS")
            .ok()
            .or_else(|| std::env::var("BROWSER_ADDRESS").ok())
            .or_else(|| Some(user.proxy_wallet.clone()));

        let exchange = Arc::new(PolymarketHttpExchange::new(
            http_pool.clone(),
            signer.clone(),
            (*api_creds).clone(),
            chain_id,
            maker_override.clone(),
        ));

        let wallet_addr = maker_override.or_else(|| {
            use alloy_primitives::hex::encode_prefixed;
            Some(encode_prefixed(signer.address().as_slice()))
        });

        let mut rpc_endpoints: Vec<String> = Vec::new();
        if !user.polygon_rpc_url.trim().is_empty() {
            rpc_endpoints.push(user.polygon_rpc_url.clone());
        }
        if let Ok(env_url) = std::env::var("POLYGON_RPC_URL") {
            let trimmed = env_url.trim();
            if !trimmed.is_empty() && !rpc_endpoints.iter().any(|u| u == trimmed) {
                rpc_endpoints.push(trimmed.to_string());
            }
        }
        for fallback in [
            "https://polygon-rpc.com",
            "https://rpc.ankr.com/polygon",
            "https://1rpc.io/matic",
            "https://rpc-mainnet.maticvigil.com",
        ] {
            if !rpc_endpoints.iter().any(|u| u == fallback) {
                rpc_endpoints.push(fallback.to_string());
            }
        }

        Ok(Self {
            cfg,
            exchange,
            api_creds,
            meta_cache: RwLock::new(HashMap::new()),
            wallet_addr,
            rpc_endpoints,
            _chain_id: chain_id,
        })
    }

    fn api_creds(&self) -> ApiCreds {
        self.api_creds.as_ref().clone()
    }

    fn user_ws_url(&self) -> &str {
        &self.cfg.ws.user_ws_url
    }

    async fn submit_limit_order(
        &self,
        token_id: &str,
        price: f64,
        size: f64,
        side: Side,
    ) -> Result<(OrderAck, f64, f64)> {
        let meta = self.ensure_meta(token_id).await?;

        if price <= 0.0 || price >= 1.0 {
            return Err(anyhow!("price must be between 0 and 1"));
        }

        let mut px = price;
        if meta.tick_size > 0.0 {
            let steps = (px / meta.tick_size).round();
            px = (steps * meta.tick_size).max(meta.tick_size);
            if px >= 1.0 {
                px = (1.0 - meta.tick_size).max(meta.tick_size);
            }
        }

        let sz = (size * 100.0).floor() / 100.0;
        if sz <= 0.0 {
            return Err(anyhow!("size must be positive"));
        }
        if let Some(min) = meta.min_order_size {
            if sz + 1e-9 < min {
                return Err(anyhow!("size {:.2} below minimum {:.2}", sz, min));
            }
        }

        let args = OrderArgs {
            token_id: token_id.to_string(),
            price: px,
            size: sz,
            side,
        };
        let opts = CreateOrderOptions {
            tick_size: Some(meta.tick_size),
            neg_risk: Some(meta.neg_risk),
        };
        let acks = self
            .exchange
            .create_orders_batch(vec![(args, OrderType::Gtc, opts)])
            .await?;
        let ack = acks.into_iter().next().unwrap_or(OrderAck {
            success: false,
            error_message: Some("empty ack".into()),
            order_id: None,
        });
        Ok((ack, px, sz))
    }

    async fn ensure_meta(&self, token_id: &str) -> Result<TokenMeta> {
        if let Some(meta) = self.meta_cache.read().await.get(token_id).copied() {
            return Ok(meta);
        }

        self.fetch_books_snapshot(&[token_id.to_string()]).await?;
        self.meta_cache
            .read()
            .await
            .get(token_id)
            .copied()
            .ok_or_else(|| anyhow!("token meta missing after fetch"))
    }

    async fn fetch_books_snapshot(&self, tokens: &[String]) -> Result<Vec<BookSnapshot>> {
        if tokens.is_empty() {
            return Ok(Vec::new());
        }
        let snapshots = self.exchange.fetch_books(tokens).await?;
        if !snapshots.is_empty() {
            let mut cache = self.meta_cache.write().await;
            for snap in snapshots.iter() {
                let asset_id = snap.order_book.asset_id.clone();
                let meta = TokenMeta {
                    tick_size: snap.tick_size,
                    neg_risk: snap.neg_risk.unwrap_or(true),
                    min_order_size: snap.min_order_size,
                };
                cache.insert(asset_id, meta);
            }
        }
        Ok(snapshots)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let setup = onboarding::run().await.map_err(|err| anyhow!(err))?;
    let user_info = UserInfo {
        username: setup.username.clone(),
        proxy_wallet: setup.proxy_wallet.clone(),
        private_key: setup.private_key.clone(),
        chain_id: setup.chain_id,
        polygon_rpc_url: setup.polygon_rpc_url.clone(),
    };

    let cfg = EngineConfig::default();
    let registry = Arc::new(Registry::new());
    let http_pool = Arc::new(HttpPool::new(&cfg, registry.as_ref())?);

    // market WS
    let market = MarketMultiWs::new(&cfg.ws.market_ws_url, cfg.ws.max_assets_per_conn)?;

    // channel for market events
    let (tx, mut rx) = mpsc::channel::<MarketEvent>(2000);
    let market_clone = market.clone();
    tokio::spawn(async move {
        loop {
            match market_clone.next().await {
                Ok(Some(ev)) => {
                    let _ = tx.send(ev).await;
                }
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    });

    // TUI setup
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let mut app = AppState::new(Some(user_info.clone()));
    let trading_ctx = match TradingContext::new(cfg.clone(), http_pool.clone(), &user_info).await {
        Ok(ctx) => {
            app.trade.enabled = true;
            app.trade.status = Some("Trading ready".into());
            Some(Arc::new(ctx))
        }
        Err(e) => {
            app.trade.status = Some(format!("Trading disabled: {}", e));
            None
        }
    };

    // key events
    let mut keys = crossterm::event::EventStream::new();

    // channels for user events and periodic open order snapshots
    let (ue_tx, mut ue_rx) = mpsc::channel::<UserEvent>(2000);
    let (ord_tx, mut ord_rx) = mpsc::channel::<Vec<OpenOrder>>(16);
    if let Some(ctx) = trading_ctx.as_ref() {
        let creds = ctx.api_creds();
        let user = UserWs::new(ctx.user_ws_url(), creds);
        match user.connect().await {
            Ok(_) => {
                let utx = ue_tx.clone();
                tokio::spawn(async move {
                    loop {
                        match user.next().await {
                            Ok(Some(ev)) => {
                                let _ = utx.send(ev).await;
                            }
                            Ok(None) => tokio::time::sleep(Duration::from_millis(50)).await,
                            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
                        }
                    }
                });
            }
            Err(e) => {
                app.trade.status = Some(format!("User stream unavailable: {}", e));
            }
        }
    }

    // periodic open orders sync every 2s
    if let Some(ctx) = trading_ctx.as_ref() {
        let ex = ctx.exchange.clone();
        let otx = ord_tx.clone();
        tokio::spawn(async move {
            let mut intv = tokio::time::interval(Duration::from_secs(3));
            loop {
                intv.tick().await;
                if let Ok(list) = ex.get_orders().await {
                    let _ = otx.send(list).await;
                }
            }
        });
    }

    // periodic positions snapshot every 2s
    let (pos_tx, mut pos_rx) = mpsc::channel::<Vec<PositionInfo>>(16);
    let (cash_tx, mut cash_rx) = mpsc::channel::<f64>(4);
    if let Some(ctx) = trading_ctx.as_ref() {
        let ctx_for_pos = Arc::clone(ctx);
        let ptx = pos_tx.clone();
        tokio::spawn(async move {
            let mut intv = tokio::time::interval(Duration::from_secs(2));
            loop {
                intv.tick().await;
                match ctx_for_pos.exchange.fetch_positions_detail().await {
                    Ok(pos) => {
                        let _ = ptx.send(pos).await;
                    }
                    Err(err) => {
                        tracing::debug!(target: "search_ob_tui", ?err, "fetch_positions_detail failed");
                    }
                }
            }
        });
        // periodic cash (USDC.e) balance every 2s via Polygon RPC
        let ctx2 = Arc::clone(ctx);
        let cash_sender = cash_tx.clone();
        tokio::spawn(async move {
            let mut intv = tokio::time::interval(Duration::from_secs(3));
            let endpoints = ctx2.rpc_endpoints.clone();
            loop {
                intv.tick().await;
                // delegate to OMS helper for modularization
                if let Some(addr) = ctx2.wallet_addr.as_ref() {
                    for rpc in endpoints.iter() {
                        match engine::oms::fetch_usdce_balance(rpc, addr).await {
                            Ok(v) => {
                                let _ = cash_sender.send(v).await;
                                break;
                            }
                            Err(err) => {
                                tracing::debug!(
                                    target: "search_ob_tui",
                                    rpc = %rpc,
                                    wallet = %addr,
                                    ?err,
                                    "fetch_usdce_balance retry"
                                );
                                continue;
                            }
                        }
                    }
                }
            }
        });
    }

    let (activity_ws_tx, mut activity_ws_rx) = mpsc::channel::<ActivityEvent>(2000);
    let (activity_tx, mut activity_rx) = mpsc::channel::<Vec<ActivityEntry>>(8);
    let market_info_client = Arc::new(reqwest::Client::new());
    let (market_info_tx, mut market_info_rx) = mpsc::channel::<MarketInfoUpdate>(32);
    let (market_info_req_tx, market_info_req_rx) = mpsc::unbounded_channel::<String>();
    {
        let client = Arc::clone(&market_info_client);
        let tx_updates = market_info_tx.clone();
        tokio::spawn(async move {
            let mut req_rx = market_info_req_rx;
            let mut pending: HashSet<String> = HashSet::new();
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                tokio::select! {
                    Some(token) = req_rx.recv() => {
                        pending.insert(token);
                    }
                    _ = interval.tick() => {
                        if pending.is_empty() {
                            continue;
                        }
                        let mut batch: Vec<String> = Vec::new();
                        for token in pending.iter().take(MARKET_INFO_BATCH_LIMIT) {
                            batch.push(token.clone());
                        }
                        for token in &batch {
                            pending.remove(token);
                        }
                        let result = fetch_market_info_batch(&client, &batch).await;
                        match result {
                            Ok(entries) => {
                                if !entries.is_empty() {
                                    let _ = tx_updates
                                        .send(MarketInfoUpdate::Loaded(entries.clone()))
                                        .await;
                                }
                                let fetched_tokens: HashSet<String> = entries
                                    .iter()
                                    .map(|e| e.token_id.clone())
                                    .collect();
                                let missing: Vec<String> = batch
                                    .iter()
                                    .filter(|t| !fetched_tokens.contains(*t))
                                    .cloned()
                                    .collect();
                                if !missing.is_empty() {
                                    for token in missing {
                                        pending.insert(token);
                                    }
                                }
                            }
                            Err(err) => {
                                tracing::debug!(
                                    target: "search_ob_tui",
                                    ?err,
                                    "fetch_market_info batch failed"
                                );
                                for token in batch {
                                    pending.insert(token);
                                }
                            }
                        }
                    }
                }
            }
        });
    }
    if !user_info.proxy_wallet.is_empty() {
        let wallet = user_info.proxy_wallet.clone();
        let atx = activity_tx.clone();
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            match fetch_user_activity(&client, &wallet).await {
                Ok(records) => {
                    let _ = atx.send(records).await;
                }
                Err(err) => {
                    tracing::debug!(target: "search_ob_tui", ?err, "fetch_user_activity failed");
                }
            }
            let mut intv = tokio::time::interval(Duration::from_secs(5));
            loop {
                intv.tick().await;
                match fetch_user_activity(&client, &wallet).await {
                    Ok(records) => {
                        let _ = atx.send(records).await;
                    }
                    Err(err) => {
                        tracing::debug!(target: "search_ob_tui", ?err, "fetch_user_activity failed");
                    }
                }
            }
        });
    }

    {
        let activity_stream = ActivityWs::new(&cfg.ws.user_ws_url);
        match activity_stream.connect().await {
            Ok(_) => {
                let tx_ws = activity_ws_tx.clone();
                tokio::spawn(async move {
                    let stream = activity_stream;
                    loop {
                        match stream.next().await {
                            Ok(Some(ev)) => {
                                let _ = tx_ws.send(ev).await;
                            }
                            Ok(None) => tokio::time::sleep(Duration::from_millis(50)).await,
                            Err(err) => {
                                tracing::debug!(target: "search_ob_tui", ?err, "activity stream error");
                                tokio::time::sleep(Duration::from_millis(200)).await;
                            }
                        }
                    }
                });
            }
            Err(err) => {
                tracing::warn!(target: "search_ob_tui", ?err, "activity stream unavailable");
            }
        }
    }

    let mut ticker_tick = tokio::time::interval(Duration::from_millis(120));

    loop {
        // Draw
        terminal.draw(|f| draw_ui(f, &mut app))?;

        tokio::select! {
            Some(ev) = rx.recv() => {
                match ev {
                    MarketEvent::OrderBook(book) => {
                        let bids: Vec<(f64,f64)> = book.bids.into_iter().map(|l| (l.price, l.size)).collect();
                        let asks: Vec<(f64,f64)> = book.asks.into_iter().map(|l| (l.price, l.size)).collect();
                        let (snapshot_bids, snapshot_asks) = {
                            let entry = app
                                .books
                                .entry(book.asset_id.clone())
                                .or_insert_with(|| OrderBookState::new(book.asset_id.clone()));
                            entry.apply_snapshot(&bids, &asks);
                            (entry.bids.levels.clone(), entry.asks.levels.clone())
                        };
                        app.record_orderbook_snapshot(
                            &book.asset_id,
                            vec![book.hash.clone()],
                            book.timestamp,
                            snapshot_bids,
                            snapshot_asks,
                        );
                    }
                    MarketEvent::DepthUpdate(upd) => {
                        let mut hashes: HashMap<String, Vec<String>> = HashMap::new();
                        for ch in upd.price_changes.into_iter() {
                            {
                                let entry = app
                                    .books
                                    .entry(ch.asset_id.clone())
                                    .or_insert_with(|| OrderBookState::new(ch.asset_id.clone()));
                                let is_bid = matches!(ch.side, Side::Buy);
                                entry.apply_delta(ch.price, ch.size, is_bid);
                            }
                            hashes
                                .entry(ch.asset_id.clone())
                                .or_default()
                                .push(ch.hash.clone());
                        }
                        for (asset_id, event_hashes) in hashes.into_iter() {
                            if let Some(book) = app.books.get(&asset_id) {
                                app.record_orderbook_snapshot(
                                    &asset_id,
                                    event_hashes,
                                    upd.timestamp,
                                    book.bids.levels.clone(),
                                    book.asks.levels.clone(),
                                );
                            }
                        }
                    }
                    MarketEvent::PublicTrade(trade) => {
                        schedule_market_info_fetch(
                            &mut app,
                            std::iter::once(trade.asset_id.clone()),
                            &market_info_req_tx,
                        );
                        let outcome = asset_outcome_label(&app, &trade.asset_id);
                        app.record_public_trade(&trade);
                        app.record_trade_event(&trade, outcome);
                    }
                    _ => {}
                }
            }
            Some(list) = pos_rx.recv() => {
                let mut value_sum = 0.0;
                let mut seen_ids: HashSet<String> = HashSet::new();
                for p in list.iter() {
                    seen_ids.insert(p.asset_id.clone());
                    app.base_positions.insert(p.asset_id.clone(), p.size);
                    if let Some(cur) = p.current_value {
                        value_sum += cur;
                    }
                }

                // Remove entries no longer present
                app.base_positions.retain(|k, _| seen_ids.contains(k));
                app.delta_positions.retain(|k, _| seen_ids.contains(k));
                app.order_matched_seen.retain(|k, _| seen_ids.contains(k));

                app.positions = list;
                app.positions_value_sum = value_sum;
                app.positions_initialized = true;
                app.clamp_positions_state();
            }
            Some(uev) = ue_rx.recv() => {
                match uev {
                    UserEvent::Order(upd) => {
                        if let Some(o) = app.orders_map.get_mut(&upd.id) {
                            o.size_matched = upd.size_matched;
                            let s = upd.status.to_uppercase();
                            o.status = match s.as_str() {
                                "LIVE" => OrderStatus::Live,
                                "MATCHED" => OrderStatus::Matched,
                                "PENDING_NEW" => OrderStatus::PendingNew,
                                "CANCELLED" | "CANCELED" => OrderStatus::Cancelled,
                                "REJECTED" => OrderStatus::Rejected,
                                _ => o.status,
                            };
                        }
                        // 增量维护仓位：根据 matched 的增量（累计值差分）
                        let prev = app.order_matched_seen.get(&upd.id).copied().unwrap_or(0.0);
                        let inc = (upd.size_matched - prev).max(0.0);
                        if inc > 0.0 {
                            let sign = if upd.side == Side::Buy { 1.0 } else { -1.0 };
                            *app.delta_positions.entry(upd.asset_id.clone()).or_insert(0.0) += sign * inc;
                            app.order_matched_seen.insert(upd.id.clone(), upd.size_matched);
                        }
                        schedule_market_info_fetch(
                            &mut app,
                            std::iter::once(upd.asset_id.clone()),
                            &market_info_req_tx,
                        );
                    }
                    UserEvent::Trade(_t) => {}
                }
            }
            Some(snapshot) = ord_rx.recv() => {
                app.orders_map.clear();
                for o in snapshot.into_iter() {
                    app.orders_map.insert(o.id.clone(), o);
                }
                app.clamp_orders_state(app.orders_map.len());
                let tokens: Vec<String> = app
                    .orders_map
                    .values()
                    .map(|o| o.asset_id.clone())
                    .collect();
                schedule_market_info_fetch(
                    &mut app,
                    tokens,
                    &market_info_req_tx,
                );
            }
            Some(cash) = cash_rx.recv() => {
                // 若查询失败上游不会发值，这里只在成功时覆盖，避免变 0
                app.cash_usdce = cash;
            }
            Some(ws_event) = activity_ws_rx.recv() => {
                if !ws_event.asset.is_empty() {
                    schedule_market_info_fetch(
                        &mut app,
                        std::iter::once(ws_event.asset.clone()),
                        &market_info_req_tx,
                    );
                }
                app.handle_live_activity(&ws_event);
            }
            Some(records) = activity_rx.recv() => {
                app.trade_activities = records;
                app.clamp_trades_state();
            }
            Some(update) = market_info_rx.recv() => {
                match update {
                    MarketInfoUpdate::Loaded(entries) => {
                        for entry in entries.into_iter() {
                            app.token_market_cache.insert(
                                entry.token_id.clone(),
                                TokenMarketInfo {
                                    question: entry.question,
                                    outcome: entry.outcome,
                                    slug: entry.slug,
                                },
                            );
                            app.token_market_pending.remove(&entry.token_id);
                        }
                    }
                    MarketInfoUpdate::Failed(tokens) => {
                        for token in tokens.into_iter() {
                            app.token_market_pending.remove(&token);
                        }
                    }
                }
            }
            _ = ticker_tick.tick() => {
                app.advance_ticker();
                app.advance_recent_trades();
            }
            Some(Ok(evt)) = keys.next() => {
                if handle_key_event(evt, &mut app, &market, trading_ctx.as_ref()).await? { break; }
            }
            else => {
                // fallthrough for optional branches
            }
        }
    }

    // cleanup
    disable_raw_mode()?;
    let mut stdout2 = io::stdout();
    execute!(stdout2, LeaveAlternateScreen)?;
    Ok(())
}

async fn fetch_user_activity(
    client: &reqwest::Client,
    proxy_wallet: &str,
) -> Result<Vec<ActivityEntry>> {
    let url = format!(
        "https://data-api.polymarket.com/activity?user={}&limit=1000&offset=0",
        proxy_wallet
    );
    let resp = client
        .get(&url)
        .header(ACCEPT, "application/json")
        .send()
        .await?;
    let resp = resp.error_for_status()?;
    let rows: Vec<ActivityRow> = resp.json().await?;
    let mut entries: Vec<ActivityEntry> = rows
        .into_iter()
        .filter_map(ActivityEntry::from_row)
        .collect();
    entries.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    Ok(entries)
}

fn schedule_market_info_fetch(
    app: &mut AppState,
    tokens: impl IntoIterator<Item = String>,
    req_tx: &UnboundedSender<String>,
) {
    for token in tokens {
        if app.token_market_cache.contains_key(&token) {
            continue;
        }
        if app.token_market_pending.insert(token.clone()) {
            let _ = req_tx.send(token);
        }
    }
}

async fn fetch_market_info_batch(
    client: &reqwest::Client,
    tokens: &[String],
) -> Result<Vec<TokenMarketEntry>> {
    if tokens.is_empty() {
        return Ok(Vec::new());
    }

    let mut url = reqwest::Url::parse("https://gamma-api.polymarket.com/markets")?;
    {
        let mut pairs = url.query_pairs_mut();
        for token in tokens {
            pairs.append_pair("clob_token_ids", token);
        }
    }

    let resp = client
        .get(url)
        .header(ACCEPT, "application/json")
        .send()
        .await?;
    let resp = resp.error_for_status()?;
    let markets: Vec<MarketSummary> = resp.json().await?;

    let mut entries: Vec<TokenMarketEntry> = Vec::new();
    for market in markets.into_iter() {
        let outcomes = market.outcomes;
        for (idx, token_id) in market.clob_token_ids.into_iter().enumerate() {
            let outcome = outcomes
                .get(idx)
                .cloned()
                .unwrap_or_else(|| format!("Outcome {}", idx + 1));
            entries.push(TokenMarketEntry {
                token_id,
                question: market.question.clone(),
                outcome,
                slug: market.slug.clone(),
            });
        }
    }

    Ok(entries)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarketSummary {
    question: String,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default, deserialize_with = "deserialize_string_vec")]
    outcomes: Vec<String>,
    #[serde(
        default,
        rename = "clobTokenIds",
        deserialize_with = "deserialize_string_vec"
    )]
    clob_token_ids: Vec<String>,
}

fn deserialize_string_vec<'de, D>(deserializer: D) -> std::result::Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum MaybeVec {
        Vec(Vec<String>),
        Str(String),
        Null,
    }

    match MaybeVec::deserialize(deserializer)? {
        MaybeVec::Vec(v) => Ok(v),
        MaybeVec::Str(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return Ok(Vec::new());
            }
            serde_json::from_str::<Vec<String>>(trimmed).map_err(de::Error::custom)
        }
        MaybeVec::Null => Ok(Vec::new()),
    }
}

async fn handle_key_event(
    evt: Event,
    app: &mut AppState,
    market: &MarketMultiWs,
    trading: Option<&Arc<TradingContext>>,
) -> Result<bool> {
    if let Event::Key(KeyEvent { code, .. }) = evt {
        if matches!(app.view_mode, ViewMode::User) {
            match code {
                KeyCode::Esc | KeyCode::Char('/') => {
                    app.view_mode = ViewMode::Main;
                    app.pending_slash = matches!(code, KeyCode::Char('/'));
                }
                KeyCode::Char('e') | KeyCode::Char('E') => {
                    if let Err(err) = export_private_key(app) {
                        app.user_status = Some(format!("Export failed: {}", err));
                    }
                }
                KeyCode::Char('t') | KeyCode::Char('T') => {
                    if app.user_tab != UserTab::Trades {
                        app.user_tab = UserTab::Trades;
                        app.clamp_trades_state();
                    }
                }
                KeyCode::Char('p') | KeyCode::Char('P') => {
                    if app.user_tab != UserTab::Positions {
                        app.user_tab = UserTab::Positions;
                        app.clamp_positions_state();
                    }
                }
                KeyCode::Up => {
                    match app.user_tab {
                        UserTab::Positions => {
                            if app.positions.is_empty() {
                                // no-op
                            } else {
                                let (start, _) = app.current_positions_slice();
                                if app.positions_sel > start {
                                    app.positions_sel -= 1;
                                } else if app.positions_page > 0 {
                                    app.positions_page -= 1;
                                    app.clamp_positions_state();
                                    let (_, end) = app.current_positions_slice();
                                    if end > 0 {
                                        app.positions_sel = end - 1;
                                    }
                                }
                            }
                        }
                        UserTab::Trades => {
                            if app.trade_activities.is_empty() {
                                // no-op
                            } else {
                                let (start, _) = app.current_trades_slice();
                                if app.trades_sel > start {
                                    app.trades_sel -= 1;
                                } else if app.trades_page > 0 {
                                    app.trades_page -= 1;
                                    app.clamp_trades_state();
                                    let (_, end) = app.current_trades_slice();
                                    if end > 0 {
                                        app.trades_sel = end - 1;
                                    }
                                }
                            }
                        }
                    }
                }
                KeyCode::Down => {
                    match app.user_tab {
                        UserTab::Positions => {
                            if app.positions.is_empty() {
                                // no-op
                            } else {
                                let (_, end) = app.current_positions_slice();
                                if app.positions_sel + 1 < end {
                                    app.positions_sel += 1;
                                } else if end < app.positions.len() {
                                    app.positions_page += 1;
                                    app.clamp_positions_state();
                                }
                            }
                        }
                        UserTab::Trades => {
                            if app.trade_activities.is_empty() {
                                // no-op
                            } else {
                                let (_, end) = app.current_trades_slice();
                                if app.trades_sel + 1 < end {
                                    app.trades_sel += 1;
                                } else if end < app.trade_activities.len() {
                                    app.trades_page += 1;
                                    app.clamp_trades_state();
                                }
                            }
                        }
                    }
                }
                KeyCode::Left => match app.user_tab {
                    UserTab::Positions => {
                        if app.positions_page > 0 {
                            app.positions_page -= 1;
                            app.clamp_positions_state();
                        }
                    }
                    UserTab::Trades => {
                        if app.trades_page > 0 {
                            app.trades_page -= 1;
                            app.clamp_trades_state();
                        }
                    }
                },
                KeyCode::Right => match app.user_tab {
                    UserTab::Positions => {
                        let total_pages = app.positions_total_pages();
                        if app.positions_page + 1 < total_pages {
                            app.positions_page += 1;
                            app.clamp_positions_state();
                        }
                    }
                    UserTab::Trades => {
                        let total_pages = app.trades_total_pages();
                        if app.trades_page + 1 < total_pages {
                            app.trades_page += 1;
                            app.clamp_trades_state();
                        }
                    }
                },
                KeyCode::Enter => match app.user_tab {
                    UserTab::Positions => {
                        if let Some(pos) = app.positions.get(app.positions_sel).cloned() {
                            app.view_mode = ViewMode::Main;
                            if let Err(err) =
                                ensure_event_detail_from_position(app, market, trading, pos).await
                            {
                                app.last_error =
                                    Some(format!("Failed to load position market: {}", err));
                            } else {
                                app.last_error = None;
                            }
                        }
                    }
                    UserTab::Trades => {
                        if let Some(entry) = app.trade_activities.get(app.trades_sel).cloned() {
                            app.view_mode = ViewMode::Main;
                            if let Err(err) =
                                ensure_event_detail_from_activity(app, market, trading, entry).await
                            {
                                app.last_error =
                                    Some(format!("Failed to load activity market: {}", err));
                            } else {
                                app.last_error = None;
                            }
                        }
                    }
                },
                _ => {}
            }
            return Ok(false);
        }

        if matches!(app.view_mode, ViewMode::Orders) {
            let orders = current_orders_snapshot(app);
            let total = orders.len();
            app.clamp_orders_state(total);
            match code {
                KeyCode::Esc | KeyCode::Char('o') | KeyCode::Char('O') | KeyCode::Char('/') => {
                    app.view_mode = ViewMode::Main;
                    app.pending_slash = false;
                }
                KeyCode::Up => {
                    if total > 0 {
                        let (start_idx, _) = app.current_orders_slice(total);
                        if app.orders_view_sel > start_idx {
                            app.orders_view_sel -= 1;
                        } else if app.orders_page > 0 {
                            app.orders_page -= 1;
                            app.clamp_orders_state(total);
                        }
                    }
                }
                KeyCode::Down => {
                    if total > 0 {
                        let (_, end_idx) = app.current_orders_slice(total);
                        if app.orders_view_sel + 1 < end_idx {
                            app.orders_view_sel += 1;
                        } else if end_idx < total {
                            app.orders_page += 1;
                            app.clamp_orders_state(total);
                        }
                    }
                }
                KeyCode::Left => {
                    if app.orders_page > 0 {
                        app.orders_page -= 1;
                        app.clamp_orders_state(total);
                    }
                }
                KeyCode::Right => {
                    let pages = app.orders_total_pages(total);
                    if pages > 0 && app.orders_page + 1 < pages {
                        app.orders_page += 1;
                        app.clamp_orders_state(total);
                    }
                }
                KeyCode::Char('c') | KeyCode::Char('C') => {
                    if let Some(order) = orders.get(app.orders_view_sel).cloned() {
                        if let Some(ctx) = trading {
                            let _ = ctx.exchange.cancel_orders(vec![order.id.clone()]).await;
                        }
                        if let Some(oo) = app.orders_map.get_mut(&order.id) {
                            oo.status = OrderStatus::Cancelled;
                        }
                        let updated_total = current_orders_snapshot(app).len();
                        app.clamp_orders_state(updated_total);
                    }
                }
                KeyCode::Enter => {
                    if let Some(order) = orders.get(app.orders_view_sel).cloned() {
                        app.view_mode = ViewMode::Main;
                        let slug_hint = app
                            .token_market_cache
                            .get(&order.asset_id)
                            .and_then(|info| info.slug.clone());
                        let fallback_order = order.clone();
                        let mut result = ensure_event_detail_from_token(
                            app,
                            market,
                            trading,
                            &order.asset_id,
                            slug_hint.as_deref(),
                        )
                        .await;
                        if result.is_err() {
                            result = ensure_event_detail_from_order(
                                app,
                                market,
                                trading,
                                fallback_order,
                            )
                            .await;
                        }

                        if let Err(err) = result {
                            app.last_error = Some(format!("Failed to load order market: {}", err));
                        } else {
                            app.last_error = None;
                        }
                    }
                }
                _ => {}
            }
            return Ok(false);
        }
        if let KeyCode::Char('/') = code {
            app.pending_slash = true;
            return Ok(false);
        }

        if app.pending_slash {
            app.pending_slash = false;
            if matches!(code, KeyCode::Char('u') | KeyCode::Char('U')) {
                app.view_mode = ViewMode::User;
                app.clamp_positions_state();
                app.clamp_trades_state();
                return Ok(false);
            } else if matches!(code, KeyCode::Char('o') | KeyCode::Char('O')) {
                app.view_mode = ViewMode::Orders;
                let orders = current_orders_snapshot(app);
                if orders.is_empty() {
                    app.orders_view_sel = 0;
                } else {
                    app.orders_view_sel = app.orders_view_sel.min(orders.len() - 1);
                }
                return Ok(false);
            } else if matches!(code, KeyCode::Char('r') | KeyCode::Char('R')) {
                if app.recording.is_some() {
                    if let Some((receiver, handle)) = app.prepare_stop_recording()? {
                        let summary = match receiver.await {
                            Ok(s) => s,
                            Err(_) => RecordingSummary::default(),
                        };
                        app.finalize_recording(handle, summary)?;
                    }
                } else if let Err(err) = app.start_recording_session() {
                    app.recording_message = Some(format!("Recording error: {}", err));
                }
                return Ok(false);
            }
            if app.focus == Focus::Search {
                app.search_input.push('/');
                if let KeyCode::Char(c) = code {
                    app.search_input.push(c);
                }
            }
            return Ok(false);
        }

        match code {
            KeyCode::Esc | KeyCode::Char('q') => return Ok(true),
            KeyCode::Tab => {
                app.focus = match app.focus {
                    Focus::Search => Focus::Event,
                    Focus::Event => Focus::Book,
                    Focus::Book => Focus::Trade,
                    Focus::Trade => Focus::Orders,
                    Focus::Orders => Focus::Search,
                };
            }
            KeyCode::Left => {
                app.focus = match app.focus {
                    Focus::Search => Focus::Search,
                    Focus::Event => Focus::Search,
                    Focus::Book => Focus::Event,
                    Focus::Trade => Focus::Book,
                    Focus::Orders => Focus::Trade,
                };
            }
            KeyCode::Right => {
                app.focus = match app.focus {
                    Focus::Search => Focus::Event,
                    Focus::Event => Focus::Book,
                    Focus::Book => Focus::Trade,
                    Focus::Trade => Focus::Orders,
                    Focus::Orders => Focus::Orders,
                };
            }
            _ => {}
        }

        match app.focus {
            Focus::Search => match code {
                KeyCode::Enter => {
                    // run search
                    let q = app.search_input.trim().to_string();
                    if !q.is_empty() {
                        app.cache.clear();
                        app.books.clear();
                        let _ = market.subscribe(vec![]).await;
                        if let Ok(resp) = search_events(&q, 20).await {
                            app.results = resp.events;
                            app.sel_event_idx = 0;
                            app.sel_market_idx = 0;
                            app.sel_outcome_idx = 0;
                            app.last_error = None;
                        }
                    }
                }
                KeyCode::Backspace => {
                    app.search_input.pop();
                }
                KeyCode::Char(c) => {
                    app.search_input.push(c);
                }
                KeyCode::Up => {
                    if app.sel_event_idx > 0 {
                        app.sel_event_idx -= 1;
                        app.books.clear();
                        let _ = market.subscribe(vec![]).await;
                    }
                }
                KeyCode::Down => {
                    if app.sel_event_idx + 1 < app.results.len() {
                        app.sel_event_idx += 1;
                        app.books.clear();
                        let _ = market.subscribe(vec![]).await;
                    }
                }
                KeyCode::Right => {
                    if let Err(e) = ensure_event_detail_loaded(app, market, trading).await {
                        app.last_error = Some(format!("Failed to load event detail: {}", e));
                    } else {
                        app.last_error = None;
                    }
                    app.focus = Focus::Event;
                }
                _ => {}
            },
            Focus::Event => match code {
                KeyCode::Enter | KeyCode::Right => {
                    if let Err(e) = ensure_event_detail_loaded(app, market, trading).await {
                        app.last_error = Some(format!("Failed to load event detail: {}", e));
                    } else {
                        app.last_error = None;
                    }
                    app.focus = Focus::Book;
                }
                KeyCode::Up => {
                    if app.sel_market_idx > 0 {
                        app.sel_market_idx -= 1;
                    }
                }
                KeyCode::Down => {
                    if let Some(ev) = app.current_event() {
                        if app.sel_market_idx + 1 < ev.markets.len() {
                            app.sel_market_idx += 1;
                        }
                    }
                }
                KeyCode::Left => {
                    app.focus = Focus::Search;
                }
                _ => {}
            },
            Focus::Book => match code {
                KeyCode::Left => {
                    app.focus = Focus::Event;
                }
                KeyCode::Right => {
                    app.focus = Focus::Trade;
                }
                KeyCode::Char('o') | KeyCode::Char('O') => {
                    toggle_outcome_selection(app);
                }
                KeyCode::Up => {
                    if app.sel_market_idx > 0 {
                        app.sel_market_idx -= 1;
                    }
                }
                KeyCode::Down => {
                    if let Some(ev) = app.current_event() {
                        if app.sel_market_idx + 1 < ev.markets.len() {
                            app.sel_market_idx += 1;
                        }
                    }
                }
                _ => {}
            },
            Focus::Trade => match code {
                KeyCode::Left => {
                    app.focus = Focus::Book;
                }
                KeyCode::Up => {
                    app.trade.field = app.trade.field.prev();
                }
                KeyCode::Down => {
                    app.trade.field = app.trade.field.next();
                }
                KeyCode::Tab => {
                    app.focus = Focus::Search;
                }
                KeyCode::Backspace => match app.trade.field {
                    TradeField::Price => {
                        app.trade.price.pop();
                    }
                    TradeField::Size => {
                        app.trade.size.pop();
                    }
                    _ => {}
                },
                KeyCode::Char(c) => {
                    match c {
                        'b' | 'B' => {
                            app.trade.side = Side::Buy;
                        }
                        's' | 'S' => {
                            app.trade.side = Side::Sell;
                        }
                        'o' | 'O' => {
                            toggle_outcome_selection(app);
                        }
                        ' ' => {
                            if app.trade.field == TradeField::Side {
                                app.trade.toggle_side();
                            }
                        }
                        _ => {}
                    }

                    match app.trade.field {
                        TradeField::Price | TradeField::Size => {
                            if matches!(c, '0'..='9' | '.') {
                                let target = if app.trade.field == TradeField::Price {
                                    &mut app.trade.price
                                } else {
                                    &mut app.trade.size
                                };
                                target.push(c);
                            }
                        }
                        TradeField::Side | TradeField::Submit => {}
                    }
                }
                KeyCode::Enter => match app.trade.field {
                    TradeField::Price => {
                        app.trade.field = TradeField::Size;
                    }
                    TradeField::Size => {
                        app.trade.field = TradeField::Side;
                    }
                    TradeField::Side => {
                        app.trade.field = TradeField::Submit;
                    }
                    TradeField::Submit => {
                        attempt_submit_order(app, trading).await?;
                    }
                },
                _ => {}
            },
            Focus::Orders => match code {
                KeyCode::Left => {
                    app.focus = Focus::Trade;
                }
                KeyCode::Up => {
                    let total = current_market_open_orders(app).len();
                    if total > 0 {
                        if app.sel_order_idx == 0 {
                            app.sel_order_idx = total - 1;
                        } else {
                            app.sel_order_idx -= 1;
                        }
                    }
                }
                KeyCode::Down => {
                    let total = current_market_open_orders(app).len();
                    if total > 0 {
                        app.sel_order_idx = (app.sel_order_idx + 1) % total;
                    }
                }
                KeyCode::Enter => {
                    let selected_order = {
                        let open_list = current_market_open_orders(app);
                        open_list.get(app.sel_order_idx).cloned()
                    };
                    if let Some(order) = selected_order {
                        let slug_hint = app
                            .token_market_cache
                            .get(&order.asset_id)
                            .and_then(|info| info.slug.clone());
                        let fallback_order = order.clone();
                        let mut result = ensure_event_detail_from_token(
                            app,
                            market,
                            trading,
                            &order.asset_id,
                            slug_hint.as_deref(),
                        )
                        .await;
                        if result.is_err() {
                            result = ensure_event_detail_from_order(
                                app,
                                market,
                                trading,
                                fallback_order,
                            )
                            .await;
                        }
                        if let Err(err) = result {
                            app.last_error = Some(format!("Failed to load order market: {}", err));
                        } else {
                            app.last_error = None;
                        }
                    }
                }
                KeyCode::Char('c') | KeyCode::Char('C') => {
                    // 取消选中订单
                    let open_list = current_market_open_orders(app);
                    if let Some(o) = open_list.get(app.sel_order_idx) {
                        let oid = o.id.clone();
                        if let Some(ctx) = trading {
                            let _ = ctx.exchange.cancel_orders(vec![oid.clone()]).await;
                        }
                        if let Some(oo) = app.orders_map.get_mut(&oid) {
                            oo.status = OrderStatus::Cancelled;
                        }
                        // 收敛索引（订单被过滤后可能长度变短）
                        let total_after = current_market_open_orders(app).len();
                        if total_after == 0 {
                            app.sel_order_idx = 0;
                        } else if app.sel_order_idx >= total_after {
                            app.sel_order_idx = total_after - 1;
                        }
                    }
                }
                _ => {}
            },
        }
    }
    Ok(false)
}

fn toggle_outcome_selection(app: &mut AppState) {
    let total = app
        .current_event()
        .and_then(|ev| ev.markets.get(app.sel_market_idx))
        .map(|market| {
            let tokens = market.clobTokenIds.len();
            let outcomes = market.outcomes.len();
            tokens.max(outcomes)
        })
        .unwrap_or(0);

    if total <= 1 {
        app.sel_outcome_idx = 0;
    } else {
        app.sel_outcome_idx = (app.sel_outcome_idx + 1) % total;
    }
}

async fn ensure_event_detail_loaded(
    app: &mut AppState,
    market_ws: &MarketMultiWs,
    trading: Option<&Arc<TradingContext>>,
) -> Result<()> {
    let slug = match app.current_event_slug() {
        Some(s) => s,
        None => return Ok(()),
    };
    if app.loading_detail {
        return Ok(());
    }
    app.loading_detail = true;
    let id_opt = app.results.get(app.sel_event_idx).map(|e| e.id.as_str());
    let detail = fetch_event_detail(&slug, id_opt).await?;
    app.books.clear();
    let _ = market_ws.subscribe(vec![]).await;
    // subscribe all token ids from this event (de-duped, preserving order)
    let mut seen = HashSet::new();
    let mut ids: Vec<String> = Vec::new();
    for m in detail.markets.iter() {
        for t in m.clobTokenIds.iter() {
            if seen.insert(t.clone()) {
                ids.push(t.clone());
            }
        }
    }
    if !ids.is_empty() {
        market_ws.subscribe(ids.clone()).await?;
        for id in ids.iter() {
            app.books
                .entry(id.clone())
                .or_insert_with(|| OrderBookState::new(id.clone()));
        }
        if let Some(ctx) = trading {
            let snaps = ctx.fetch_books_snapshot(&ids).await?;
            for snap in snaps.into_iter() {
                let asset_id = snap.order_book.asset_id.clone();
                let bids: Vec<(f64, f64)> = snap
                    .order_book
                    .bids
                    .iter()
                    .map(|lvl| (lvl.price, lvl.size))
                    .collect();
                let asks: Vec<(f64, f64)> = snap
                    .order_book
                    .asks
                    .iter()
                    .map(|lvl| (lvl.price, lvl.size))
                    .collect();
                let entry = app
                    .books
                    .entry(asset_id.clone())
                    .or_insert_with(|| OrderBookState::new(asset_id.clone()));
                entry.apply_snapshot(&bids, &asks);
            }
        }
    }
    // always refresh the cache for the selected slug
    app.cache.insert(slug, detail);
    app.loading_detail = false;
    Ok(())
}

async fn ensure_event_detail_from_position(
    app: &mut AppState,
    market_ws: &MarketMultiWs,
    trading: Option<&Arc<TradingContext>>,
    pos: PositionInfo,
) -> Result<()> {
    let slug = pos.slug.trim().to_string();
    if slug.is_empty() {
        anyhow::bail!("position missing slug");
    }

    app.search_input = slug.clone();
    let resp = search_events(&slug, 20).await?;
    if resp.events.is_empty() {
        anyhow::bail!("no events found for slug {}", slug);
    }
    app.results = resp.events;
    app.sel_event_idx = app.results.iter().position(|e| e.slug == slug).unwrap_or(0);
    app.sel_market_idx = 0;
    app.sel_outcome_idx = 0;

    ensure_event_detail_loaded(app, market_ws, trading).await?;

    let asset_id = pos.asset_id.clone();
    if let Some(ev) = app.current_event() {
        if let Some((m_idx, tok_idx)) = ev.markets.iter().enumerate().find_map(|(i, m)| {
            m.clobTokenIds
                .iter()
                .position(|t| t == &asset_id)
                .map(|tok_idx| (i, tok_idx))
        }) {
            app.sel_market_idx = m_idx;
            app.sel_outcome_idx = tok_idx;
        } else if let Some(oi) = pos.outcome_index {
            if let Some((m_idx, tok_idx)) = ev.markets.iter().enumerate().find_map(|(i, m)| {
                let oi_usize = oi as usize;
                if oi_usize < m.clobTokenIds.len() {
                    Some((i, oi_usize))
                } else {
                    None
                }
            }) {
                app.sel_market_idx = m_idx;
                app.sel_outcome_idx = tok_idx;
            }
        }
    }

    app.focus = Focus::Book;
    Ok(())
}

async fn ensure_event_detail_from_activity(
    app: &mut AppState,
    market_ws: &MarketMultiWs,
    trading: Option<&Arc<TradingContext>>,
    entry: ActivityEntry,
) -> Result<()> {
    let slug = entry
        .slug
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("activity missing slug"))?
        .to_string();

    app.search_input = slug.clone();
    let resp = search_events(&slug, 20).await?;
    if resp.events.is_empty() {
        anyhow::bail!("no events found for slug {}", slug);
    }
    app.results = resp.events;
    app.sel_event_idx = app.results.iter().position(|e| e.slug == slug).unwrap_or(0);
    app.sel_market_idx = 0;
    app.sel_outcome_idx = 0;

    ensure_event_detail_loaded(app, market_ws, trading).await?;

    let mut target: Option<(usize, usize)> = None;
    if let Some(ev) = app.current_event() {
        if let Some(asset) = entry.asset_id.as_ref() {
            target = ev.markets.iter().enumerate().find_map(|(i, m)| {
                m.clobTokenIds
                    .iter()
                    .position(|t| t == asset)
                    .map(|tok_idx| (i, tok_idx))
            });
        }

        if target.is_none() {
            if let Some(oi) = entry.outcome_index {
                target = ev.markets.iter().enumerate().find_map(|(i, m)| {
                    if oi < m.clobTokenIds.len() {
                        Some((i, oi))
                    } else {
                        None
                    }
                });
            }
        }
    }

    if let Some((m_idx, tok_idx)) = target {
        app.sel_market_idx = m_idx;
        app.sel_outcome_idx = tok_idx;
    }

    app.focus = Focus::Book;
    Ok(())
}

async fn ensure_event_detail_from_token(
    app: &mut AppState,
    market_ws: &MarketMultiWs,
    trading: Option<&Arc<TradingContext>>,
    token_id: &str,
    slug_hint: Option<&str>,
) -> Result<()> {
    let slug = slug_hint
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("token {} missing slug", token_id))?
        .to_string();

    app.search_input = slug.clone();
    let resp = search_events(&slug, 20).await?;
    if resp.events.is_empty() {
        anyhow::bail!("no events found for slug {}", slug);
    }
    app.results = resp.events;
    app.sel_event_idx = app.results.iter().position(|e| e.slug == slug).unwrap_or(0);
    app.sel_market_idx = 0;
    app.sel_outcome_idx = 0;

    ensure_event_detail_loaded(app, market_ws, trading).await?;

    if let Some(ev) = app.current_event() {
        if let Some((m_idx, tok_idx)) = ev.markets.iter().enumerate().find_map(|(i, m)| {
            m.clobTokenIds
                .iter()
                .position(|t| t == token_id)
                .map(|tok_idx| (i, tok_idx))
        }) {
            app.sel_market_idx = m_idx;
            app.sel_outcome_idx = tok_idx;
        }
    }

    app.focus = Focus::Book;
    Ok(())
}

async fn ensure_event_detail_from_order(
    app: &mut AppState,
    market_ws: &MarketMultiWs,
    trading: Option<&Arc<TradingContext>>,
    order: OpenOrder,
) -> Result<()> {
    if let Some(slug_owned) = app
        .token_market_cache
        .get(&order.asset_id)
        .and_then(|info| info.slug.clone())
    {
        if ensure_event_detail_from_token(
            app,
            market_ws,
            trading,
            &order.asset_id,
            Some(slug_owned.as_str()),
        )
        .await
        .is_ok()
        {
            return Ok(());
        }
    }

    let mut query = order.market.clone();
    if query.trim().is_empty() {
        query = order.asset_id.clone();
    }
    if query.trim().is_empty() {
        anyhow::bail!("order missing market identifier");
    }

    let resp = search_events(&query, 20).await?;
    if resp.events.is_empty() {
        anyhow::bail!("no events found for query {}", query);
    }
    app.results = resp.events;
    app.sel_event_idx = app
        .results
        .iter()
        .position(|e| e.slug == query)
        .unwrap_or(0);
    app.sel_market_idx = 0;
    app.sel_outcome_idx = 0;

    ensure_event_detail_loaded(app, market_ws, trading).await?;

    let asset_id = order.asset_id.clone();
    if let Some(ev) = app.current_event() {
        if let Some((m_idx, tok_idx)) = ev.markets.iter().enumerate().find_map(|(i, m)| {
            m.clobTokenIds
                .iter()
                .position(|t| t == &asset_id)
                .map(|tok_idx| (i, tok_idx))
        }) {
            app.sel_market_idx = m_idx;
            app.sel_outcome_idx = tok_idx;
        }
    }

    app.focus = Focus::Book;
    Ok(())
}

async fn attempt_submit_order(
    app: &mut AppState,
    trading: Option<&Arc<TradingContext>>,
) -> Result<()> {
    if !app.trade.enabled {
        app.trade.status = Some("Trading unavailable".into());
        return Ok(());
    }

    let (_, token_opt) = match app.selected_market_and_token() {
        Some(res) => res,
        None => {
            app.trade.status = Some("Select a market first".into());
            return Ok(());
        }
    };
    let token_id = match token_opt {
        Some(t) => t,
        None => {
            app.trade.status = Some("No tradable token for the current selection".into());
            return Ok(());
        }
    };

    let price = match app.trade.price.trim().parse::<f64>() {
        Ok(p) if p > 0.0 => p,
        _ => {
            app.trade.status = Some("Enter a valid price".into());
            return Ok(());
        }
    };

    let size = match app.trade.size.trim().parse::<f64>() {
        Ok(s) if s > 0.0 => s,
        _ => {
            app.trade.status = Some("Enter a valid size".into());
            return Ok(());
        }
    };

    let side = app.trade.side;

    let ctx = match trading {
        Some(c) => c,
        None => {
            app.trade.status = Some("Trading context unavailable".into());
            return Ok(());
        }
    };

    app.trade.status = Some("Submitting...".into());
    match ctx.submit_limit_order(&token_id, price, size, side).await {
        Ok((ack, norm_price, norm_size)) => {
            let order_id_opt = ack.order_id.clone();
            app.trade.last_order_id = order_id_opt.clone();
            if ack.success {
                let id_str = order_id_opt.clone().unwrap_or_else(|| "unknown".into());
                app.trade.status = Some(format!(
                    "Order submitted: {} @ {:.4} x {:.2}",
                    id_str, norm_price, norm_size
                ));
                app.trade.field = TradeField::Price;
                app.orders.push(OrderLogEntry {
                    token_id: token_id.clone(),
                    side,
                    price: norm_price,
                    size: norm_size,
                    success: true,
                    order_id: order_id_opt.clone(),
                    error_message: None,
                });
                if let Some(order_id) = order_id_opt {
                    // 乐观插入 open order，仅在返回了订单 ID 的情况下
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or(Duration::from_secs(0))
                        .as_millis() as i64;
                    app.orders_map.insert(
                        order_id.clone(),
                        OpenOrder {
                            id: order_id,
                            status: OrderStatus::PendingNew,
                            market: app.selected_market_title().unwrap_or_default(),
                            size: norm_size,
                            price: norm_price,
                            side,
                            size_matched: 0.0,
                            asset_id: token_id.clone(),
                            created_at: now_ms,
                        },
                    );
                }
            } else {
                let message = ack
                    .error_message
                    .clone()
                    .unwrap_or_else(|| "Order failed".into());
                app.trade.status = Some(message.clone());
                app.orders.push(OrderLogEntry {
                    token_id: token_id.clone(),
                    side,
                    price: norm_price,
                    size: norm_size,
                    success: false,
                    order_id: order_id_opt,
                    error_message: Some(message),
                });
            }
        }
        Err(e) => {
            app.trade.status = Some(format!("Order error: {}", e));
            app.orders.push(OrderLogEntry {
                token_id: token_id.clone(),
                side,
                price,
                size,
                success: false,
                order_id: None,
                error_message: Some(e.to_string()),
            });
        }
    }
    Ok(())
}

fn draw_search(f: &mut ratatui::Frame, area: Rect, app: &AppState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(5), Constraint::Min(3)].as_ref())
        .split(area);
    let title_str = compose_search_title(chunks[0].width);
    let title = Span::styled(
        title_str,
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    );
    let mut lines: Vec<Line> = vec![Line::from(app.search_input.as_str())];
    if let Some(err) = &app.last_error {
        lines.push(Line::from(Span::styled(
            err,
            Style::default().fg(Color::Red),
        )));
    }
    let input = Paragraph::new(lines)
        .block(Block::default().title(title).borders(Borders::ALL))
        .wrap(Wrap { trim: true });
    f.render_widget(input, chunks[0]);

    let items: Vec<ListItem> = app
        .results
        .iter()
        .enumerate()
        .map(|(i, e)| {
            let name = if e.title.is_empty() {
                e.slug.clone()
            } else {
                e.title.clone()
            };
            let line = Line::from(format!("{:>2}. {}", i + 1, name));
            ListItem::new(line)
        })
        .collect();
    let list = List::new(items)
        .block(Block::default().title("Results").borders(Borders::ALL))
        .highlight_style(Style::default().bg(Color::Blue).fg(Color::White));
    let mut state = list_state(app.sel_event_idx, app.results.len());
    f.render_stateful_widget(list, chunks[1], &mut state);
}

fn draw_event(f: &mut ratatui::Frame, area: Rect, app: &AppState) {
    let ev = app.current_event();
    let title_raw = if let Some(detail) = ev {
        detail.title.clone()
    } else {
        app.current_event_slug().unwrap_or_default()
    };
    let title = compose_event_title(area.width, &title_raw);
    let header = Block::default()
        .title(Span::styled(
            title,
            Style::default()
                .fg(Color::Magenta)
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL);
    f.render_widget(header, area);

    if let Some(detail) = ev {
        // inner list of markets
        let inner = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Min(2)])
            .split(area);
        let hint_str = compose_event_hint(inner[0].width);
        let hint = Paragraph::new(hint_str).block(Block::default().borders(Borders::NONE));
        f.render_widget(hint, inner[0]);

        let items: Vec<ListItem> = detail
            .markets
            .iter()
            .enumerate()
            .map(|(i, m)| {
                let status = if m.closed { "[CLOSED]" } else { "" };
                let nm = if m.question.is_empty() {
                    m.slug.clone()
                } else {
                    m.question.clone()
                };
                // 计算该市场下的仓位（base + delta），两边 outcome，多行展示
                let mut lines: Vec<Line> = Vec::new();
                lines.push(Line::from(format!("{:>2}. {} {}", i + 1, nm, status)));
                for (idx, tok) in m.clobTokenIds.iter().enumerate() {
                    let base = app.base_positions.get(tok).copied().unwrap_or(0.0);
                    let delta = app.delta_positions.get(tok).copied().unwrap_or(0.0);
                    let val = if app.positions_initialized {
                        base + delta
                    } else {
                        delta
                    };
                    if val.abs() > 1e-9 {
                        let name = m
                            .outcomes
                            .get(idx)
                            .cloned()
                            .unwrap_or_else(|| format!("Outcome {}", idx + 1));
                        let color = if idx == 0 { Color::Green } else { Color::Red };
                        lines.push(Line::from(vec![
                            Span::raw("    "),
                            Span::styled(
                                format!("{} {:.2}", name, val),
                                Style::default().fg(color).add_modifier(Modifier::BOLD),
                            ),
                        ]));
                    }
                }
                ListItem::new(lines)
            })
            .collect();
        let list = List::new(items)
            .block(Block::default().borders(Borders::NONE))
            .highlight_style(Style::default().bg(Color::Blue).fg(Color::White));
        let mut state = list_state(app.sel_market_idx, detail.markets.len());
        f.render_stateful_widget(list, inner[1], &mut state);
    }
}

fn draw_book(f: &mut ratatui::Frame, area: Rect, app: &AppState) {
    let (market, token_opt) = if let Some((m, t)) = app.selected_market_and_token() {
        (Some(m), t)
    } else {
        (None, None)
    };
    let mut title = match market {
        Some(m) => {
            let nm = if m.question.is_empty() {
                m.slug.clone()
            } else {
                m.question.clone()
            };
            compose_book_title(area.width, &nm, app.sel_outcome_idx)
        }
        None => compose_book_title(area.width, "Order Book", app.sel_outcome_idx),
    };

    if let Some(token) = token_opt.as_deref() {
        if let Some(ob) = app.books.get(token) {
            if !ob.asset_id.is_empty() {
                title = format!("{} [{}]", title, short_id(&ob.asset_id));
            }
        }
    }

    let block = Block::default()
        .title(Span::styled(
            title,
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL);
    let inner = block.inner(area);
    f.render_widget(block, area);

    if inner.width == 0 || inner.height == 0 {
        return;
    }

    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(inner);

    if let Some(token) = token_opt.as_deref() {
        if let Some(ob) = app.books.get(token) {
            let complement_token = market.and_then(|m| {
                if m.clobTokenIds.len() >= 2 {
                    let len = m.clobTokenIds.len();
                    let current_idx = app.sel_outcome_idx.min(len - 1);
                    let complement_idx = if len == 2 {
                        1 - current_idx
                    } else {
                        (current_idx + 1) % len
                    };
                    m.clobTokenIds
                        .get(complement_idx)
                        .filter(|tok| tok.as_str() != token)
                        .cloned()
                } else {
                    None
                }
            });

            let (highlight_bids, highlight_asks) =
                compute_highlight_prices(app, Some(token), complement_token.as_deref());

            f.render_widget(
                DepthColumn::new("Bids", &ob.bids.levels, true, &highlight_bids),
                columns[0],
            );
            f.render_widget(
                DepthColumn::new("Asks", &ob.asks.levels, false, &highlight_asks),
                columns[1],
            );
            return;
        }
    }

    let empty_levels: &[(f64, f64)] = &[];
    let empty_highlights: &[f64] = &[];
    f.render_widget(
        DepthColumn::new("Bids", empty_levels, true, empty_highlights),
        columns[0],
    );
    f.render_widget(
        DepthColumn::new("Asks", empty_levels, false, empty_highlights),
        columns[1],
    );
}

fn draw_trade(f: &mut ratatui::Frame, area: Rect, app: &AppState) {
    let market_title = app
        .selected_market_title()
        .unwrap_or_else(|| "No market selected".into());
    let outcome_title = app
        .selected_outcome_name()
        .unwrap_or_else(|| format!("Outcome {}", app.sel_outcome_idx + 1));
    let token_id = app
        .selected_market_and_token()
        .and_then(|(_, t)| t)
        .unwrap_or_default();
    let token_display = if token_id.is_empty() {
        "-".into()
    } else {
        short_id(&token_id)
    };

    let active = app.focus == Focus::Trade;
    let price_style = TradeForm::highlight_style(active && app.trade.field == TradeField::Price);
    let size_style = TradeForm::highlight_style(active && app.trade.field == TradeField::Size);
    let side_style = {
        let base = TradeForm::highlight_style(active && app.trade.field == TradeField::Side);
        let color = match app.trade.side {
            Side::Buy => Color::Green,
            Side::Sell => Color::Red,
        };
        base.fg(color).add_modifier(Modifier::BOLD)
    };
    let submit_style = TradeForm::highlight_style(active && app.trade.field == TradeField::Submit)
        .add_modifier(Modifier::BOLD);

    let price_val = if app.trade.price.is_empty() {
        "-"
    } else {
        app.trade.price.as_str()
    };
    let size_val = if app.trade.size.is_empty() {
        "-"
    } else {
        app.trade.size.as_str()
    };
    let side_str = match app.trade.side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    };

    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(vec![
        Span::styled(
            "Trading ",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("{} - {}", market_title, outcome_title)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Token: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(token_display),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Price: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(price_val.to_string(), price_style),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Size: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(size_val.to_string(), size_style),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Side: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(side_str.to_string(), side_style),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Submit (Enter)", submit_style),
        Span::raw("  Space: toggle side  Tab: switch focus"),
    ]));

    if let Some(status) = &app.trade.status {
        lines.push(Line::from(Span::styled(
            status.clone(),
            Style::default().fg(Color::Cyan),
        )));
    }
    if let Some(msg) = &app.recording_message {
        lines.push(Line::from(Span::styled(
            msg.clone(),
            Style::default().fg(Color::Magenta),
        )));
    }
    if let Some(last) = &app.trade.last_order_id {
        lines.push(Line::from(Span::styled(
            format!("Last order: {}", short_id(last)),
            Style::default().fg(Color::Gray),
        )));
    }

    let message = if app.trade.enabled {
        ""
    } else {
        "Trading not ready (check private key/API)"
    };
    if !message.is_empty() {
        lines.push(Line::from(Span::styled(
            message,
            Style::default().fg(Color::Red),
        )));
    }

    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: true });
    f.render_widget(paragraph, area);
}

fn list_state(selected: usize, len: usize) -> ratatui::widgets::ListState {
    let mut state = ratatui::widgets::ListState::default();
    if len > 0 {
        state.select(Some(selected.min(len - 1)));
    }
    state
}

fn draw_ticker_line(f: &mut ratatui::Frame, area: Rect, app: &mut AppState) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    let block = Block::default()
        .style(Style::default().bg(Color::Rgb(30, 30, 30)))
        .borders(Borders::NONE);
    let inner = block.inner(area);
    f.render_widget(block, area);
    let text = app.ticker_display(inner.width as usize);
    let line = Line::from(Span::styled(text, Style::default().fg(Color::Yellow)));
    let para = Paragraph::new(line).alignment(Alignment::Left);
    f.render_widget(para, inner);
}

fn draw_ui(f: &mut ratatui::Frame, app: &mut AppState) {
    let full = f.size();
    if full.height == 0 {
        return;
    }
    let regions = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1)])
        .split(full);
    draw_ticker_line(f, regions[0], app);
    let body = regions[1];

    match app.view_mode {
        ViewMode::Positions => {
            draw_positions_view(f, body, app);
        }
        ViewMode::Orders => {
            draw_orders_overview(f, body, app);
        }
        ViewMode::User => {
            draw_user_view(f, body, app);
        }
        ViewMode::Main => {
            draw_main_view(f, body, app);
        }
    }
}

fn draw_main_view(f: &mut ratatui::Frame, area: Rect, app: &mut AppState) {
    if area.height == 0 || area.width == 0 {
        return;
    }

    let mut trade_height = (area.height / 4).max(6);
    if trade_height >= area.height {
        trade_height = area.height.saturating_sub(1);
    }
    if trade_height == 0 {
        trade_height = 1;
    }
    let top_height = area.height.saturating_sub(trade_height);

    let top_rect = Rect {
        x: area.x,
        y: area.y,
        width: area.width,
        height: top_height,
    };
    let top_cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(30),
            Constraint::Percentage(35),
            Constraint::Percentage(35),
        ])
        .split(top_rect);

    let (b0, b1, b2) = (
        outer_block(app.focus == Focus::Search),
        outer_block(app.focus == Focus::Event),
        outer_block(app.focus == Focus::Book),
    );

    if top_height > 0 {
        f.render_widget(b0.clone(), top_cols[0]);
        f.render_widget(b1.clone(), top_cols[1]);
        f.render_widget(b2.clone(), top_cols[2]);

        let c0 = b0.inner(top_cols[0]);
        let c1 = b1.inner(top_cols[1]);
        let c2 = b2.inner(top_cols[2]);

        draw_search(f, c0, app);
        draw_event(f, c1, app);
        draw_book(f, c2, app);
    }

    let trade_y = area.y + top_height;
    let bottom_area = Rect {
        x: area.x,
        y: trade_y,
        width: area.width,
        height: trade_height,
    };
    let bottom_cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(30),
            Constraint::Percentage(35),
            Constraint::Percentage(35),
        ])
        .split(bottom_area);

    let trade_block = outer_block(app.focus == Focus::Trade);
    f.render_widget(trade_block.clone(), bottom_cols[0]);
    draw_trade(f, trade_block.inner(bottom_cols[0]), app);

    let orders_block = outer_block(app.focus == Focus::Orders);
    f.render_widget(orders_block.clone(), bottom_cols[1]);
    let or_area = orders_block.inner(bottom_cols[1]);
    let or_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(3)])
        .split(or_area);
    draw_orders(f, or_chunks[0], app);
    draw_profile(f, or_chunks[1], app);

    let trades_block = outer_block(false).title(Span::styled(
        "Recent Trades",
        Style::default()
            .fg(Color::Magenta)
            .add_modifier(Modifier::BOLD),
    ));
    let trades_inner = trades_block.inner(bottom_cols[2]);
    f.render_widget(trades_block, bottom_cols[2]);
    draw_recent_trades(f, trades_inner, app);
}

fn draw_positions_view(f: &mut ratatui::Frame, area: Rect, app: &mut AppState) {
    let block = Block::default()
        .title(Span::styled(
            "Positions – press /u or Esc to return",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL);
    let inner = block.inner(area);
    f.render_widget(block, area);
    if inner.height == 0 || inner.width == 0 {
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(3)])
        .split(inner);

    let mut summary_lines: Vec<Line> = Vec::new();
    summary_lines.push(Line::from(format!(
        "Total positions: {} | Portfolio value: {:.2} USDC.e",
        app.positions.len(),
        app.positions_value_sum
    )));
    summary_lines.push(Line::from(vec![
        Span::raw("Use "),
        Span::styled("↑/↓", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(" to navigate, "),
        Span::styled("Enter", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(" to open in the main view."),
    ]));
    let summary_para = Paragraph::new(summary_lines).wrap(Wrap { trim: true });
    f.render_widget(summary_para, chunks[0]);

    render_positions_table(f, chunks[1], app);
}

fn draw_orders_overview(f: &mut ratatui::Frame, area: Rect, app: &mut AppState) {
    let block = Block::default()
        .title(Span::styled(
            "Orders – press /o or Esc to return",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL);
    let inner = block.inner(area);
    f.render_widget(block, area);
    if inner.width == 0 || inner.height == 0 {
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            [
                Constraint::Length(2),
                Constraint::Length(1),
                Constraint::Min(2),
            ]
            .as_ref(),
        )
        .split(inner);

    let orders = current_orders_snapshot(app);
    let total = orders.len();

    let list_height = chunks[2].height as usize;
    if list_height == 0 {
        let summary = if total == 0 {
            "Open orders: 0".to_string()
        } else {
            let pages = app.orders_total_pages(total);
            format!(
                "Open orders: {} | Page {}/{} ({} per page)",
                total,
                app.orders_page + 1,
                pages.max(1),
                app.orders_rows_per_page
            )
        };
        let summary_para = Paragraph::new(summary).wrap(Wrap { trim: true });
        f.render_widget(summary_para, chunks[0]);
        let header = format!(
            "{:>3}  {:<18} {:<32} {:<14} {:>5} {:>10} {:>10} {:>10}",
            "#", "Order ID", "Market", "Outcome", "Side", "Price", "Size", "Status"
        );
        f.render_widget(
            Paragraph::new(header)
                .style(Style::default().add_modifier(Modifier::BOLD))
                .wrap(Wrap { trim: true }),
            chunks[1],
        );
        return;
    }

    app.update_orders_rows_per_page(list_height, total);
    app.clamp_orders_state(total);

    let summary = if total == 0 {
        "Open orders: 0".to_string()
    } else {
        let pages = app.orders_total_pages(total);
        format!(
            "Open orders: {} | Page {}/{} ({} per page)",
            total,
            app.orders_page + 1,
            pages.max(1),
            app.orders_rows_per_page
        )
    };
    let summary_para = Paragraph::new(summary).wrap(Wrap { trim: true });
    f.render_widget(summary_para, chunks[0]);

    let header = format!(
        "{:>3}  {:<18} {:<32} {:<14} {:>5} {:>10} {:>10} {:>10}",
        "#", "Order ID", "Market", "Outcome", "Side", "Price", "Size", "Status"
    );
    f.render_widget(
        Paragraph::new(header)
            .style(Style::default().add_modifier(Modifier::BOLD))
            .wrap(Wrap { trim: true }),
        chunks[1],
    );

    if total == 0 {
        let empty = Paragraph::new("No open orders").wrap(Wrap { trim: true });
        f.render_widget(empty, chunks[2]);
        return;
    }

    let (start, end) = app.current_orders_slice(total);

    let mut items: Vec<ListItem> = Vec::new();
    for (offset, order) in orders[start..end].iter().enumerate() {
        let idx = start + offset;
        let id = short_id(&order.id);
        let (market_title, outcome_title) = order_market_labels(app, order);
        let market_display = trim_to_width(&market_title, 32);
        let outcome_display = if outcome_title.is_empty() {
            "-".to_string()
        } else {
            trim_to_width(&outcome_title, 14)
        };
        let side = match order.side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        };
        let status = format!("{:?}", order.status);
        let line = Line::from(vec![
            Span::raw(format!("{:>3}. ", idx + 1)),
            Span::raw(format!("{:<18}", id)),
            Span::raw(format!("{:<32}", market_display)),
            Span::raw(" "),
            Span::raw(format!("{:<14}", outcome_display)),
            Span::raw(format!("{:>5}", side)),
            Span::raw(format!("{:>10.4}", order.price)),
            Span::raw(format!("{:>10.2}", order.size)),
            Span::raw(format!("{:>10}", status)),
        ]);
        items.push(ListItem::new(line));
    }

    if items.is_empty() {
        let empty = Paragraph::new("No open orders").wrap(Wrap { trim: true });
        f.render_widget(empty, chunks[2]);
        return;
    }

    let selected_rel = app
        .orders_view_sel
        .saturating_sub(start)
        .min(items.len() - 1);
    let mut state = list_state(selected_rel, items.len());
    let list = List::new(items).highlight_style(Style::default().bg(Color::Blue).fg(Color::White));
    f.render_stateful_widget(list, chunks[2], &mut state);
}

fn draw_user_view(f: &mut ratatui::Frame, area: Rect, app: &mut AppState) {
    let block = Block::default()
        .title(Span::styled(
            "User – press /u or Esc to return",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL);
    let inner = block.inner(area);
    f.render_widget(block, area);
    if inner.width == 0 || inner.height == 0 {
        return;
    }

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(12), Constraint::Min(4)])
        .split(inner);

    let table_area = layout[1];
    match app.user_tab {
        UserTab::Positions => {
            let rows = table_area.height.saturating_sub(1) as usize;
            if rows > 0 {
                app.update_positions_rows_per_page(rows.max(1));
            }
            app.clamp_positions_state();
        }
        UserTab::Trades => {
            let rows = table_area.height.saturating_sub(1) as usize;
            if rows > 0 {
                app.update_trades_rows_per_page(rows.max(1));
            }
            app.clamp_trades_state();
        }
    }

    let mut lines: Vec<Line> = Vec::new();
    if let Some(user) = app.user_profile.as_ref() {
        lines.push(Line::from(vec![
            Span::styled("Username: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(user.username.clone()),
        ]));
        lines.push(Line::from(vec![
            Span::styled(
                "Proxy wallet: ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                user.proxy_wallet.clone(),
                Style::default().fg(Color::Yellow),
            ),
        ]));
    } else {
        lines.push(Line::from("No user information available."));
    }

    let cash = app.cash_usdce;
    let total = app.positions_value_sum + cash;
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled(
            "Cash (USDC.e): ",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("{:.2}", cash)),
    ]));
    lines.push(Line::from(vec![
        Span::styled(
            "Portfolio value: ",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("{:.2}", total)),
    ]));

    lines.push(Line::from(""));

    match app.user_tab {
        UserTab::Positions => {
            let total_positions = app.positions.len();
            let total_pages = app.positions_total_pages();
            let page_info = if total_pages > 0 {
                format!(
                    "Page {}/{} ({} per page)",
                    app.positions_page + 1,
                    total_pages,
                    app.positions_rows_per_page
                )
            } else {
                "No pagination".to_string()
            };
            lines.push(Line::from(vec![
                Span::styled("Positions: ", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(format!("{}", total_positions)),
                Span::raw("  |  "),
                Span::raw(page_info),
            ]));
            lines.push(Line::from(vec![
                Span::raw("Use ↑/↓ to move, ←/→ to change page, Enter to open, press t for activity, p for positions."),
            ]));
        }
        UserTab::Trades => {
            let total_trades = app.trade_activities.len();
            let total_pages = app.trades_total_pages();
            let page_info = if total_pages > 0 {
                format!(
                    "Page {}/{} ({} per page)",
                    app.trades_page + 1,
                    total_pages,
                    app.trades_rows_per_page
                )
            } else {
                "No pagination".to_string()
            };
            lines.push(Line::from(vec![
                Span::styled("Activity: ", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(format!("{}", total_trades)),
                Span::raw("  |  "),
                Span::raw(page_info),
            ]));
            lines.push(Line::from(vec![Span::raw(
                "Use ↑/↓ to move, ←/→ to change page, Enter to open, press p for positions, t for activity.",
            )]));
        }
    }

    lines.push(Line::from(vec![
        Span::styled("Press E", Style::default().fg(Color::Yellow)),
        Span::raw(" to export the decrypted private key."),
    ]));

    if let Some(status) = app.user_status.as_ref() {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled("Status: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(status.clone()),
        ]));
    }

    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: true });
    f.render_widget(paragraph, layout[0]);
    match app.user_tab {
        UserTab::Positions => render_positions_table(f, layout[1], app),
        UserTab::Trades => render_trades_table(f, layout[1], app),
    }
}

fn current_orders_snapshot(app: &AppState) -> Vec<OpenOrder> {
    let mut orders: Vec<OpenOrder> = app.orders_map.values().cloned().collect();
    orders.sort_by_key(|o| -(o.created_at as i64));
    orders
}

fn order_market_labels(app: &AppState, order: &OpenOrder) -> (String, String) {
    if let Some(info) = app.token_market_cache.get(&order.asset_id) {
        let market = if info.question.trim().is_empty() {
            info.slug
                .as_deref()
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .unwrap_or_else(|| fallback_order_market(order))
        } else {
            info.question.clone()
        };
        let outcome = if info.outcome.is_empty() {
            String::new()
        } else {
            info.outcome.clone()
        };
        (market, outcome)
    } else {
        (fallback_order_market(order), String::new())
    }
}

fn fallback_order_market(order: &OpenOrder) -> String {
    let raw = order.market.trim();
    if raw.is_empty() {
        short_id(&order.asset_id)
    } else {
        raw.to_string()
    }
}

fn current_market_open_orders(app: &AppState) -> Vec<OpenOrder> {
    let tokens: Vec<String> = app.current_market_tokens();
    if tokens.is_empty() {
        return vec![];
    }

    let set: HashSet<&str> = tokens.iter().map(|s| s.as_str()).collect();
    let mut v: Vec<OpenOrder> = app
        .orders_map
        .values()
        .filter(|o| {
            set.contains(o.asset_id.as_str())
                && matches!(o.status, OrderStatus::Live | OrderStatus::PendingNew)
        })
        .cloned()
        .collect();
    v.sort_by_key(|o| -(o.created_at as i64));
    v
}

fn order_outcome_name(app: &AppState, o: &OpenOrder) -> String {
    if let Some(info) = app.token_market_cache.get(&o.asset_id) {
        if !info.outcome.is_empty() {
            return info.outcome.clone();
        }
    }
    if let Some(ev) = app.current_event() {
        if let Some(m) = ev.markets.get(app.sel_market_idx) {
            if let Some(pos) = m.clobTokenIds.iter().position(|t| t == &o.asset_id) {
                if let Some(name) = m.outcomes.get(pos) {
                    return name.clone();
                }
                return format!("Outcome {}", pos + 1);
            }
        }
    }
    String::new()
}

fn draw_orders(f: &mut ratatui::Frame, area: Rect, app: &AppState) {
    let mut rows: Vec<String> = Vec::new();
    let mkt_title = app.selected_market_title().unwrap_or_else(|| "".into());
    rows.push(format!("Open Orders - {}", mkt_title));

    let mut open_orders: Vec<OpenOrder> = current_market_open_orders(app);
    open_orders.sort_by_key(|o| -(o.created_at as i64));

    let max_rows = (area.height as usize).saturating_sub(2);
    let total = open_orders.len();
    let sel = if total == 0 {
        0
    } else {
        app.sel_order_idx.min(total - 1)
    };
    let start = sel.saturating_sub(max_rows.saturating_sub(1));
    let end = (start + max_rows).min(total);

    for (idx, o) in open_orders.iter().enumerate().skip(start).take(end - start) {
        let side_str = match o.side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        };
        let matched = o.size_matched;
        let total = o.size;
        let selected = idx == sel;
        let outcome = order_outcome_name(app, o);
        let prefix = if selected { "> " } else { "  " };
        rows.push(format!(
            "{}{} {} {:.2}/{:.2} @ {:.4}",
            prefix, side_str, outcome, matched, total, o.price
        ));
    }

    let mut lines: Vec<Line> = Vec::new();
    for r in rows.into_iter() {
        lines.push(Line::from(r));
    }
    let para = Paragraph::new(lines).wrap(Wrap { trim: true });
    f.render_widget(para, area);
}

fn draw_recent_trades(f: &mut ratatui::Frame, area: Rect, app: &mut AppState) {
    let height = area.height as usize;
    app.recent_trades_visible_rows = height;
    if area.width == 0 || area.height == 0 {
        app.recent_trades_last_len = 0;
        return;
    }

    let tokens = app.current_market_tokens();
    if tokens != app.recent_trades_active_tokens {
        app.recent_trades_active_tokens = tokens.clone();
        app.recent_trades_offset = 0;
        app.recent_trades_scroll_tick = 0;
    }

    if tokens.is_empty() {
        app.recent_trades_last_len = 0;
        let msg = Paragraph::new("No market selected").wrap(Wrap { trim: true });
        f.render_widget(msg, area);
        return;
    }

    let trades = app.gather_recent_trades(&tokens);
    app.recent_trades_last_len = trades.len();
    if trades.is_empty() {
        let msg = Paragraph::new("No trades yet").wrap(Wrap { trim: true });
        f.render_widget(msg, area);
        return;
    }

    if trades.len() <= height || height == 0 {
        app.recent_trades_offset = 0;
    } else {
        app.recent_trades_offset %= trades.len();
    }

    let visible = height.min(trades.len());
    if visible == 0 {
        return;
    }

    let mut items: Vec<ListItem> = Vec::with_capacity(visible);
    for idx in 0..visible {
        let pos = (app.recent_trades_offset + idx) % trades.len();
        let entry = &trades[pos];
        items.push(ListItem::new(format_recent_trade_line(app, entry)));
    }

    let list = List::new(items);
    f.render_widget(list, area);
}

fn draw_profile(f: &mut ratatui::Frame, area: Rect, app: &AppState) {
    let cash = app.cash_usdce;
    let total = app.positions_value_sum + cash;

    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(vec![
        Span::styled("Profile ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw("(USDC.e)"),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Cash: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(format!("{:.2}", cash), Style::default().fg(Color::Yellow)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Portfolio: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(format!("{:.2}", total), Style::default().fg(Color::Cyan)),
    ]));
    let para = Paragraph::new(lines).wrap(Wrap { trim: true });
    f.render_widget(para, area);
}

fn format_recent_trade_line(app: &AppState, trade: &RecentTrade) -> Line<'static> {
    let (side_label, side_style) = match trade.side {
        Side::Buy => (
            "buy",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        Side::Sell => (
            "sell",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
    };
    let outcome = asset_outcome_label(app, &trade.asset_id);
    let qty_price = format!("{:.2}@{:.3}", trade.size, trade.price);
    let rel = format_relative_time(trade.timestamp_secs, SystemTime::now());
    Line::from(vec![
        Span::styled(side_label.to_string(), side_style),
        Span::raw(" "),
        Span::styled(outcome, Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(" "),
        Span::raw(qty_price),
        Span::raw("  "),
        Span::styled(rel, Style::default().fg(Color::DarkGray)),
    ])
}

fn asset_outcome_label(app: &AppState, asset_id: &str) -> String {
    if let Some(info) = app.token_market_cache.get(asset_id) {
        if !info.outcome.is_empty() {
            return info.outcome.clone();
        }
    }
    if let Some(ev) = app.current_event() {
        if let Some(m) = ev.markets.get(app.sel_market_idx) {
            if let Some(pos) = m.clobTokenIds.iter().position(|t| t == asset_id) {
                if let Some(name) = m.outcomes.get(pos) {
                    return name.clone();
                }
                return format!("Outcome {}", pos + 1);
            }
        }
    }
    short_id(asset_id)
}

fn now_ts_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn sanitize_filename(input: &str) -> String {
    let mut out = String::new();
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if ch == '-' {
            out.push(ch);
        } else {
            if out.chars().last().unwrap_or('_') != '_' {
                out.push('_');
            }
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "recording".into()
    } else {
        trimmed.to_string()
    }
}

fn format_ms_for_filename(ms: i64) -> String {
    if let Some(naive) = NaiveDateTime::from_timestamp_millis(ms) {
        let dt: DateTime<Utc> = DateTime::<Utc>::from_utc(naive, Utc);
        dt.format("%Y%m%d_%H%M%S").to_string()
    } else {
        ms.to_string()
    }
}

fn render_positions_table(f: &mut ratatui::Frame, area: Rect, app: &mut AppState) {
    if area.width == 0 || area.height == 0 {
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1)])
        .split(area);

    let header_line = format!(
        "{:>3}  {:<32} {:<12} {:>7} {:>9} {:>9} {:>9} {:>7}",
        "#", "Market", "Outcome", "Size", "AvgPx", "Init", "Cur", "Price"
    );
    f.render_widget(
        Paragraph::new(header_line)
            .style(Style::default().add_modifier(Modifier::BOLD))
            .wrap(Wrap { trim: true }),
        chunks[0],
    );

    let rows_available = chunks[1].height as usize;
    if rows_available == 0 {
        return;
    }
    app.update_positions_rows_per_page(rows_available.max(1));

    if app.positions.is_empty() {
        let empty = Paragraph::new("No positions").wrap(Wrap { trim: true });
        f.render_widget(empty, chunks[1]);
        return;
    }

    let (start, end) = app.current_positions_slice();
    let selected_rel = app.positions_sel.saturating_sub(start);
    let mut items: Vec<ListItem> = Vec::new();
    for (offset, pos) in app.positions[start..end].iter().enumerate() {
        let idx = start + offset;
        let title = trim_to_width(&pos.title, 32);
        let outcome = trim_to_width(&pos.outcome, 12);
        let line = Line::from(vec![
            Span::raw(format!("{:>3}. ", idx + 1)),
            Span::raw(format!("{:<32}", title)),
            Span::raw(" "),
            Span::raw(format!("{:<12}", outcome)),
            Span::raw(" "),
            Span::raw(format!("{:>7.2}", pos.size)),
            Span::raw(" "),
            Span::raw(format!("{:>9.3}", pos.avg_price.unwrap_or(0.0))),
            Span::raw(" "),
            Span::raw(format!("{:>8.2}", pos.initial_value.unwrap_or(0.0))),
            Span::raw(" "),
            Span::raw(format!("{:>8.2}", pos.current_value.unwrap_or(0.0))),
            Span::raw(" "),
            Span::raw(format!("{:>6.3}", pos.cur_price.unwrap_or(0.0))),
        ]);
        items.push(ListItem::new(line));
    }

    let mut state = ratatui::widgets::ListState::default();
    if !items.is_empty() {
        state.select(Some(selected_rel.min(items.len() - 1)));
    }
    let list = List::new(items).highlight_style(Style::default().bg(Color::Blue).fg(Color::White));
    f.render_stateful_widget(list, chunks[1], &mut state);
}

fn render_trades_table(f: &mut ratatui::Frame, area: Rect, app: &mut AppState) {
    if area.width == 0 || area.height == 0 {
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1)])
        .split(area);

    let header_line = format!(
        "{:>3}  {:<10} {:<6} {:>10} {:>10} {:>10} {:<18} {:<32}",
        "#", "Type", "Side", "Size", "USDC", "Price", "When", "Market"
    );
    f.render_widget(
        Paragraph::new(header_line)
            .style(Style::default().add_modifier(Modifier::BOLD))
            .wrap(Wrap { trim: true }),
        chunks[0],
    );

    let rows_available = chunks[1].height as usize;
    if rows_available == 0 {
        return;
    }
    app.update_trades_rows_per_page(rows_available.max(1));

    if app.trade_activities.is_empty() {
        let empty = Paragraph::new("No activity found").wrap(Wrap { trim: true });
        f.render_widget(empty, chunks[1]);
        return;
    }

    let (start, end) = app.current_trades_slice();
    let selected_rel = app.trades_sel.saturating_sub(start);
    let now = SystemTime::now();
    let mut items: Vec<ListItem> = Vec::new();
    for (offset, entry) in app.trade_activities[start..end].iter().enumerate() {
        let idx = start + offset + 1;
        let ty = entry.ty_label();
        let side = entry.side_label();
        let size = format!("{:.2}", entry.size());
        let usdc = format!("{:.2}", entry.usdc_size());
        let price = entry
            .price()
            .map(|p| format!("{:.4}", p))
            .unwrap_or_else(|| "-".into());
        let time_str = format_relative_time(entry.timestamp, now);
        let title = trim_to_width(&entry.title, 32);
        let line = Line::from(vec![
            Span::raw(format!("{:>3}. ", idx)),
            Span::raw(format!("{:<10}", ty)),
            Span::raw(" "),
            Span::raw(format!("{:<6}", side)),
            Span::raw(" "),
            Span::raw(format!("{:>10}", size)),
            Span::raw(" "),
            Span::raw(format!("{:>10}", usdc)),
            Span::raw(" "),
            Span::raw(format!("{:>10}", price)),
            Span::raw(" "),
            Span::raw(format!("{:<18}", time_str)),
            Span::raw(" "),
            Span::raw(format!("{:<32}", title)),
        ]);
        items.push(ListItem::new(line));
    }

    let mut state = ratatui::widgets::ListState::default();
    if !items.is_empty() {
        state.select(Some(selected_rel.min(items.len() - 1)));
    }
    let list = List::new(items).highlight_style(Style::default().bg(Color::Blue).fg(Color::White));
    f.render_stateful_widget(list, chunks[1], &mut state);
}

fn export_private_key(app: &mut AppState) -> Result<()> {
    let user = app
        .user_profile
        .as_ref()
        .ok_or_else(|| anyhow!("user profile unavailable"))?;
    let path = Path::new("private_key_export.txt");
    fs::write(path, format!("{}\n", user.private_key))?;
    app.user_status = Some(format!("Private key exported to {}", path.display()));
    Ok(())
}

fn outer_block(focused: bool) -> Block<'static> {
    let mut blk = Block::default().borders(Borders::ALL);
    if focused {
        blk = blk
            .border_type(BorderType::Thick)
            .border_style(Style::default().fg(Color::Cyan));
    }
    blk
}

fn fit_text_variants(max_width: u16, variants: &[&str]) -> String {
    let w = max_width as usize;
    for v in variants.iter() {
        if v.chars().count() <= w {
            return (*v).to_string();
        }
    }
    // fallback: hard cut
    let s = variants.last().copied().unwrap_or("");
    trim_to_width(s, w)
}

fn trim_to_width(s: &str, w: usize) -> String {
    if s.chars().count() <= w {
        return s.to_string();
    }
    if w == 0 {
        return String::new();
    }
    if w <= 3 {
        return ".".repeat(w);
    }
    let mut out = String::new();
    for ch in s.chars().take(w - 3) {
        out.push(ch);
    }
    out.push_str("...");
    out
}

fn format_relative_time(timestamp: i64, now: SystemTime) -> String {
    if timestamp <= 0 {
        return "unknown".to_string();
    }
    let ts = if timestamp < 0 { 0 } else { timestamp as u64 };
    let event_time = UNIX_EPOCH + Duration::from_secs(ts);
    let diff = match now.duration_since(event_time) {
        Ok(d) => d,
        Err(_) => Duration::from_secs(0),
    };
    let secs = diff.as_secs();
    if secs < 60 {
        if secs == 1 {
            return "1 second ago".to_string();
        }
        return format!("{} seconds ago", secs);
    }
    if secs < 3600 {
        let minutes = secs / 60;
        if minutes == 1 {
            return "1 minute ago".to_string();
        }
        return format!("{} minutes ago", minutes);
    }
    if secs < 86_400 {
        let hours = secs / 3600;
        let minutes = (secs % 3600) / 60;
        if hours == 1 && minutes == 0 {
            return "1 hour ago".to_string();
        }
        if minutes > 0 {
            return format!("{}h {}m ago", hours, minutes);
        }
        return format!("{} hours ago", hours);
    }
    let days = secs / 86_400;
    let hours = (secs % 86_400) / 3600;
    if hours > 0 {
        return format!("{}d {}h ago", days, hours);
    } else {
        if days == 1 {
            return "1 day ago".to_string();
        }
        return format!("{} days ago", days);
    }
}

fn write_line_with_style(buf: &mut Buffer, x: u16, y: u16, width: usize, text: &str, style: Style) {
    let mut chars = text.chars();
    for i in 0..width {
        let symbol = chars.next().unwrap_or(' ');
        let mut char_buf = [0u8; 4];
        let rendered = symbol.encode_utf8(&mut char_buf);
        let cell = buf.get_mut(x + i as u16, y);
        cell.set_symbol(rendered);
        cell.set_style(style);
    }
}

fn compose_search_title(width: u16) -> String {
    let variants = [
        "Search Events (Enter to search, Up/Down to select, Right for details)",
        "Search (Enter search, Up/Down select, Right for details)",
        "Search (Enter, Up/Down, Right)",
        "Search",
    ];
    fit_text_variants(width.saturating_sub(2), &variants)
}

fn compose_event_hint(width: u16) -> String {
    let variants = [
        "Markets (Up/Down select, Enter/Right view book, 'o' flip outcome)",
        "Markets (Up/Down, Enter/Right, 'o')",
        "Markets",
    ];
    fit_text_variants(width, &variants)
}

fn compose_event_title(width: u16, title: &str) -> String {
    trim_to_width(title, width.saturating_sub(2) as usize)
}

fn compose_book_title(width: u16, name: &str, outcome_idx: usize) -> String {
    let base = format!("Order Book - {} (out: {})", name, outcome_idx + 1);
    trim_to_width(&base, width.saturating_sub(2) as usize)
}

fn short_id(id: &str) -> String {
    if id.len() <= 12 {
        return id.to_string();
    }
    let head = &id[..6];
    let tail = &id[id.len().saturating_sub(4)..];
    format!("{}...{}", head, tail)
}

use anyhow::Result;
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures::StreamExt;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Row, Table, Wrap},
    Frame, Terminal,
};
use std::io::{self};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use engine::{model::*, ws_market_multi::MarketMultiWs, MarketStream};
mod tui;
use tui::OrderBookState;

#[tokio::main]
async fn main() -> Result<()> {
    // 读取命令行参数：资产ID
    let asset_id = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: ob_tui <asset_id>");
        std::process::exit(1);
    });

    // 启动 Market WS 订阅（使用现有引擎组件）
    let market = MarketMultiWs::new("wss://ws-subscriptions-clob.polymarket.com/ws/market", 500)?;
    market.subscribe(vec![asset_id.clone()]).await?;

    // 渲染通信通道
    let (tx, mut rx) = mpsc::channel::<MarketEvent>(1000);
    let market_clone = market.clone();
    tokio::spawn(async move {
        loop {
            match market_clone.next().await {
                Ok(Some(ev)) => {
                    let _ = tx.send(ev).await;
                }
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    });

    // 初始化 TUI 终端
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let mut ob = OrderBookState::new(asset_id.clone());
    // 事件流：键盘（异步）
    let mut keys = crossterm::event::EventStream::new();

    // 主循环：事件驱动渲染（收到市场或键盘事件时即时刷新）
    loop {
        tokio::select! {
            // 市场事件：更新状态并立即重绘
            Some(ev) = rx.recv() => {
                match ev {
                    MarketEvent::OrderBook(book) => {
                        if book.asset_id == ob.asset_id {
                            let bids: Vec<(f64,f64)> = book.bids.into_iter().map(|l| (l.price, l.size)).collect();
                            let asks: Vec<(f64,f64)> = book.asks.into_iter().map(|l| (l.price, l.size)).collect();
                            ob.apply_snapshot(&bids, &asks);
                            terminal.draw(|f| draw_ui(f, &ob))?;
                        }
                    }
                    MarketEvent::DepthUpdate(upd) => {
                        let mut touched = false;
                        for ch in upd.price_changes.into_iter() {
                            if ch.asset_id == ob.asset_id {
                                let is_bid = matches!(ch.side, Side::Buy);
                                ob.apply_delta(ch.price, ch.size, is_bid);
                                touched = true;
                            }
                        }
                        if touched { terminal.draw(|f| draw_ui(f, &ob))?; }
                    }
                    _ => {}
                }
            }
            // 键盘事件：q/Esc 退出
            Some(Ok(evt)) = keys.next() => {
                if let Event::Key(KeyEvent{ code, .. }) = evt {
                    match code {
                        KeyCode::Char('q') | KeyCode::Esc => { cleanup_terminal()?; return Ok(()); }
                        _ => {}
                    }
                }
            }
        }
    }
}

fn cleanup_terminal() -> Result<()> {
    disable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, LeaveAlternateScreen)?;
    Ok(())
}

fn draw_ui(f: &mut Frame, ob: &OrderBookState) {
    let size = f.size();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(5)].as_ref())
        .split(size);

    // Header: Asset, BBA, Spread
    let spread = if ob.best_ask.is_finite() && ob.best_ask > 0.0 {
        ob.best_ask - ob.best_bid
    } else {
        0.0
    };
    let header = vec![
        Line::from(vec![
            Span::styled(
                "Order Book ",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(&ob.asset_id),
        ]),
        Line::from(vec![
            Span::styled("Best Bid: ", Style::default().fg(Color::Green)),
            Span::raw(format!("{:.4}", ob.best_bid)),
            Span::raw("    "),
            Span::styled("Best Ask: ", Style::default().fg(Color::Red)),
            Span::raw(format!(
                "{:.4}",
                if ob.best_ask.is_finite() {
                    ob.best_ask
                } else {
                    0.0
                }
            )),
        ]),
        Line::from(vec![
            Span::styled("Spread: ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:.4}", spread)),
        ]),
    ];
    let header_block = Paragraph::new(header)
        .block(
            Block::default()
                .title(Span::styled(
                    "BLOOM-OB",
                    Style::default()
                        .fg(Color::Magenta)
                        .add_modifier(Modifier::BOLD),
                ))
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: true });
    f.render_widget(header_block, chunks[0]);

    // Body: two columns table-like
    let mid = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
        .split(chunks[1]);

    let max_rows = ((chunks[1].height as usize).saturating_sub(5)).max(10);
    let bid_rows: Vec<Row> = ob
        .bids
        .levels
        .iter()
        .take(max_rows)
        .map(|(p, s)| {
            Row::new(vec![format!("{:.4}", p), format!("{:.2}", s)])
                .style(Style::default().fg(Color::Green))
        })
        .collect();
    let ask_rows: Vec<Row> = ob
        .asks
        .levels
        .iter()
        .take(max_rows)
        .map(|(p, s)| {
            Row::new(vec![format!("{:.4}", p), format!("{:.2}", s)])
                .style(Style::default().fg(Color::Red))
        })
        .collect();

    let widths = [Constraint::Percentage(50), Constraint::Percentage(50)];
    let bids_tbl = Table::new(bid_rows, widths)
        .header(
            Row::new(vec!["Bid Px", "Size"]).style(
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
        )
        .block(Block::default().title("Bids").borders(Borders::ALL));
    let asks_tbl = Table::new(ask_rows, widths)
        .header(
            Row::new(vec!["Ask Px", "Size"])
                .style(Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
        )
        .block(Block::default().title("Asks").borders(Borders::ALL));

    f.render_widget(bids_tbl, mid[0]);
    f.render_widget(asks_tbl, mid[1]);
}

use anyhow::Result;
use std::io::{self, Write};

mod search;
use search::search_events;

#[tokio::main]
async fn main() -> Result<()> {
    let query = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: search_demo <query>");
        std::process::exit(1);
    });
    let resp = search_events(&query, 10).await?;
    if resp.events.is_empty() {
        println!("No events found for query: {}", query);
        return Ok(());
    }
    println!("Found {} events:\n", resp.events.len());
    for (i, ev) in resp.events.iter().enumerate() {
        println!(
            "{:>2}. {}  (slug: {})  markets: {}",
            i + 1,
            ev.title,
            ev.slug,
            ev.markets.len()
        );
    }
    print!("\nSelect an event index to view its markets: ");
    io::stdout().flush().ok();
    let mut input = String::new();
    io::stdin().read_line(&mut input).ok();
    let idx: usize = input.trim().parse().unwrap_or(1);
    let idx0 = idx.saturating_sub(1).min(resp.events.len() - 1);
    let ev = &resp.events[idx0];
    println!("\nEvent: {}", ev.title);
    if ev.markets.is_empty() {
        println!("No markets under this event.");
    } else {
        for (j, m) in ev.markets.iter().enumerate() {
            println!("{:>2}. {}  (slug: {})", j + 1, m.question, m.slug);
        }
    }
    Ok(())
}

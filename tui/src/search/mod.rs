use anyhow::{Context, Result};
use serde::Deserialize;
pub mod detail;

#[derive(Debug, Deserialize, Clone)]
#[allow(non_snake_case)]
#[allow(dead_code)]
pub struct SearchResponse {
    #[serde(default)]
    pub events: Vec<EventItem>,
    #[serde(default)]
    pub hasMore: bool,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(non_snake_case)]
#[allow(dead_code)]
pub struct EventItem {
    pub id: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub slug: String,
    #[serde(default)]
    pub closed: bool,
    #[serde(default)]
    pub negRisk: bool,
    #[serde(default)]
    pub markets: Vec<MarketItem>,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(non_snake_case)]
#[allow(dead_code)]
pub struct MarketItem {
    #[serde(default)]
    pub question: String,
    #[serde(default)]
    pub slug: String,
    #[serde(default)]
    pub outcomes: Vec<String>,
    #[serde(default)]
    pub outcomePrices: Vec<String>,
    #[serde(default)]
    pub bestAsk: Option<f64>,
    #[serde(default)]
    pub bestBid: Option<f64>,
}

fn build_url(query: &str, limit: u32) -> Result<url::Url> {
    let mut url = url::Url::parse("https://gamma-api.polymarket.com/public-search")?;
    url.query_pairs_mut()
        .append_pair("q", query)
        .append_pair("optimized", "true")
        .append_pair("limit_per_type", &limit.to_string())
        .append_pair("type", "events")
        .append_pair("search_tags", "true")
        .append_pair("search_profiles", "false")
        .append_pair("cache", "false");
    Ok(url)
}

pub async fn search_events(query: &str, limit_per_type: u32) -> Result<SearchResponse> {
    let url = build_url(query, limit_per_type)?;
    let client = reqwest::Client::builder()
        .user_agent("polyrust-search/0.1 (+https://polymarket.com)")
        .gzip(true)
        .brotli(true)
        .deflate(true)
        .build()
        .context("build reqwest client for gamma-api")?;
    let resp = client
        .get(url)
        .send()
        .await
        .context("send search request")?;
    let status = resp.status();
    let body = resp.bytes().await.context("read search body")?;
    if !status.is_success() {
        return Err(anyhow::anyhow!(
            "search http {}: {}",
            status.as_u16(),
            String::from_utf8_lossy(&body)
        ));
    }
    let mut parsed: SearchResponse = serde_json::from_slice(&body).context("parse search json")?;
    parsed.events.retain(|e| !e.closed);
    Ok(parsed)
}

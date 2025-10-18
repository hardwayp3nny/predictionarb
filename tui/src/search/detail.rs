use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;

fn de_vec_string_from_str_or_vec<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // Accept either a JSON array of strings, or a JSON-encoded string containing such an array
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Helper {
        Vec(Vec<String>),
        Str(String),
        Null,
    }
    let helper = Helper::deserialize(deserializer)?;
    Ok(match helper {
        Helper::Vec(v) => v,
        Helper::Str(s) => serde_json::from_str::<Vec<String>>(&s).unwrap_or_default(),
        Helper::Null => Vec::new(),
    })
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
#[allow(dead_code)]
pub struct EventDetail {
    pub id: String,
    pub slug: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub closed: bool,
    #[serde(default)]
    pub negRisk: bool,
    #[serde(default)]
    pub markets: Vec<EventMarket>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
#[allow(dead_code)]
pub struct EventMarket {
    pub id: String,
    #[serde(default)]
    pub question: String,
    #[serde(default)]
    pub slug: String,
    #[serde(default, deserialize_with = "de_vec_string_from_str_or_vec")]
    pub outcomes: Vec<String>,
    #[serde(default, deserialize_with = "de_vec_string_from_str_or_vec")]
    pub clobTokenIds: Vec<String>,
    #[serde(default)]
    pub closed: bool,
    #[serde(default)]
    pub bestAsk: Option<f64>,
    #[serde(default)]
    pub bestBid: Option<f64>,
}

pub async fn fetch_event_detail(slug: &str, id_opt: Option<&str>) -> Result<EventDetail> {
    // First try the straightforward URL
    let url1 = format!("https://gamma-api.polymarket.com/events/slug/{}", slug);
    let client = reqwest::Client::builder()
        .user_agent("polyrust-search/0.1 (+https://polymarket.com)")
        .gzip(true)
        .brotli(true)
        .deflate(true)
        .build()
        .context("build reqwest client for gamma-api detail")?;
    if let Ok(ok) = fetch_once(&client, &url1).await {
        return Ok(ok);
    }
    let url2 = format!(
        "https://gamma-api.polymarket.com/events/slug/{}",
        urlencoding::encode(slug)
    );
    if let Ok(ok) = fetch_once(&client, &url2).await {
        return Ok(ok);
    }
    if let Some(id) = id_opt {
        let url3 = format!("https://gamma-api.polymarket.com/events/{}", id);
        if let Ok(ok) = fetch_once(&client, &url3).await {
            return Ok(ok);
        }
    }
    Err(anyhow::anyhow!("cannot fetch event detail by slug or id"))
}

async fn fetch_once(client: &Client, u: &str) -> Result<EventDetail> {
    let resp = client
        .get(u)
        .send()
        .await
        .context("send event detail request")?;
    let status = resp.status();
    let body = resp.bytes().await.context("read event detail body")?;
    if !status.is_success() {
        return Err(anyhow::anyhow!(
            "event detail http {}: {}",
            status.as_u16(),
            String::from_utf8_lossy(&body)
        ));
    }
    let parsed: EventDetail = serde_json::from_slice(&body).context("parse event detail json")?;
    Ok(parsed)
}

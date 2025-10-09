use anyhow::Result;
use engine::{auth::ApiCreds, config::EngineConfig, ws_user2::UserWsV2, UserStream};
use std::{env, fs::OpenOptions};
use tokio::time::{sleep, Duration};

const DEFAULT_API_KEY: &str = "";
const DEFAULT_API_SECRET: &str = "";
const DEFAULT_API_PASSPHRASE: &str = "";

#[tokio::main]
async fn main() -> Result<()> {
    let log_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open("demo.log")?;

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .with_writer(move || log_file.try_clone().expect("failed to clone log file"))
        .init();

    let mut args = env::args().skip(1);
    let api_key = args.next().unwrap_or_else(|| DEFAULT_API_KEY.to_string());
    let api_secret = args
        .next()
        .unwrap_or_else(|| DEFAULT_API_SECRET.to_string());
    let api_passphrase = args
        .next()
        .unwrap_or_else(|| DEFAULT_API_PASSPHRASE.to_string());

    let default_url = EngineConfig::default().ws.user_ws_url_v2;
    let url = args.next().unwrap_or(default_url);

    let creds = ApiCreds {
        api_key,
        secret: api_secret,
        passphrase: api_passphrase,
    };

    tracing::info!(target: "ws_user2_demo", url = %url, "connecting via UserWsV2");
    let stream = UserWsV2::new(&url, creds);
    stream.connect().await?;

    loop {
        match stream.next().await? {
            Some(event) => {
                tracing::info!(target: "ws_user2_demo", event = ?event, "received user event");
            }
            None => {
                tracing::warn!(target: "ws_user2_demo", "stream returned None; sleeping before retry");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

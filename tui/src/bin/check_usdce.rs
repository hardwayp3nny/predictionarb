use anyhow::{anyhow, Result};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let rpc = env::var("POLYGON_RPC_URL").unwrap_or_else(|_| "https://rpc.polygon.com".to_string());
    let addr = env::var("WALLET_ADDRESS")
        .or_else(|_| env::var("SAFE_ADDRESS"))
        .or_else(|_| env::var("BROWSER_ADDRESS"))
        .map_err(|_| anyhow!("WALLET_ADDRESS/SAFE_ADDRESS/BROWSER_ADDRESS not set"))?;
    let bal = engine::oms::fetch_usdce_balance(&rpc, &addr).await?;
    println!("{:.6}", bal);
    Ok(())
}

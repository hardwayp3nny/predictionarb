use std::io::{self, Write};
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::hex;
use alloy_signer_local::PrivateKeySigner;
use engine::eip712_sign::parse_pk;
use eyre::{eyre, Result, WrapErr};
use indicatif::{ProgressBar, ProgressStyle};
use rand_core::{OsRng, RngCore};

use crate::errors::custom::CustomError;
use crate::local_store::{
    load_credentials, persist_credentials, PersistCredentials, StoredCredentials,
};
use crate::registration::{
    approve_trading_allowances, authenticate, create_or_derive_api_key, ensure_profile,
    ensure_proxy_wallet_active, set_username, AuthContext, ProfileContext,
};

#[derive(Debug, Clone)]
pub struct OnboardingOutput {
    pub private_key: String,
    pub proxy_wallet: String,
    pub username: String,
    pub chain_id: u64,
    pub polygon_rpc_url: String,
    pub config_secret: String,
}

const DEFAULT_CHAIN_ID: u64 = 137;
const DEFAULT_POLYGON_RPC_URL: &str = "https://polygon-rpc.com";

pub async fn run() -> Result<OnboardingOutput> {
    if let Some(stored) = load_credentials().await? {
        if let (Some(proxy_wallet), Some(username)) =
            (stored.proxy_wallet.as_ref(), stored.username.as_ref())
        {
            return Ok(OnboardingOutput {
                private_key: stored.private_key,
                proxy_wallet: proxy_wallet.clone(),
                username: username.clone(),
                chain_id: DEFAULT_CHAIN_ID,
                polygon_rpc_url: DEFAULT_POLYGON_RPC_URL.to_string(),
                config_secret: stored.config_secret,
            });
        }
        return interactive(Some(stored)).await;
    }
    interactive(None).await
}

async fn interactive(existing: Option<StoredCredentials>) -> Result<OnboardingOutput> {
    println!("Polymarket TUI setup");
    println!("====================\n");

    let config_secret = existing
        .as_ref()
        .map(|s| s.config_secret.clone())
        .unwrap_or_else(|| "polyhedron123".to_string());

    let private_key = if let Some(stored) = existing.as_ref() {
        println!("Found stored private key. Reusing existing key.");
        stored.private_key.clone()
    } else {
        prompt_private_key()?
    };

    let chain_id = DEFAULT_CHAIN_ID;
    let polygon_rpc_url = DEFAULT_POLYGON_RPC_URL.to_string();

    let proxy = None;

    let signer = Arc::new(parse_private_key(&private_key)?);

    let spinner = funnel_spinner("Authenticating with Polymarket");
    let mut auth = authenticate(signer.clone(), proxy).await?;
    spinner.finish_with_message("Authenticated");

    let spinner = funnel_spinner("Fetching profile");
    let mut profile = ensure_profile(&mut auth).await?;
    spinner.finish_with_message("Profile loaded");

    let username = resolve_username(&mut auth, &mut profile).await?;

    let spinner = funnel_spinner("Ensuring proxy wallet is active");
    if let Some(tx_hash) =
        ensure_proxy_wallet_active(&mut auth, profile.proxy_wallet, &polygon_rpc_url).await?
    {
        spinner.finish_with_message(format!(
            "Proxy wallet activated ({})",
            shorten_hash(&tx_hash)
        ));
    } else {
        spinner.finish_with_message("Proxy wallet active");
    }

    if existing.is_none() {
        let spinner = funnel_spinner("Checking trading allowances");
        match approve_trading_allowances(&mut auth).await? {
            Some(tx_hash) => spinner
                .finish_with_message(format!("Allowances confirmed ({})", shorten_hash(&tx_hash))),
            None => spinner.finish_with_message("Allowances already granted"),
        }
    } else {
        let spinner = funnel_spinner("Checking trading allowances");
        spinner.finish_with_message("Skipping allowance check (existing account)");
    }

    let spinner = funnel_spinner("Preparing API credentials");
    create_or_derive_api_key(signer.clone(), auth.proxy_ref()).await?;
    spinner.finish_with_message("API credentials ready");

    persist_credentials(&PersistCredentials {
        private_key: &private_key,
        proxy_wallet: Some(&profile.proxy_wallet.to_string()),
        username: Some(&username),
        chain_id: Some(chain_id),
        polygon_rpc_url: Some(&polygon_rpc_url),
        secret: Some(&config_secret),
    })
    .await?;

    Ok(OnboardingOutput {
        private_key,
        proxy_wallet: profile.proxy_wallet.to_string(),
        username,
        chain_id,
        polygon_rpc_url,
        config_secret,
    })
}

fn prompt_private_key() -> Result<String> {
    loop {
        println!("Choose private key option:");
        println!("  1) Enter existing private key");
        println!("  2) Generate random private key");
        print!("Selection [1/2]: ");
        io::stdout().flush().ok();
        let mut selection = String::new();
        io::stdin().read_line(&mut selection)?;
        match selection.trim() {
            "1" => {
                let input =
                    rpassword::prompt_password("Enter private key (hex, with or without 0x): ")?;
                let cleaned = input.trim();
                if cleaned.is_empty() {
                    println!("Private key cannot be empty. Try again.\n");
                    continue;
                }
                return Ok(normalize_hex(cleaned));
            }
            "2" => {
                let mut bytes = [0u8; 32];
                OsRng.fill_bytes(&mut bytes);
                return Ok(hex::encode_prefixed(bytes));
            }
            _ => {
                println!("Invalid selection. Please choose 1 or 2.\n");
            }
        }
    }
}

async fn resolve_username(auth: &mut AuthContext, profile: &mut ProfileContext) -> Result<String> {
    if let Some(existing) = profile.username.clone() {
        print!(
            "Existing username detected ({}). Keep it? [Y/n]: ",
            existing
        );
        io::stdout().flush().ok();
        let mut buf = String::new();
        io::stdin().read_line(&mut buf)?;
        if !matches!(buf.trim(), "n" | "N" | "no" | "No") {
            return Ok(existing);
        }
    }

    loop {
        let username = prompt_non_empty("Enter desired username: ")?;
        match set_username(auth, profile, &username).await {
            Ok(_) => return Ok(username),
            Err(err) => {
                if let Some(custom) = err.downcast_ref::<CustomError>() {
                    if let CustomError::HttpStatusError { text, .. } = custom {
                        println!("Username \"{}\" rejected: {}", username, text);
                    } else {
                        println!("Username \"{}\" rejected: {}", username, custom);
                    }
                } else {
                    println!("Username \"{}\" rejected: {}", username, err);
                }
                println!("Please enter a different username.\n");
                continue;
            }
        }
    }
}

fn prompt_non_empty(prompt: &str) -> Result<String> {
    loop {
        print!("{}", prompt);
        io::stdout().flush().ok();
        let mut buf = String::new();
        io::stdin().read_line(&mut buf)?;
        let trimmed = buf.trim();
        if trimmed.is_empty() {
            println!("Value cannot be empty. Try again.\n");
            continue;
        }
        return Ok(trimmed.to_string());
    }
}

fn funnel_spinner(message: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    let style = ProgressStyle::default_spinner()
        .tick_strings(&["v", ">", "<", "^"])
        .template("{spinner} {msg}")
        .unwrap_or_else(|_| ProgressStyle::default_spinner());
    pb.set_style(style);
    pb.set_message(message.to_string());
    pb.enable_steady_tick(Duration::from_millis(120));
    pb
}

fn shorten_hash(hash: &str) -> String {
    if hash.len() <= 10 {
        hash.to_string()
    } else {
        format!("{}…{}", &hash[..6], &hash[hash.len() - 4..])
    }
}

fn normalize_hex(input: &str) -> String {
    if input.starts_with("0x") || input.starts_with("0X") {
        input.to_lowercase()
    } else {
        format!("0x{}", input)
    }
}

fn parse_private_key(pk: &str) -> Result<PrivateKeySigner> {
    parse_pk(pk).map_err(|e| eyre!(e))
}

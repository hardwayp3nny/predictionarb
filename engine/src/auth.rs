use crate::eip712_sign::sign_clob_auth_message;
use crate::http_pool::HttpPool;
use alloy_primitives::U256;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiCreds {
    #[serde(rename = "apiKey")]
    pub api_key: String,
    pub secret: String,
    pub passphrase: String,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn header_insert(map: &mut HeaderMap, key: &str, val: &str) {
    if let (Ok(name), Ok(value)) = (
        HeaderName::from_bytes(key.as_bytes()),
        HeaderValue::from_str(val),
    ) {
        map.insert(name, value);
    }
}

pub fn create_l1_headers(signer: &PrivateKeySigner, nonce: Option<U256>) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    let ts = now_secs().to_string();
    let nonce = nonce.unwrap_or(U256::ZERO);
    let sig = sign_clob_auth_message(signer, ts.clone(), nonce)?;
    let addr = format!("0x{:x}", signer.address());

    header_insert(&mut headers, "poly_address", &addr);
    header_insert(&mut headers, "poly_signature", &sig);
    header_insert(&mut headers, "poly_timestamp", &ts);
    header_insert(&mut headers, "poly_nonce", &nonce.to_string());
    Ok(headers)
}

pub async fn create_api_key(
    pool: &HttpPool,
    signer: &PrivateKeySigner,
    nonce: Option<U256>,
) -> Result<ApiCreds> {
    let headers = create_l1_headers(signer, nonce)?;
    let resp = pool.post("/auth/api-key", Some(headers), None).await?;
    let js = resp
        .json
        .ok_or_else(|| anyhow!("no json from create_api_key"))?;
    let api_key = js
        .get("apiKey")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing apiKey"))?
        .to_string();
    let secret = js
        .get("secret")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing secret"))?
        .to_string();
    let passphrase = js
        .get("passphrase")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing passphrase"))?
        .to_string();
    Ok(ApiCreds {
        api_key,
        secret,
        passphrase,
    })
}

pub async fn derive_api_key(
    pool: &HttpPool,
    signer: &PrivateKeySigner,
    nonce: Option<U256>,
) -> Result<ApiCreds> {
    let headers = create_l1_headers(signer, nonce)?;
    let resp = pool.get("/auth/derive-api-key", Some(headers)).await?;
    let js = resp
        .json
        .ok_or_else(|| anyhow!("no json from derive_api_key"))?;
    let api_key = js
        .get("apiKey")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing apiKey"))?
        .to_string();
    let secret = js
        .get("secret")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing secret"))?
        .to_string();
    let passphrase = js
        .get("passphrase")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing passphrase"))?
        .to_string();
    Ok(ApiCreds {
        api_key,
        secret,
        passphrase,
    })
}

pub async fn create_or_derive_api_creds(
    pool: &HttpPool,
    signer: &PrivateKeySigner,
    nonce: Option<U256>,
) -> Result<ApiCreds> {
    match create_api_key(pool, signer, nonce).await {
        Ok(c) => Ok(c),
        Err(_) => derive_api_key(pool, signer, nonce).await,
    }
}

pub fn create_l2_headers(
    signer: &PrivateKeySigner,
    creds: &ApiCreds,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> Result<HeaderMap> {
    let ts = now_secs().to_string();
    let mut message = String::new();
    message.push_str(&ts);
    message.push_str(&method.to_uppercase());
    message.push_str(path);
    if let Some(b) = body {
        let s = serde_json::to_string(b)?;
        // 与 Python 对齐：确保使用双引号的 JSON
        message.push_str(&s);
    }
    let mut headers = HeaderMap::new();
    // secret 是 URL-safe base64，需要先解码
    let secret_bytes = URL_SAFE
        .decode(creds.secret.as_bytes())
        .unwrap_or_else(|_| creds.secret.as_bytes().to_vec());
    let mut mac = Hmac::<Sha256>::new_from_slice(&secret_bytes)?;
    mac.update(message.as_bytes());
    let sig = URL_SAFE.encode(mac.finalize().into_bytes());

    let addr = format!("0x{:x}", signer.address());
    header_insert(&mut headers, "poly_address", &addr);
    header_insert(&mut headers, "poly_signature", &sig);
    header_insert(&mut headers, "poly_timestamp", &ts);
    header_insert(&mut headers, "poly_api_key", &creds.api_key);
    header_insert(&mut headers, "poly_passphrase", &creds.passphrase);
    Ok(headers)
}

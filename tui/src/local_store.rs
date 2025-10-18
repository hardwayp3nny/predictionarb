use std::{env, path::Path, path::PathBuf};

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use chacha20poly1305::{aead::Aead, ChaCha20Poly1305, Key, KeyInit, Nonce};
use eyre::{eyre, Context, Result};
use rand_core::{OsRng, RngCore};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::fs;

const ENC_PREFIX: &str = "enc:";
const DEFAULT_SECRET: &str = "polyhedron123";

#[derive(Debug, Clone)]
pub struct StoredCredentials {
    pub private_key: String,
    pub proxy_wallet: Option<String>,
    pub username: Option<String>,
    pub chain_id: Option<u64>,
    pub polygon_rpc_url: Option<String>,
    pub config_secret: String,
}

#[derive(Debug)]
pub struct PersistCredentials<'a> {
    pub private_key: &'a str,
    pub proxy_wallet: Option<&'a str>,
    pub username: Option<&'a str>,
    pub chain_id: Option<u64>,
    pub polygon_rpc_url: Option<&'a str>,
    pub secret: Option<&'a str>,
}

pub fn config_path() -> PathBuf {
    PathBuf::from("config.json")
}

pub async fn load_credentials() -> Result<Option<StoredCredentials>> {
    let path = config_path();
    if !path.exists() {
        return Ok(None);
    }

    let raw = fs::read(&path)
        .await
        .with_context(|| format!("read config file: {}", path.display()))?;
    let cfg: Value = serde_json::from_slice(&raw).context("parse config json")?;

    let secret = resolve_secret(&cfg);

    let client_cfg = cfg
        .get("client_config")
        .and_then(Value::as_object)
        .ok_or_else(|| eyre!("missing client_config section"))?;

    let pk_value = client_cfg
        .get("private_key")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("missing client_config.private_key"))?;

    let private_key = if let Some(rest) = pk_value.strip_prefix(ENC_PREFIX) {
        decrypt_private_key(&secret, rest)?
    } else {
        pk_value.to_string()
    };

    let proxy_wallet = client_cfg
        .get("proxy_wallet")
        .and_then(Value::as_str)
        .map(|s| s.to_string());
    let username = client_cfg
        .get("username")
        .and_then(Value::as_str)
        .map(|s| s.to_string());
    let chain_id = client_cfg.get("chain_id").and_then(|v| v.as_u64());
    let polygon_rpc_url = client_cfg
        .get("polygon_rpc_url")
        .and_then(Value::as_str)
        .map(|s| s.to_string());

    Ok(Some(StoredCredentials {
        private_key,
        proxy_wallet,
        username,
        chain_id,
        polygon_rpc_url,
        config_secret: secret,
    }))
}

pub async fn persist_credentials(data: &PersistCredentials<'_>) -> Result<()> {
    let path = config_path();
    let mut root: Value = if path.exists() {
        let raw = fs::read(&path)
            .await
            .with_context(|| format!("read config file: {}", path.display()))?;
        serde_json::from_slice(&raw).context("parse config json")?
    } else {
        json!({})
    };

    let secret = data
        .secret
        .map(|s| s.to_string())
        .or_else(|| {
            root.get("config_secret")
                .and_then(Value::as_str)
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| DEFAULT_SECRET.to_string());

    if !matches!(root.get("config_secret"), Some(Value::String(_))) {
        root["config_secret"] = Value::String(secret.clone());
    } else {
        root["config_secret"] = Value::String(secret.clone());
    }

    let client_cfg = root
        .as_object_mut()
        .ok_or_else(|| eyre!("config root must be an object"))?
        .entry("client_config")
        .or_insert_with(|| Value::Object(Default::default()));

    let client_obj = client_cfg
        .as_object_mut()
        .ok_or_else(|| eyre!("client_config must be an object"))?;

    let enc_pk = encrypt_private_key(&secret, data.private_key)?;
    client_obj.insert(
        "private_key".to_string(),
        Value::String(format!("{}{}", ENC_PREFIX, enc_pk)),
    );

    if let Some(proxy) = data.proxy_wallet {
        client_obj.insert("proxy_wallet".to_string(), Value::String(proxy.to_string()));
    }
    if let Some(name) = data.username {
        client_obj.insert("username".to_string(), Value::String(name.to_string()));
    }
    if let Some(chain_id) = data.chain_id {
        client_obj.insert("chain_id".to_string(), Value::from(chain_id));
    }
    if let Some(rpc) = data.polygon_rpc_url {
        client_obj.insert(
            "polygon_rpc_url".to_string(),
            Value::String(rpc.to_string()),
        );
    }

    let mut json_bytes = serde_json::to_vec_pretty(&root).context("serialize config")?;
    json_bytes.push(b'\n');
    let tmp_path = temp_path(&path);
    fs::write(&tmp_path, &json_bytes)
        .await
        .with_context(|| format!("write temp config: {}", tmp_path.display()))?;
    fs::rename(&tmp_path, &path)
        .await
        .with_context(|| format!("replace config: {}", path.display()))?;

    Ok(())
}

fn resolve_secret(cfg: &Value) -> String {
    if let Ok(secret) = env::var("CONFIG_SECRET") {
        return secret;
    }
    if let Ok(secret) = env::var("CONFIG_ENCRYPTION_KEY") {
        return secret;
    }
    cfg.get("config_secret")
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .unwrap_or_else(|| DEFAULT_SECRET.to_string())
}

fn encrypt_private_key(secret: &str, plaintext: &str) -> Result<String> {
    let cipher = build_cipher(secret);
    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let mut ciphertext = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|err| eyre!("encrypt private key: {}", err))?;

    let mut combined = Vec::with_capacity(nonce_bytes.len() + ciphertext.len());
    combined.extend_from_slice(&nonce_bytes);
    combined.append(&mut ciphertext);

    Ok(BASE64_STANDARD.encode(combined))
}

fn decrypt_private_key(secret: &str, encoded: &str) -> Result<String> {
    let cipher = build_cipher(secret);
    let data = BASE64_STANDARD
        .decode(encoded.as_bytes())
        .map_err(|err| eyre!("decode encrypted private key: {}", err))?;
    if data.len() < 12 {
        return Err(eyre!("encrypted private key too short"));
    }
    let (nonce_bytes, ciphertext) = data.split_at(12);
    let nonce = Nonce::from_slice(nonce_bytes);
    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|_| eyre!("failed to decrypt private key; check CONFIG_SECRET"))?;
    String::from_utf8(plaintext).map_err(|err| eyre!("decrypted private key not utf-8: {}", err))
}

fn build_cipher(secret: &str) -> ChaCha20Poly1305 {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    let digest = hasher.finalize();
    let key = Key::from_slice(&digest);
    ChaCha20Poly1305::new(key)
}

fn temp_path(path: &Path) -> PathBuf {
    let mut os_string = path.as_os_str().to_os_string();
    os_string.push(".tmp");
    os_string.into()
}

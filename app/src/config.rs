use std::{env, path::Path};

use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use chacha20poly1305::{aead::Aead, ChaCha20Poly1305, Key, KeyInit as _, Nonce};
use rand_core::{OsRng, RngCore};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::fs;
use tracing::info;

const ENC_PREFIX: &str = "enc:";
const DEFAULT_SECRET: &str = "polyhedron123";

pub async fn load_config(config_path: &Path) -> Result<Value> {
    let raw = fs::read(config_path)
        .await
        .with_context(|| format!("read config file: {}", config_path.display()))?;
    let mut disk_cfg: Value = serde_json::from_slice(&raw).context("parse config json")?;

    let (secret, secret_updated) = load_secret(&mut disk_cfg)?;
    let mut runtime_cfg = disk_cfg.clone();

    let private_key_updated = reconcile_private_key(&secret, &mut disk_cfg, &mut runtime_cfg)
        .context("process private key")?;

    if secret_updated || private_key_updated {
        write_config(config_path, &disk_cfg).await?;
        if private_key_updated {
            info!(
                "wrote encrypted private key back to config: {}",
                config_path.display()
            );
        } else {
            info!("persisted config secret default: {}", config_path.display());
        }
    }

    Ok(runtime_cfg)
}

fn load_secret(cfg: &mut Value) -> Result<(String, bool)> {
    if let Ok(secret) = env::var("CONFIG_SECRET") {
        return Ok((secret, false));
    }
    if let Ok(secret) = env::var("CONFIG_ENCRYPTION_KEY") {
        return Ok((secret, false));
    }

    let obj = cfg
        .as_object_mut()
        .ok_or_else(|| anyhow!("config root must be a JSON object"))?;

    match obj.get("config_secret") {
        Some(Value::String(secret)) => Ok((secret.clone(), false)),
        Some(_) => Err(anyhow!("config_secret must be a string")),
        None => {
            obj.insert(
                "config_secret".to_string(),
                Value::String(DEFAULT_SECRET.to_string()),
            );
            Ok((DEFAULT_SECRET.to_string(), true))
        }
    }
}

fn reconcile_private_key(
    secret: &str,
    disk_cfg: &mut Value,
    runtime_cfg: &mut Value,
) -> Result<bool> {
    let disk_client = disk_cfg
        .get_mut("client_config")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| anyhow!("missing client_config section"))?;
    let runtime_client = runtime_cfg
        .get_mut("client_config")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| anyhow!("missing client_config section"))?;

    let pk_value = disk_client
        .get("private_key")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing client_config.private_key"))?;

    if let Some(rest) = pk_value.strip_prefix(ENC_PREFIX) {
        let plaintext = decrypt_private_key(secret, rest)?;
        runtime_client.insert("private_key".to_string(), Value::String(plaintext));
        Ok(false)
    } else {
        let plaintext = pk_value.to_string();
        let encrypted = encrypt_private_key(secret, &plaintext)?;
        disk_client.insert(
            "private_key".to_string(),
            Value::String(format!("{}{}", ENC_PREFIX, encrypted)),
        );
        runtime_client.insert("private_key".to_string(), Value::String(plaintext));
        Ok(true)
    }
}

fn encrypt_private_key(secret: &str, plaintext: &str) -> Result<String> {
    let cipher = build_cipher(secret);
    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let mut ciphertext = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|err| anyhow!("encrypt private key: {}", err))?;

    let mut combined = Vec::with_capacity(nonce_bytes.len() + ciphertext.len());
    combined.extend_from_slice(&nonce_bytes);
    combined.append(&mut ciphertext);

    Ok(BASE64_STANDARD.encode(combined))
}

fn decrypt_private_key(secret: &str, encoded: &str) -> Result<String> {
    let cipher = build_cipher(secret);
    let data = BASE64_STANDARD
        .decode(encoded.as_bytes())
        .map_err(|err| anyhow!("decode encrypted private key: {}", err))?;
    if data.len() < 12 {
        return Err(anyhow!("encrypted private key too short"));
    }
    let (nonce_bytes, ciphertext) = data.split_at(12);
    let nonce = Nonce::from_slice(nonce_bytes);
    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|_| anyhow!("failed to decrypt private key; check CONFIG_SECRET"))?;
    String::from_utf8(plaintext).map_err(|err| anyhow!("decrypted private key not utf-8: {}", err))
}

fn build_cipher(secret: &str) -> ChaCha20Poly1305 {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    let digest = hasher.finalize();
    let key = Key::from_slice(&digest);
    ChaCha20Poly1305::new(key)
}

async fn write_config(path: &Path, value: &Value) -> Result<()> {
    let mut json = serde_json::to_vec_pretty(value).context("serialize config")?;
    json.push(b'\n');
    let tmp_path = temp_path(path);
    fs::write(&tmp_path, &json)
        .await
        .with_context(|| format!("write temp config: {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path)
        .await
        .with_context(|| format!("replace config: {}", path.display()))?;
    Ok(())
}

fn temp_path(path: &Path) -> std::path::PathBuf {
    let mut os_string = path.as_os_str().to_os_string();
    os_string.push(".tmp");
    os_string.into()
}

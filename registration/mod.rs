use std::{str::FromStr, sync::Arc};

use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use eyre::{eyre, Result, WrapErr};
use reqwest::{Client, Proxy};
use serde_json::json;

use crate::{
    errors::custom::CustomError,
    polymarket::api::{
        clob::schemas::ClobApiKeyResponseBody,
        relayer::{
            common::{approve_tokens, enable_trading},
            endpoints::wait_for_transaction_confirmation,
        },
        typedefs::{AmpCookie, AuthHeaderPayload},
        user::endpoints::{
            create_profile, get_auth_nonce, get_profile_user_data, get_user, login,
            update_preferences, update_username,
        },
    },
    utils::poly::sign_enable_trading_message,
};

#[derive(Debug)]
pub struct AuthContext {
    pub signer: Arc<PrivateKeySigner>,
    pub proxy: Option<Proxy>,
    pub amp_cookie: AmpCookie,
    pub polymarket_nonce: String,
    pub polymarket_session: String,
}

impl AuthContext {
    pub fn proxy_ref(&self) -> Option<&Proxy> {
        self.proxy.as_ref()
    }

    fn proxy_owned(&self) -> Option<Proxy> {
        self.proxy.clone()
    }
}

#[derive(Debug)]
pub struct ProfileContext {
    pub username: Option<String>,
    pub proxy_wallet: Address,
    pub profile_id: String,
    pub preferences_id: Option<String>,
}

#[derive(Debug)]
pub struct RegistrationResult {
    pub username: String,
    pub proxy_wallet: Address,
}

pub async fn authenticate(
    signer: Arc<PrivateKeySigner>,
    proxy: Option<Proxy>,
) -> Result<AuthContext> {
    let (msg_nonce, polymarket_nonce) =
        get_auth_nonce(proxy.as_ref()).await.map_err(|e| eyre!(e))?;

    let payload = AuthHeaderPayload::new(signer.address(), &msg_nonce);
    let mut amp_cookie = AmpCookie::new();
    let auth_header_value = payload.get_auth_header_value(signer.clone()).await;

    let polymarket_session = login(
        &amp_cookie.to_base64_url_encoded(),
        &polymarket_nonce,
        &auth_header_value,
    )
    .await
    .map_err(|e| eyre!(e))?;

    Ok(AuthContext {
        signer,
        proxy,
        amp_cookie,
        polymarket_nonce,
        polymarket_session,
    })
}

pub async fn ensure_profile(auth: &mut AuthContext) -> Result<ProfileContext> {
    let proxy = auth.proxy_owned();
    if let Some(user) = get_user(
        &mut auth.amp_cookie,
        &auth.polymarket_nonce,
        &auth.polymarket_session,
        proxy.as_ref(),
    )
    .await
    .map_err(|e| eyre!(e))?
    {
        let proxy_wallet = Address::from_str(&user.proxy_wallet)
            .wrap_err("parse proxy wallet address from existing user")?;
        let proxy_wallet_checksum = proxy_wallet.to_string();
        auth.amp_cookie
            .set_user_id(Some(proxy_wallet_checksum.clone()));

        let proxy_wallet_query = proxy_wallet_checksum.to_lowercase();
        let resolved_username = get_profile_user_data(&proxy_wallet_query, auth.proxy_ref())
            .await
            .ok()
            .and_then(|resp| resp.name)
            .unwrap_or_else(|| user.username.clone());

        let profile_id = user
            .profile_id
            .map(|id| id.to_string())
            .ok_or_else(|| eyre!("existing user missing profile id"))?;
        let preferences_id = user
            .preferences
            .as_ref()
            .and_then(|prefs| prefs.first())
            .and_then(|pref| pref.id.clone());

        return Ok(ProfileContext {
            username: Some(resolved_username),
            proxy_wallet,
            profile_id,
            preferences_id,
        });
    }

    let proxy = auth.proxy_owned();
    let profile = create_profile(
        auth.signer.clone(),
        proxy.as_ref(),
        &mut auth.amp_cookie,
        &auth.polymarket_nonce,
        &auth.polymarket_session,
    )
    .await
    .map_err(|e| eyre!(e))?;

    let proxy_wallet = Address::from_str(&profile.proxy_wallet)
        .wrap_err("parse proxy wallet address from created profile")?;
    auth.amp_cookie.set_user_id(Some(proxy_wallet.to_string()));

    let preferences_id = profile
        .users
        .get(0)
        .and_then(|user| user.preferences.as_ref())
        .and_then(|prefs| prefs.first())
        .and_then(|pref| pref.id.clone());

    if let Some(pref_id) = &preferences_id {
        let proxy = auth.proxy_owned();
        update_preferences(
            pref_id,
            &mut auth.amp_cookie,
            &auth.polymarket_nonce,
            &auth.polymarket_session,
            proxy.as_ref(),
        )
        .await
        .map_err(|e| eyre!(e))?;
    }

    Ok(ProfileContext {
        username: None,
        proxy_wallet,
        profile_id: profile.id,
        preferences_id,
    })
}

pub async fn set_username(
    auth: &mut AuthContext,
    profile: &ProfileContext,
    username: &str,
) -> Result<()> {
    let proxy = auth.proxy_owned();
    update_username(
        username,
        &profile.profile_id,
        &mut auth.amp_cookie,
        &auth.polymarket_nonce,
        &auth.polymarket_session,
        proxy.as_ref(),
    )
    .await
    .map_err(|e| eyre!(e))
}

pub async fn ensure_proxy_wallet_active(
    auth: &mut AuthContext,
    proxy_wallet: Address,
    polygon_rpc_url: &str,
) -> Result<Option<String>> {
    if is_proxy_wallet_activated(polygon_rpc_url, proxy_wallet).await? {
        return Ok(None);
    }

    let signature = sign_enable_trading_message(auth.signer.clone()).await;
    let proxy = auth.proxy_owned();
    let tx_id = enable_trading(
        auth.signer.clone(),
        &signature,
        &mut auth.amp_cookie,
        &auth.polymarket_nonce,
        &auth.polymarket_session,
        proxy.as_ref(),
    )
    .await
    .map_err(|e| eyre!(e))?;

    let proxy = auth.proxy_owned();
    let tx_hash = wait_for_transaction_confirmation(
        &tx_id,
        &mut auth.amp_cookie,
        &auth.polymarket_nonce,
        &auth.polymarket_session,
        proxy.as_ref(),
        None,
        None,
    )
    .await
    .map_err(|e| eyre!(e))?;

    Ok(Some(tx_hash))
}

pub async fn approve_trading_allowances(auth: &mut AuthContext) -> Result<Option<String>> {
    let proxy = auth.proxy_owned();
    match approve_tokens(
        auth.signer.clone(),
        &mut auth.amp_cookie,
        &auth.polymarket_nonce,
        &auth.polymarket_session,
        proxy.as_ref(),
    )
    .await
    {
        Ok(tx_id) => {
            let proxy = auth.proxy_owned();
            let tx_hash = wait_for_transaction_confirmation(
                &tx_id,
                &mut auth.amp_cookie,
                &auth.polymarket_nonce,
                &auth.polymarket_session,
                proxy.as_ref(),
                None,
                None,
            )
            .await
            .map_err(|e| eyre!(e))?;
            Ok(Some(tx_hash))
        }
        Err(CustomError::HttpStatusError { text, .. }) if text.contains("already executed") => {
            Ok(None)
        }
        Err(err) => Err(eyre!(err)),
    }
}

pub async fn create_or_derive_api_key(
    signer: Arc<PrivateKeySigner>,
    proxy: Option<&Proxy>,
) -> Result<ClobApiKeyResponseBody> {
    match crate::polymarket::api::clob::endpoints::derive_api_key(signer.clone(), proxy).await {
        Ok(resp) => Ok(resp),
        Err(CustomError::HttpStatusError { status, .. }) if status.is_client_error() => {
            crate::polymarket::api::clob::endpoints::create_api_key(signer, proxy)
                .await
                .map_err(|e| eyre!(e))
        }
        Err(err) => Err(eyre!(err)),
    }
}

async fn is_proxy_wallet_activated(rpc_url: &str, proxy_wallet: Address) -> Result<bool> {
    let checksum = proxy_wallet.to_string();
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getCode",
        "params": [checksum, "latest"],
    });

    let client = Client::new();
    let response = client
        .post(rpc_url)
        .json(&payload)
        .send()
        .await
        .wrap_err("eth_getCode request failed")?;

    let value: serde_json::Value = response
        .json()
        .await
        .wrap_err("parse eth_getCode response")?;
    let code = value.get("result").and_then(|v| v.as_str()).unwrap_or("0x");

    Ok(code != "0x")
}

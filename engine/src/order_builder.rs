use crate::eip712_sign::{calculate_order_id, sign_order, Eip712Order};
use crate::model::Side;
use crate::order_types::{CreateOrderOptionsRs, SigType, SignedOrderRequest, SignedOrderWithId};
use alloy_primitives::{Address, U256};
use alloy_signer_local::PrivateKeySigner;
use anyhow::{anyhow, Result};
use rand::Rng;
use rust_decimal::Decimal;
use rust_decimal::RoundingStrategy::{AwayFromZero, MidpointTowardZero, ToZero};
use std::str::FromStr;

#[derive(Copy, Clone)]
struct RoundConfig {
    price: u32,
    size: u32,
    amount: u32,
}

fn decimals_from_tick(tick: f64) -> usize {
    // Convert to string with enough precision, then count fractional digits
    let s = format!("{:.9}", tick); // up to 9 decimals
    if let Some(pos) = s.find('.') {
        let frac = &s[pos + 1..];
        let trimmed = frac.trim_end_matches('0');
        trimmed.len()
    } else {
        0
    }
}

fn round_config_from_tick(tick: f64) -> RoundConfig {
    let dp = decimals_from_tick(tick);
    let price = dp as u32;
    let size = 2u32;
    let amount = price + 2u32; // align with known mapping
    RoundConfig {
        price,
        size,
        amount,
    }
}

fn dec_to_u32(amt: Decimal) -> u32 {
    let mut v = Decimal::from_scientific("1e6").unwrap() * amt;
    if v.scale() > 0 {
        v = v.round_dp_with_strategy(0, MidpointTowardZero);
    }
    v.try_into().expect("round to integer")
}

fn fix_amount_rounding(mut amt: Decimal, rc: &RoundConfig) -> Decimal {
    if amt.scale() > rc.amount {
        amt = amt.round_dp_with_strategy(rc.amount + 4, AwayFromZero);
        if amt.scale() > rc.amount {
            amt = amt.round_dp_with_strategy(rc.amount, ToZero);
        }
    }
    amt
}

fn get_amounts(side: Side, size: Decimal, price: Decimal, rc: &RoundConfig) -> (u32, u32) {
    let raw_price = price.round_dp_with_strategy(rc.price, MidpointTowardZero);
    match side {
        Side::Buy => {
            let t = size.round_dp_with_strategy(rc.size, ToZero);
            let mut m = t * raw_price;
            m = fix_amount_rounding(m, rc);
            (dec_to_u32(m), dec_to_u32(t))
        }
        Side::Sell => {
            let m = size.round_dp_with_strategy(rc.size, ToZero);
            let mut t = m * raw_price;
            t = fix_amount_rounding(t, rc);
            (dec_to_u32(m), dec_to_u32(t))
        }
    }
}

fn exchange_address(chain_id: u64, neg_risk: bool) -> Address {
    // from our TradingContext mapping
    let addr = if chain_id == 137 {
        if neg_risk {
            "0xC5d563A36AE78145C45a50134d48A1215220f80a"
        } else {
            "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
        }
    } else {
        panic!("unsupported chain")
    };
    Address::from_str(addr).unwrap()
}

pub fn build_signed_order(
    signer: &PrivateKeySigner,
    chain_id: u64,
    token_id_dec: &str,
    side: Side,
    price: f64,
    size: f64,
    opts: CreateOrderOptionsRs,
    expiration: u64,
    fee_rate_bps: u32,
    nonce: U256,
    taker: &str,
    maker_str: Option<&str>,
    sig_type: SigType,
) -> Result<SignedOrderWithId> {
    let tick = opts
        .tick_size
        .ok_or_else(|| anyhow!("tick_size required"))?;
    if !(tick > 0.0 && tick < 1.0) {
        return Err(anyhow!("invalid tick_size: {}", tick));
    }
    let rc = round_config_from_tick(tick);

    let price_d = Decimal::from_f64_retain(price).ok_or_else(|| anyhow!("invalid price"))?;
    let size_d = Decimal::from_f64_retain(size).ok_or_else(|| anyhow!("invalid size"))?;
    let (maker_amount, taker_amount) = get_amounts(side, size_d, price_d, &rc);

    let token = U256::from_str_radix(token_id_dec, 10).unwrap();
    let taker_addr = Address::from_str(taker).unwrap_or(Address::ZERO);
    let exchange = exchange_address(chain_id, opts.neg_risk.unwrap_or(false));

    let mut rng = rand::thread_rng();
    let salt: u64 = rng.gen::<u64>() & 0x0000_FFFF_FFFF_FFFFu64;

    // maker address (Safe when using PolyGnosisSafe, else EOA)
    let maker_addr = if let Some(m) = maker_str {
        Address::from_str(m).unwrap_or(signer.address())
    } else {
        signer.address()
    };

    let order = Eip712Order {
        salt: U256::from(salt),
        maker: maker_addr, // maker set below
        signer: signer.address(),
        taker: taker_addr,
        tokenId: token,
        makerAmount: U256::from(maker_amount),
        takerAmount: U256::from(taker_amount),
        expiration: U256::from(expiration),
        nonce,
        feeRateBps: U256::from(fee_rate_bps),
        side: match side {
            Side::Buy => 0u8,
            Side::Sell => 1u8,
        },
        signatureType: sig_type as u8,
    };
    let sig = sign_order(signer, &order, chain_id, exchange)?;
    let domain = alloy_sol_types::eip712_domain!( name: "Polymarket CTF Exchange", version: "1", chain_id: chain_id, verifying_contract: exchange, );
    let order_id = calculate_order_id(&order, &domain);

    let req = SignedOrderRequest {
        salt,
        maker: maker_addr.to_checksum(None),
        signer: signer.address().to_checksum(None),
        taker: taker_addr.to_checksum(None),
        token_id: token_id_dec.to_string(),
        maker_amount: maker_amount.to_string(),
        taker_amount: taker_amount.to_string(),
        expiration: expiration.to_string(),
        nonce: nonce.to_string(),
        fee_rate_bps: fee_rate_bps.to_string(),
        side: match side {
            Side::Buy => "BUY".into(),
            Side::Sell => "SELL".into(),
        },
        signature_type: sig_type as u8,
        signature: sig,
    };
    Ok(SignedOrderWithId {
        order: req,
        order_id,
    })
}

// build_signed_order_with_id removed; build_signed_order now returns id by default.

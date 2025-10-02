use alloy_primitives::{hex::encode_prefixed, Address, U256};
use alloy_primitives::{keccak256, B256};
use alloy_signer::{Signer, SignerSync};
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{eip712_domain, sol, SolStruct};

sol! {
    struct ClobAuth {
        address address;
        string timestamp;
        uint256 nonce;
        string message;
    }
}

sol! {
    struct Order {
        uint256 salt;
        address maker;
        address signer;
        address taker;
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint256 expiration;
        uint256 nonce;
        uint256 feeRateBps;
        uint8 side;
        uint8 signatureType;
    }
}

pub fn parse_pk(pk_hex: &str) -> anyhow::Result<PrivateKeySigner> {
    Ok(pk_hex.parse::<PrivateKeySigner>()?)
}

pub fn address_hex(signer: &PrivateKeySigner) -> String {
    encode_prefixed(signer.address().as_slice())
}

pub fn sign_clob_auth_message(
    signer: &PrivateKeySigner,
    timestamp: String,
    nonce: U256,
) -> anyhow::Result<String> {
    let my_struct = ClobAuth {
        address: signer.address(),
        timestamp,
        nonce,
        message: "This message attests that I control the given wallet".to_owned(),
    };
    let domain = eip712_domain!( name: "ClobAuthDomain", version: "1", chain_id: 137u64, );
    let val = signer.sign_typed_data_sync(&my_struct, &domain)?;
    Ok(encode_prefixed(val.as_bytes()))
}

pub fn sign_order(
    signer: &PrivateKeySigner,
    order: &Eip712Order,
    chain_id: u64,
    verifying_contract: Address,
) -> anyhow::Result<String> {
    let domain = eip712_domain!( name: "Polymarket CTF Exchange", version: "1", chain_id: chain_id, verifying_contract: verifying_contract, );
    let val = signer.sign_typed_data_sync(order, &domain)?;
    Ok(encode_prefixed(val.as_bytes()))
}

pub use Order as Eip712Order;

pub fn calculate_order_id(order: &Eip712Order, domain: &alloy_sol_types::Eip712Domain) -> String {
    // eip-712 digest = keccak256(0x1901 || domainSeparator || hashStruct(order))
    let struct_hash: B256 = order.eip712_hash_struct();
    let domain_sep: B256 = domain.separator();
    let mut buf = [0u8; 2 + 32 + 32];
    buf[0] = 0x19;
    buf[1] = 0x01;
    buf[2..34].copy_from_slice(domain_sep.as_slice());
    buf[34..66].copy_from_slice(struct_hash.as_slice());
    let digest = keccak256(buf);
    format!("0x{:x}", digest)
}

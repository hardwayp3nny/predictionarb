use reqwest::StatusCode;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CustomError {
    #[error("http {status}: {text}")]
    HttpStatusError { status: StatusCode, text: String },

    #[error("polymarket api error: {0}")]
    PolymarketApi(String),

    #[error("timeout: {0}")]
    Timeout(String),

    #[error("decode error: {0}")]
    Decode(String),

    #[error("clob api error: {0}")]
    ClobApiError(String),

    #[error("retry limit exceeded")]
    TriesExceeded,

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

impl From<std::io::Error> for CustomError {
    fn from(err: std::io::Error) -> Self {
        Self::PolymarketApi(err.to_string())
    }
}

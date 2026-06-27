use serde::Deserialize;

use crate::error::Result;
use crate::http::HttpClient;

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct DataTrade {
    pub id: Option<String>,
    pub market: Option<String>,
    pub asset_id: Option<String>,
    pub timestamp: Option<i64>,
    pub price: Option<f64>,
    pub size: Option<f64>,
    pub side: Option<String>,
    pub transaction_hash: Option<String>,
    pub maker_address: Option<String>,
    pub taker_address: Option<String>,
}

pub struct DataClient {
    pub base_url: String,
    pub http: HttpClient,
}

impl DataClient {
    pub fn new(base_url: impl Into<String>, http: HttpClient) -> Self {
        Self {
            base_url: base_url.into(),
            http,
        }
    }

    pub async fn fetch_trades(&self, market_id: &str, limit: usize) -> Result<Vec<DataTrade>> {
        let url = format!(
            "{}/trades?market={market_id}&limit={limit}",
            self.base_url
        );
        let body = self.http.get_bytes(&url).await?;
        let trades: Vec<DataTrade> = serde_json::from_slice(&body).unwrap_or_default();
        Ok(trades)
    }
}

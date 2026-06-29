use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::http::HttpClient;

#[derive(Debug, Clone, Deserialize, Serialize)]
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

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PolymarketUserPosition {
    pub proxy_wallet: Option<String>,
    pub user: Option<String>,
    pub condition_id: Option<String>,
    pub market: Option<String>,
    pub asset: Option<String>,
    pub asset_id: Option<String>,
    pub token_id: Option<String>,
    pub size: Option<f64>,
    pub avg_price: Option<f64>,
    pub cur_price: Option<f64>,
    pub current_value: Option<f64>,
    pub value: Option<f64>,
    pub cash_pnl: Option<f64>,
    pub realized_pnl: Option<f64>,
    pub percent_pnl: Option<f64>,
    pub title: Option<String>,
    pub status: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PolymarketUserActivity {
    pub proxy_wallet: Option<String>,
    pub user: Option<String>,
    #[serde(rename = "type")]
    pub activity_type: Option<String>,
    pub transaction_hash: Option<String>,
    pub condition_id: Option<String>,
    pub market: Option<String>,
    pub asset: Option<String>,
    pub asset_id: Option<String>,
    pub token_id: Option<String>,
    pub timestamp: Option<i64>,
    pub price: Option<f64>,
    pub size: Option<f64>,
    pub usdc_size: Option<f64>,
    pub side: Option<String>,
    pub outcome: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
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
        let url = format!("{}/trades?market={market_id}&limit={limit}", self.base_url);
        let body = self.http.get_bytes(&url).await?;
        let trades: Vec<DataTrade> = serde_json::from_slice(&body).unwrap_or_default();
        Ok(trades)
    }

    pub async fn fetch_user_activity(
        &self,
        user_id: &str,
        limit: Option<usize>,
    ) -> Result<Vec<PolymarketUserActivity>> {
        let url = self.user_url("/activity", user_id, limit);
        let body = self.http.get_bytes(url.as_str()).await?;
        Ok(serde_json::from_slice(&body).unwrap_or_default())
    }

    pub async fn fetch_current_positions(
        &self,
        user_id: &str,
        limit: Option<usize>,
    ) -> Result<Vec<PolymarketUserPosition>> {
        let url = self.user_url("/positions", user_id, limit);
        let body = self.http.get_bytes(url.as_str()).await?;
        Ok(serde_json::from_slice(&body).unwrap_or_default())
    }

    pub async fn fetch_closed_positions(
        &self,
        user_id: &str,
        limit: Option<usize>,
    ) -> Result<Vec<PolymarketUserPosition>> {
        let url = self.user_url("/closed-positions", user_id, limit);
        let body = self.http.get_bytes(url.as_str()).await?;
        Ok(serde_json::from_slice(&body).unwrap_or_default())
    }

    pub async fn fetch_user_value(&self, user_id: &str) -> Result<serde_json::Value> {
        let mut url = self.url("/value");
        url.query_pairs_mut().append_pair("user", user_id);
        self.http.get_json(url.as_str()).await
    }

    pub async fn download_accounting_snapshot(&self, user_id: &str) -> Result<Vec<u8>> {
        let mut url = self.url("/accounting");
        url.query_pairs_mut().append_pair("user", user_id);
        self.http.get_bytes(url.as_str()).await
    }

    fn user_url(&self, path: &str, user_id: &str, limit: Option<usize>) -> reqwest::Url {
        let mut url = self.url(path);
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("user", user_id);
            if let Some(limit) = limit {
                pairs.append_pair("limit", &limit.to_string());
            }
        }
        url
    }

    fn url(&self, path: &str) -> reqwest::Url {
        reqwest::Url::parse(&format!("{}{}", self.base_url.trim_end_matches('/'), path))
            .expect("valid Polymarket data API URL")
    }
}

use serde::Deserialize;

use crate::config::BATCH_TOKEN_LIMIT;
use crate::error::Result;
use crate::http::HttpClient;

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct OrderBookResponse {
    pub hash: Option<String>,
    pub market: Option<String>,
    pub asset_id: Option<String>,
    pub timestamp: Option<String>,
    pub bids: Option<Vec<BookLevelJson>>,
    pub asks: Option<Vec<BookLevelJson>>,
    pub min_order_size: Option<String>,
    pub tick_size: Option<String>,
    pub neg_risk: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct BookLevelJson {
    pub price: String,
    pub size: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PriceHistoryPoint {
    pub t: i64,
    pub p: f64,
}

#[derive(Clone)]
pub struct ClobClient {
    pub base_url: String,
    pub http: HttpClient,
}

impl ClobClient {
    pub fn new(base_url: impl Into<String>, http: HttpClient) -> Self {
        Self {
            base_url: base_url.into(),
            http,
        }
    }

    pub async fn get_book(&self, token_id: &str) -> Result<OrderBookResponse> {
        let url = format!("{}/book?token_id={token_id}", self.base_url);
        let body = self.http.get_bytes(&url).await?;
        Ok(serde_json::from_slice(&body)?)
    }

    pub async fn get_books_batch(&self, token_ids: &[String]) -> Result<Vec<OrderBookResponse>> {
        let mut books = Vec::new();
        for chunk in token_ids.chunks(BATCH_TOKEN_LIMIT) {
            let url = format!("{}/books", self.base_url);
            let payload = serde_json::Value::Array(
                chunk
                    .iter()
                    .map(|token_id| serde_json::json!({ "token_id": token_id }))
                    .collect(),
            );
            self.throttle_post_json(&url, &payload, &mut books).await?;
        }
        Ok(books)
    }

    pub async fn get_midpoint(&self, token_id: &str) -> Result<f64> {
        let url = format!("{}/midpoint?token_id={token_id}", self.base_url);
        let json = self.http.get_json(&url).await?;
        json.get("mid")
            .and_then(|v| v.as_f64())
            .or_else(|| json.get("mid").and_then(|v| v.as_str()?.parse().ok()))
            .ok_or_else(|| crate::error::OddsfoxError::Parse {
                table: "midpoint".into(),
                message: format!("missing midpoint for {token_id}"),
            })
    }

    pub async fn get_spread(&self, token_id: &str) -> Result<f64> {
        let url = format!("{}/spread?token_id={token_id}", self.base_url);
        let json = self.http.get_json(&url).await?;
        json.get("spread")
            .and_then(|v| v.as_f64())
            .or_else(|| json.get("spread").and_then(|v| v.as_str()?.parse().ok()))
            .ok_or_else(|| crate::error::OddsfoxError::Parse {
                table: "spread".into(),
                message: format!("missing spread for {token_id}"),
            })
    }

    pub async fn get_price(&self, token_id: &str, side: &str) -> Result<f64> {
        let url = format!("{}/price?token_id={token_id}&side={side}", self.base_url);
        let json = self.http.get_json(&url).await?;
        json.get("price")
            .and_then(|v| v.as_f64())
            .or_else(|| json.get("price").and_then(|v| v.as_str()?.parse().ok()))
            .ok_or_else(|| crate::error::OddsfoxError::Parse {
                table: "price".into(),
                message: format!("missing price for {token_id}"),
            })
    }

    pub async fn get_prices_history(
        &self,
        token_id: &str,
        interval: Option<&str>,
        fidelity: Option<u32>,
        start_ts: Option<i64>,
        end_ts: Option<i64>,
    ) -> Result<Vec<PriceHistoryPoint>> {
        let mut url = format!("{}/prices-history?market={token_id}", self.base_url);
        if let Some(interval) = interval {
            url.push_str(&format!("&interval={interval}"));
        }
        if let Some(fidelity) = fidelity {
            url.push_str(&format!("&fidelity={fidelity}"));
        }
        if let Some(start_ts) = start_ts {
            url.push_str(&format!("&startTs={start_ts}"));
        }
        if let Some(end_ts) = end_ts {
            url.push_str(&format!("&endTs={end_ts}"));
        }
        let json = self.http.get_json(&url).await?;
        let history = json
            .get("history")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        Ok(history)
    }

    async fn throttle_post_json<T: for<'de> Deserialize<'de>>(
        &self,
        url: &str,
        payload: &serde_json::Value,
        out: &mut Vec<T>,
    ) -> Result<()> {
        let response = reqwest::Client::new()
            .post(url)
            .header("User-Agent", self.http.user_agent())
            .json(payload)
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(crate::error::OddsfoxError::Http {
                url: url.to_string(),
                status: response.status().as_u16(),
            });
        }
        let items: Vec<T> = response.json().await?;
        out.extend(items);
        Ok(())
    }
}

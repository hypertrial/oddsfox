use std::path::Path;

use base64::Engine;
use reqwest::header::{HeaderMap, HeaderValue};
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::pss::BlindedSigningKey;
use rsa::rand_core::OsRng;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::{sha2::Sha256, RsaPrivateKey};

use crate::config::KalshiStatus;
use crate::error::{OddsfoxError, Result};
use crate::http::HttpClient;

use super::models::{
    HistoricalCutoff, KalshiCandlestick, KalshiCandlestickResponse, KalshiEventEnvelope,
    KalshiFill, KalshiFillsResponse, KalshiMarketResponse, KalshiOrderbookEnvelope, KalshiPosition,
    KalshiPositionsResponse, KalshiTrade, KalshiTradesResponse,
};

#[derive(Clone)]
pub struct KalshiAuth {
    key_id: String,
    private_key: RsaPrivateKey,
}

impl KalshiAuth {
    pub fn from_key_file(key_id: String, path: &Path) -> Result<Self> {
        let pem = std::fs::read_to_string(path)?;
        let private_key = RsaPrivateKey::from_pkcs8_pem(&pem)
            .or_else(|_| RsaPrivateKey::from_pkcs1_pem(&pem))
            .map_err(|err| OddsfoxError::Config(format!("invalid Kalshi private key: {err}")))?;
        Ok(Self {
            key_id,
            private_key,
        })
    }

    pub fn headers(
        &self,
        method: &str,
        path_without_query: &str,
        timestamp_ms: i64,
    ) -> Result<HeaderMap> {
        let message = format!("{timestamp_ms}{method}{path_without_query}");
        let signing_key = BlindedSigningKey::<Sha256>::new(self.private_key.clone());
        let signature = signing_key.sign_with_rng(&mut OsRng, message.as_bytes());
        let encoded = base64::engine::general_purpose::STANDARD.encode(signature.to_bytes());
        let mut headers = HeaderMap::new();
        headers.insert(
            "KALSHI-ACCESS-KEY",
            HeaderValue::from_str(&self.key_id)
                .map_err(|err| OddsfoxError::Config(err.to_string()))?,
        );
        headers.insert(
            "KALSHI-ACCESS-SIGNATURE",
            HeaderValue::from_str(&encoded).map_err(|err| OddsfoxError::Config(err.to_string()))?,
        );
        headers.insert(
            "KALSHI-ACCESS-TIMESTAMP",
            HeaderValue::from_str(&timestamp_ms.to_string())
                .map_err(|err| OddsfoxError::Config(err.to_string()))?,
        );
        Ok(headers)
    }
}

#[derive(Clone)]
pub struct KalshiClient {
    base_url: String,
    http: HttpClient,
    auth: Option<KalshiAuth>,
}

impl KalshiClient {
    pub fn new(base_url: String, http: HttpClient, auth: Option<KalshiAuth>) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http,
            auth,
        }
    }

    pub fn markets_url(
        &self,
        status: KalshiStatus,
        series: Option<&str>,
        limit: Option<usize>,
    ) -> String {
        let mut url = self.url("/markets");
        {
            let mut pairs = url.query_pairs_mut();
            if let Some(series) = series {
                pairs.append_pair("series_ticker", series);
            }
            if let Some(status) = status_param(status) {
                pairs.append_pair("status", status);
            }
            if let Some(limit) = limit {
                pairs.append_pair("limit", &limit.to_string());
            }
        }
        url.to_string()
    }

    pub async fn get_markets(
        &self,
        status: KalshiStatus,
        series: Option<&str>,
        limit: Option<usize>,
    ) -> Result<KalshiMarketResponse> {
        let mut out = Vec::new();
        let mut cursor = None;
        loop {
            let mut url = self.url("/markets");
            {
                let mut pairs = url.query_pairs_mut();
                if let Some(series) = series {
                    pairs.append_pair("series_ticker", series);
                }
                if let Some(status) = status_param(status) {
                    pairs.append_pair("status", status);
                }
                pairs.append_pair("limit", &limit.unwrap_or(200).min(1000).to_string());
                if let Some(cursor) = cursor.as_deref() {
                    pairs.append_pair("cursor", cursor);
                }
            }
            let page: KalshiMarketResponse = self.get_json(url).await?;
            out.extend(page.markets);
            cursor = page.cursor.filter(|c| !c.is_empty());
            if cursor.is_none() || limit.is_some_and(|max| out.len() >= max) {
                break;
            }
        }
        if let Some(max) = limit {
            out.truncate(max);
        }
        Ok(KalshiMarketResponse {
            markets: out,
            cursor,
        })
    }

    pub async fn get_event(&self, event_ticker: &str) -> Result<KalshiEventEnvelope> {
        self.get_json(self.url(&format!("/events/{event_ticker}")))
            .await
    }

    pub async fn get_candlesticks(
        &self,
        series_ticker: &str,
        market_ticker: &str,
        period_interval: u32,
        start_ts: Option<i64>,
        end_ts: Option<i64>,
    ) -> Result<Vec<KalshiCandlestick>> {
        let mut url = self.url(&format!(
            "/series/{series_ticker}/markets/{market_ticker}/candlesticks"
        ));
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("period_interval", &period_interval.to_string());
            if let Some(start_ts) = start_ts {
                pairs.append_pair("start_ts", &start_ts.to_string());
            }
            if let Some(end_ts) = end_ts {
                pairs.append_pair("end_ts", &end_ts.to_string());
            }
        }
        let response: KalshiCandlestickResponse = self.get_json(url).await?;
        Ok(response.candlesticks)
    }

    pub async fn get_trades(
        &self,
        ticker: Option<&str>,
        min_ts: Option<i64>,
        max_ts: Option<i64>,
        limit: Option<usize>,
    ) -> Result<Vec<KalshiTrade>> {
        let mut out = Vec::new();
        let mut cursor = None;
        loop {
            let mut url = self.url("/markets/trades");
            {
                let mut pairs = url.query_pairs_mut();
                if let Some(ticker) = ticker {
                    pairs.append_pair("ticker", ticker);
                }
                if let Some(min_ts) = min_ts {
                    pairs.append_pair("min_ts", &min_ts.to_string());
                }
                if let Some(max_ts) = max_ts {
                    pairs.append_pair("max_ts", &max_ts.to_string());
                }
                pairs.append_pair("limit", &limit.unwrap_or(500).min(1000).to_string());
                if let Some(cursor) = cursor.as_deref() {
                    pairs.append_pair("cursor", cursor);
                }
            }
            let page: KalshiTradesResponse = self.get_json(url).await?;
            out.extend(page.trades);
            cursor = page.cursor.filter(|c| !c.is_empty());
            if cursor.is_none() || limit.is_some_and(|max| out.len() >= max) {
                break;
            }
        }
        if let Some(max) = limit {
            out.truncate(max);
        }
        Ok(out)
    }

    pub async fn get_fills(
        &self,
        min_ts: Option<i64>,
        limit: Option<usize>,
    ) -> Result<Vec<KalshiFill>> {
        let mut out = Vec::new();
        let mut cursor = None;
        loop {
            let mut url = self.url("/portfolio/fills");
            {
                let mut pairs = url.query_pairs_mut();
                if let Some(min_ts) = min_ts {
                    pairs.append_pair("min_ts", &min_ts.to_string());
                }
                pairs.append_pair("limit", &limit.unwrap_or(500).min(1000).to_string());
                if let Some(cursor) = cursor.as_deref() {
                    pairs.append_pair("cursor", cursor);
                }
            }
            let page: KalshiFillsResponse = self.get_json(url).await?;
            out.extend(page.fills);
            cursor = page.cursor.filter(|c| !c.is_empty());
            if cursor.is_none() || limit.is_some_and(|max| out.len() >= max) {
                break;
            }
        }
        if let Some(max) = limit {
            out.truncate(max);
        }
        Ok(out)
    }

    pub async fn get_positions(&self, limit: Option<usize>) -> Result<Vec<KalshiPosition>> {
        let mut out = Vec::new();
        let mut cursor = None;
        loop {
            let mut url = self.url("/portfolio/positions");
            {
                let mut pairs = url.query_pairs_mut();
                pairs.append_pair("limit", &limit.unwrap_or(500).min(1000).to_string());
                if let Some(cursor) = cursor.as_deref() {
                    pairs.append_pair("cursor", cursor);
                }
            }
            let page: KalshiPositionsResponse = self.get_json(url).await?;
            if page.positions.is_empty() {
                out.extend(page.market_positions);
            } else {
                out.extend(page.positions);
            }
            cursor = page.cursor.filter(|c| !c.is_empty());
            if cursor.is_none() || limit.is_some_and(|max| out.len() >= max) {
                break;
            }
        }
        if let Some(max) = limit {
            out.truncate(max);
        }
        Ok(out)
    }

    pub async fn get_orderbook(
        &self,
        ticker: &str,
        depth: Option<u32>,
    ) -> Result<KalshiOrderbookEnvelope> {
        let mut url = self.url(&format!("/markets/{ticker}/orderbook"));
        if let Some(depth) = depth {
            url.query_pairs_mut()
                .append_pair("depth", &depth.min(100).to_string());
        }
        self.get_json(url).await
    }

    pub async fn get_historical_cutoff(&self) -> Result<Option<i64>> {
        let value: serde_json::Value = self.get_json(self.url("/historical/cutoff")).await?;
        if let Ok(cutoff) = serde_json::from_value::<HistoricalCutoff>(value.clone()) {
            return Ok(cutoff.cutoff_ts);
        }
        Ok(value.get("cutoff_ts").and_then(|v| v.as_i64()))
    }

    async fn get_json<T: for<'de> serde::Deserialize<'de>>(&self, url: reqwest::Url) -> Result<T> {
        let headers = self.auth_headers(&url)?;
        let body = self
            .http
            .get_bytes_with_headers(url.as_str(), headers)
            .await?;
        Ok(serde_json::from_slice(&body)?)
    }

    fn auth_headers(&self, url: &reqwest::Url) -> Result<HeaderMap> {
        match &self.auth {
            Some(auth) => auth.headers("GET", url.path(), chrono::Utc::now().timestamp_millis()),
            None => Ok(HeaderMap::new()),
        }
    }

    fn url(&self, path: &str) -> reqwest::Url {
        reqwest::Url::parse(&format!("{}{}", self.base_url, path)).expect("valid Kalshi base URL")
    }
}

fn status_param(status: KalshiStatus) -> Option<&'static str> {
    match status {
        KalshiStatus::Open => Some("open"),
        KalshiStatus::Closed => Some("closed"),
        KalshiStatus::Settled => Some("settled"),
        KalshiStatus::All => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path, query_param, query_param_is_missing};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn auth_headers_sign_path_without_query() {
        let key = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
        let auth = KalshiAuth {
            key_id: "kid".into(),
            private_key: key,
        };
        let headers = auth.headers("GET", "/trade-api/v2/markets", 123).unwrap();
        assert_eq!(headers["KALSHI-ACCESS-KEY"], "kid");
        assert_eq!(headers["KALSHI-ACCESS-TIMESTAMP"], "123");
        assert!(!headers["KALSHI-ACCESS-SIGNATURE"].is_empty());
    }

    #[tokio::test]
    async fn paginates_markets_and_reads_cutoff() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/markets"))
            .and(query_param("status", "open"))
            .and(query_param_is_missing("cursor"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "markets": [{"ticker": "KXTEST-1"}],
                "cursor": "c2"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/markets"))
            .and(query_param("cursor", "c2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "markets": [{"ticker": "KXTEST-2"}],
                "cursor": ""
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/historical/cutoff"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"cutoff_ts": 1700000000})),
            )
            .mount(&server)
            .await;

        let client = KalshiClient::new(
            server.uri(),
            crate::http::HttpClient::new(100.0, 0, "test").unwrap(),
            None,
        );
        let markets = client
            .get_markets(KalshiStatus::Open, None, None)
            .await
            .unwrap();
        assert_eq!(markets.markets.len(), 2);
        assert_eq!(
            client.get_historical_cutoff().await.unwrap(),
            Some(1700000000)
        );
    }
}

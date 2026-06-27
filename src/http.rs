use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use tokio::sync::Mutex;
use tokio::time::{sleep, Instant};

use crate::error::{OddsfoxError, Result};

#[derive(Clone)]
pub struct HttpClient {
    inner: Client,
    min_interval: Duration,
    last_request: Arc<Mutex<Instant>>,
    max_retries: u32,
    user_agent: String,
}

impl HttpClient {
    pub fn new(requests_per_second: f64, max_retries: u32, user_agent: impl Into<String>) -> Result<Self> {
        let rps = requests_per_second.max(0.1);
        let user_agent = user_agent.into();
        let client = Client::builder()
            .user_agent(&user_agent)
            .timeout(Duration::from_secs(120))
            .build()?;
        Ok(Self {
            inner: client,
            min_interval: Duration::from_secs_f64(1.0 / rps),
            last_request: Arc::new(Mutex::new(Instant::now() - Duration::from_secs(1))),
            max_retries,
            user_agent,
        })
    }

    pub fn user_agent(&self) -> &str {
        &self.user_agent
    }

    pub async fn get_json(&self, url: &str) -> Result<serde_json::Value> {
        let body = self.get_bytes(url).await?;
        Ok(serde_json::from_slice(&body)?)
    }

    pub async fn get_bytes(&self, url: &str) -> Result<Vec<u8>> {
        let mut attempt = 0;
        loop {
            self.throttle().await;
            let response = self.inner.get(url).send().await?;
            let status = response.status();
            if status.is_success() {
                return Ok(response.bytes().await?.to_vec());
            }
            if status.as_u16() == 429 || status.is_server_error() {
                attempt += 1;
                if attempt > self.max_retries {
                    return Err(OddsfoxError::Http {
                        url: url.to_string(),
                        status: status.as_u16(),
                    });
                }
                sleep(Duration::from_millis(500 * attempt as u64)).await;
                continue;
            }
            return Err(OddsfoxError::Http {
                url: url.to_string(),
                status: status.as_u16(),
            });
        }
    }

    async fn throttle(&self) {
        let mut last = self.last_request.lock().await;
        let elapsed = last.elapsed();
        if elapsed < self.min_interval {
            sleep(self.min_interval - elapsed).await;
        }
        *last = Instant::now();
    }
}

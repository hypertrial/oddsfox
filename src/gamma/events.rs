#![allow(non_snake_case)]

use serde::Deserialize;

use crate::error::Result;
use crate::http::HttpClient;

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct GammaEvent {
    pub id: String,
    pub slug: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
    pub category: Option<String>,
    #[serde(default)]
    pub tags: Vec<GammaTag>,
    pub active: Option<bool>,
    pub closed: Option<bool>,
    #[serde(default)]
    pub markets: Vec<super::markets::GammaMarket>,
    pub startDate: Option<String>,
    pub endDate: Option<String>,
    pub createdAt: Option<String>,
    pub updatedAt: Option<String>,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct GammaTag {
    pub label: Option<String>,
    pub slug: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct FetchEventsParams<'a> {
    pub base_url: &'a str,
    pub active: Option<bool>,
    pub closed: Option<bool>,
    pub tag: Option<&'a str>,
    pub limit: usize,
    pub offset: usize,
}

pub async fn fetch_events(
    client: &HttpClient,
    params: FetchEventsParams<'_>,
) -> Result<Vec<GammaEvent>> {
    let mut url = format!(
        "{}/events?limit={}&offset={}",
        params.base_url, params.limit, params.offset
    );
    if let Some(active) = params.active {
        url.push_str(&format!("&active={active}"));
    }
    if let Some(closed) = params.closed {
        url.push_str(&format!("&closed={closed}"));
    }
    if let Some(tag) = params.tag {
        url.push_str(&format!("&tag_slug={tag}"));
    }
    let body = client.get_bytes(&url).await?;
    let events: Vec<GammaEvent> = serde_json::from_slice(&body)?;
    Ok(events)
}

pub async fn fetch_event_by_id(
    client: &HttpClient,
    base_url: &str,
    event_id: &str,
) -> Result<GammaEvent> {
    let url = format!("{base_url}/events/{event_id}");
    let body = client.get_bytes(&url).await?;
    Ok(serde_json::from_slice(&body)?)
}

pub async fn fetch_all_events(
    client: &HttpClient,
    params: FetchEventsParams<'_>,
    max_records: Option<usize>,
) -> Result<Vec<GammaEvent>> {
    let mut all = Vec::new();
    let page_size = params.limit.max(1);
    let mut offset = params.offset;
    loop {
        let page = fetch_events(
            client,
            FetchEventsParams {
                base_url: params.base_url,
                active: params.active,
                closed: params.closed,
                tag: params.tag,
                limit: page_size,
                offset,
            },
        )
        .await?;
        if page.is_empty() {
            break;
        }
        let count = page.len();
        all.extend(page);
        if let Some(max) = max_records {
            if all.len() >= max {
                all.truncate(max);
                break;
            }
        }
        if count < page_size {
            break;
        }
        offset += page_size;
    }
    Ok(all)
}

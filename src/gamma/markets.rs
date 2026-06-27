#![allow(non_snake_case)]

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct GammaMarket {
    pub id: String,
    pub event_id: Option<String>,
    pub conditionId: Option<String>,
    pub questionID: Option<String>,
    pub slug: Option<String>,
    pub question: Option<String>,
    pub description: Option<String>,
    pub active: Option<bool>,
    pub closed: Option<bool>,
    pub resolved: Option<bool>,
    pub enableOrderBook: Option<bool>,
    pub negRisk: Option<bool>,
    pub liquidity: Option<String>,
    pub volume: Option<String>,
    pub volume24hr: Option<String>,
    pub openInterest: Option<String>,
    pub endDate: Option<String>,
    pub resolutionTime: Option<String>,
    pub resolutionSource: Option<String>,
    #[serde(default)]
    pub outcomes: Option<String>,
    #[serde(default)]
    pub outcomePrices: Option<String>,
    #[serde(default)]
    pub clobTokenIds: Option<String>,
    pub winningOutcome: Option<String>,
    pub winningOutcomeIndex: Option<i32>,
}

impl GammaMarket {
    pub fn parsed_outcomes(&self) -> Vec<(i32, String, Option<String>)> {
        let names: Vec<String> = self
            .outcomes
            .as_ref()
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or_default();
        let token_ids: Vec<String> = self
            .clobTokenIds
            .as_ref()
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or_default();
        names
            .into_iter()
            .enumerate()
            .map(|(idx, name)| {
                let token_id = token_ids.get(idx).cloned();
                (idx as i32, name, token_id)
            })
            .collect()
    }
}

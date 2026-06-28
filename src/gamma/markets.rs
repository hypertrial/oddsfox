#![allow(non_snake_case)]

use serde::Deserialize;

/// Gamma returns monetary fields inconsistently as JSON strings or numbers.
/// Normalize either representation (and null) into `Option<String>`.
fn de_stringish<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    Ok(value.and_then(|value| match value {
        serde_json::Value::String(s) => Some(s),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }))
}

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
    #[serde(default, deserialize_with = "de_stringish")]
    pub liquidity: Option<String>,
    #[serde(default, deserialize_with = "de_stringish")]
    pub volume: Option<String>,
    #[serde(default, deserialize_with = "de_stringish")]
    pub volume24hr: Option<String>,
    #[serde(default, deserialize_with = "de_stringish")]
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

#[cfg(test)]
mod tests {
    use super::GammaMarket;

    #[test]
    fn deserializes_numeric_monetary_fields() {
        // Gamma returns these fields as bare JSON numbers for some markets.
        let json = r#"{
            "id": "691547",
            "volume24hr": 30.700835,
            "liquidity": 4262.9944,
            "volume": 541493.91993,
            "openInterest": 12
        }"#;
        let market: GammaMarket = serde_json::from_str(json).unwrap();
        assert_eq!(market.volume24hr.as_deref(), Some("30.700835"));
        assert_eq!(market.liquidity.as_deref(), Some("4262.9944"));
        assert_eq!(market.volume.as_deref(), Some("541493.91993"));
        assert_eq!(market.openInterest.as_deref(), Some("12"));
    }

    #[test]
    fn deserializes_string_monetary_fields() {
        let json = r#"{ "id": "1", "volume24hr": "30.7", "liquidity": null }"#;
        let market: GammaMarket = serde_json::from_str(json).unwrap();
        assert_eq!(market.volume24hr.as_deref(), Some("30.7"));
        assert_eq!(market.liquidity, None);
    }
}

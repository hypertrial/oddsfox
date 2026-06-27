use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{OddsfoxError, Result};

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_string())
            }
        }

        impl FromStr for $name {
            type Err = OddsfoxError;

            fn from_str(s: &str) -> Result<Self> {
                let s = s.trim();
                if s.is_empty() {
                    return Err(OddsfoxError::InvalidId {
                        kind: stringify!($name).to_string(),
                        value: s.to_string(),
                    });
                }
                Ok(Self(s.to_string()))
            }
        }
    };
}

id_type!(EventId);
id_type!(MarketId);
id_type!(TokenId);
id_type!(ConditionId);
id_type!(QuestionId);
id_type!(WalletAddress);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_id_parses() {
        let id = TokenId::from_str("12345").unwrap();
        assert_eq!(id.as_str(), "12345");
    }

    #[test]
    fn empty_id_rejected() {
        assert!(TokenId::from_str("").is_err());
    }
}

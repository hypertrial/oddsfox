mod events;
mod markets;
mod outcomes;
mod prices;
mod orderbooks;
mod trades;
mod resolutions;

pub use events::events_batch;
pub use markets::markets_batch;
pub use outcomes::outcomes_batch;
pub use prices::prices_batch;
pub use orderbooks::{book_levels_batch, new_snapshot_id, orderbooks_batch, SnapshotRecord};
pub use trades::trades_batch;
pub use resolutions::resolutions_batch;

use chrono::{DateTime, Utc};

pub fn parse_ts(raw: Option<&str>) -> Option<DateTime<Utc>> {
    raw.and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|| {
            raw.and_then(|value| {
                value
                    .parse::<i64>()
                    .ok()
                    .and_then(|secs| DateTime::from_timestamp(secs, 0))
            })
        })
}

pub fn parse_f64(raw: Option<&str>) -> Option<f64> {
    raw.and_then(|value| value.parse::<f64>().ok())
}

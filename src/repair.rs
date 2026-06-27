use std::path::Path;

use crate::config::SyncMarketsOptions;
use crate::error::Result;

pub async fn run(out: &Path) -> Result<()> {
    let options = SyncMarketsOptions {
        out: out.to_path_buf(),
        active: true,
        closed: false,
        all: false,
        tag: None,
        since: None,
        limit: None,
        resume: false,
        overwrite: true,
        gamma_base_url: crate::config::DEFAULT_GAMMA_BASE_URL.into(),
        requests_per_second: crate::config::DEFAULT_REQUESTS_PER_SECOND,
        max_retries: crate::config::DEFAULT_MAX_RETRIES,
        user_agent: crate::config::DEFAULT_USER_AGENT.into(),
        raw_retention_days: crate::config::DEFAULT_RAW_RETENTION_DAYS,
    };
    crate::sync::sync_markets(options).await?;
    Ok(())
}

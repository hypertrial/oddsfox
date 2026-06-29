use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::config::WatchOptions;
use crate::error::{OddsfoxError, Result};
use crate::manifest::{new_run_id, ManifestStore};
use crate::paths::LakePaths;
use crate::quarantine::{sha256_hex, write_raw_json};

pub async fn watch_markets(options: WatchOptions) -> Result<()> {
    let paths = LakePaths::new(&options.out);
    let store = ManifestStore::open(&options.out)?;
    let run_id = new_run_id();
    let started = Utc::now();
    let run = store.start_run("watch", &run_id, started)?;
    let token_ids = resolve_watch_tokens(&options).await?;
    if token_ids.is_empty() {
        return Err(OddsfoxError::SyncIncomplete {
            message: "no token ids selected for watch".into(),
        });
    }

    let (ws_stream, _) = connect_async(&options.ws_url)
        .await
        .map_err(|err| OddsfoxError::WebSocket(err.to_string()))?;
    let (mut write, mut read) = ws_stream.split();

    let subscribe = serde_json::json!({
        "assets_ids": token_ids,
        "type": "market",
    });
    write
        .send(Message::Text(subscribe.to_string().into()))
        .await
        .map_err(|err| OddsfoxError::WebSocket(err.to_string()))?;

    let raw_log = paths
        .raw_dir("websocket")
        .join(format!("watch-{run_id}.jsonl"));
    paths.ensure_parent(&raw_log)?;

    let mut seen = HashSet::new();
    let mut events = 0_i64;
    while let Some(msg) = read.next().await {
        let msg = msg.map_err(|err| OddsfoxError::WebSocket(err.to_string()))?;
        if let Message::Text(text) = msg {
            let body = text.as_bytes();
            let filename = format!("ws-{}-{}.json", events, &sha256_hex(body)[..8]);
            write_raw_json(&paths, "websocket", &filename, body)?;
            append_raw_log(&raw_log, body)?;
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(event_type) = value.get("event_type").and_then(|v| v.as_str()) {
                    seen.insert(event_type.to_string());
                }
            }
            events += 1;
            if events >= 100 {
                break;
            }
        }
    }

    run.complete(events)?;

    println!(
        "watch complete: recorded {events} websocket events ({})",
        seen.into_iter().collect::<Vec<_>>().join(", ")
    );
    Ok(())
}

async fn resolve_watch_tokens(options: &WatchOptions) -> Result<Vec<String>> {
    if let Some(market_id) = &options.market_id {
        return crate::sync::token_ids_for_market(&options.out, market_id).await;
    }
    crate::sync::top_token_ids(&options.out, options.top_volume.unwrap_or(25)).await
}

fn append_raw_log(path: &std::path::Path, body: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(body)?;
    file.write_all(b"\n")?;
    Ok(())
}

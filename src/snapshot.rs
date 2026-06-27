use std::fs;
use std::io::{BufRead, BufReader};

use chrono::Utc;

use crate::clob::book::parse_book;
use crate::clob::ClobClient;
use crate::config::{SnapshotBooksOptions, Table, TopBy};
use crate::error::Result;
use crate::http::HttpClient;
use crate::manifest::{new_run_id, ManifestStore, RunRecord};
use crate::normalize::{book_levels_batch, new_snapshot_id, orderbooks_batch, SnapshotRecord};
use crate::parquet::write_snapshot;
use crate::paths::LakePaths;
use crate::sync::{token_ids_for_market, top_token_ids};

pub async fn snapshot_books(options: SnapshotBooksOptions) -> Result<()> {
    let paths = LakePaths::new(&options.out);
    let store = ManifestStore::open(&options.out)?;
    let run_id = new_run_id();
    let started = Utc::now();
    let http = HttpClient::new(
        options.requests_per_second,
        options.max_retries,
        options.user_agent.clone(),
    )?;
    let clob = ClobClient::new(options.clob_base_url.clone(), http);

    let token_ids = resolve_tokens(&options).await?;
    let mut records = Vec::new();
    for token_id in token_ids {
        let book = clob.get_book(&token_id).await?;
        let parsed = parse_book(&book);
        records.push(SnapshotRecord {
            snapshot_id: new_snapshot_id(),
            token_id,
            market_id: book.market.clone(),
            book,
            parsed,
        });
    }

    if records.is_empty() {
        println!("snapshot books: no tokens selected");
        return Ok(());
    }

    let books_batch = orderbooks_batch(&records, "clob_book", &run_id)?;
    let levels_batch = book_levels_batch(&records, "clob_book", &run_id)?;
    write_snapshot(&paths, Table::Orderbooks, &run_id, &[books_batch])?;
    write_snapshot(&paths, Table::BookLevels, &run_id, &[levels_batch])?;

    store.append_run(RunRecord {
        run_id: run_id.clone(),
        command: "snapshot books".into(),
        started_at: started,
        finished_at: Some(Utc::now()),
        status: "complete".into(),
        rows_written: records.len() as i64,
        oddsfox_version: env!("CARGO_PKG_VERSION").into(),
    })?;
    println!("snapshot books complete: {} snapshots (run={run_id})", records.len());
    Ok(())
}

async fn resolve_tokens(options: &SnapshotBooksOptions) -> Result<Vec<String>> {
    if let Some(path) = &options.tokens_file {
        let file = fs::File::open(path)?;
        let reader = BufReader::new(file);
        return Ok(reader.lines().map_while(|line| line.ok()).collect());
    }
    if let Some(market_id) = &options.market_id {
        return token_ids_for_market(&options.out, market_id).await;
    }
    if options.active {
        return top_token_ids(&options.out, options.top_volume.unwrap_or(50)).await;
    }
    Ok(Vec::new())
}

pub fn top_markets(out: &std::path::Path, by: TopBy, limit: usize) -> Result<Vec<MarketSummary>> {
    let paths = LakePaths::new(out);
    let glob = paths.duckdb_parquet_glob(Table::Markets);
    let order = match by {
        TopBy::Volume24h => "volume_24h DESC NULLS LAST",
        TopBy::Spread => "volume_24h DESC NULLS LAST",
        TopBy::Liquidity => "liquidity DESC NULLS LAST",
        TopBy::Volume => "volume DESC NULLS LAST",
    };
    let conn = crate::duckdb_engine::open_connection(None)?;
    let sql = format!(
        "SELECT market_id, question, active, volume_24h, liquidity
         FROM read_parquet('{glob}')
         ORDER BY {order}
         LIMIT {limit}"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok(MarketSummary {
            market_id: row.get(0)?,
            question: row.get(1)?,
            active: row.get(2)?,
            volume_24h: row.get(3)?,
            liquidity: row.get(4)?,
        })
    })?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MarketSummary {
    pub market_id: String,
    pub question: Option<String>,
    pub active: Option<bool>,
    pub volume_24h: Option<f64>,
    pub liquidity: Option<f64>,
}

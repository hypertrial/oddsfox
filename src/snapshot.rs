use std::fs;
use std::io::{BufRead, BufReader};

use chrono::Utc;

use crate::clob::book::parse_book;
use crate::clob::ClobClient;
use crate::config::{SnapshotBooksOptions, Table, TopBy};
use crate::duckdb_engine::{bronze_source_sql, open_connection};
use crate::error::Result;
use crate::http::HttpClient;
use crate::manifest::{new_run_id, ManifestStore};
use crate::normalize::{book_levels_batch, new_snapshot_id, orderbooks_batch, SnapshotRecord};
use crate::parquet::write_snapshot;
use crate::paths::LakePaths;
use crate::sync::{token_ids_for_market, top_token_ids};

pub async fn snapshot_books(options: SnapshotBooksOptions) -> Result<()> {
    let paths = LakePaths::new(&options.out);
    let store = ManifestStore::open(&options.out)?;
    let run_id = new_run_id();
    let started = Utc::now();
    let run = store.start_run("snapshot books", &run_id, started)?;
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
        run.complete(0)?;
        return Ok(());
    }

    let books_batch = orderbooks_batch(&records, "clob_book", &run_id)?;
    let levels_batch = book_levels_batch(&records, "clob_book", &run_id)?;
    write_snapshot(&paths, Table::Orderbooks, &run_id, &[books_batch])?;
    write_snapshot(&paths, Table::BookLevels, &run_id, &[levels_batch])?;

    run.complete(records.len() as i64)?;
    println!(
        "snapshot books complete: {} snapshots (run={run_id})",
        records.len()
    );
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

pub fn top_markets(
    out: &std::path::Path,
    by: TopBy,
    limit: usize,
    active: Option<bool>,
    tag: Option<&str>,
) -> Result<Vec<MarketSummary>> {
    let paths = LakePaths::new(out);
    let markets_source = bronze_source_sql(&paths, Table::Markets);
    let orderbooks_glob = paths.duckdb_parquet_glob(Table::Orderbooks);
    let has_orderbooks = crate::duckdb_engine::glob_exists(&orderbooks_glob);
    let events_glob = paths.duckdb_parquet_glob(Table::Events);
    if tag.is_some() && !crate::duckdb_engine::glob_exists(&events_glob) {
        return Ok(Vec::new());
    }

    let order = match (by, has_orderbooks) {
        (TopBy::Volume24h, _) => "m.volume_24h DESC NULLS LAST",
        (TopBy::Spread, true) => "spread ASC NULLS LAST, m.volume_24h DESC NULLS LAST",
        (TopBy::Spread, false) => "m.volume_24h DESC NULLS LAST",
        (TopBy::Liquidity, _) => "m.liquidity DESC NULLS LAST",
        (TopBy::Volume, _) => "m.volume DESC NULLS LAST",
    };
    let conn = open_connection(None)?;
    let mut sql = format!(
        "SELECT m.market_id, m.question, m.active, m.volume_24h, m.liquidity, {} AS spread
         FROM {markets_source} m",
        if has_orderbooks {
            "ob.spread"
        } else {
            "CAST(NULL AS DOUBLE)"
        }
    );
    if has_orderbooks {
        let orderbooks_source = bronze_source_sql(&paths, Table::Orderbooks);
        sql.push_str(&format!(
            " LEFT JOIN (
                SELECT market_id, spread,
                       ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY ts DESC) AS rn
                FROM {orderbooks_source}
                WHERE spread IS NOT NULL
              ) ob ON ob.market_id = m.market_id AND ob.rn = 1"
        ));
    }
    let mut params = Vec::new();
    if tag.is_some() {
        let events_source = bronze_source_sql(&paths, Table::Events);
        sql.push_str(&format!(
            " JOIN {events_source} e ON m.event_id = e.event_id"
        ));
    }
    sql.push_str(" WHERE true");
    if let Some(active) = active {
        sql.push_str(if active {
            " AND m.active = true"
        } else {
            " AND m.active = false"
        });
    }
    if let Some(tag) = tag {
        sql.push_str(" AND e.tags LIKE ?");
        params.push(format!("%{tag}%"));
    }
    sql.push_str(&format!(" ORDER BY {order} LIMIT {limit}"));
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(duckdb::params_from_iter(params.iter()), |row| {
        Ok(MarketSummary {
            market_id: row.get(0)?,
            question: row.get(1)?,
            active: row.get(2)?,
            volume_24h: row.get(3)?,
            liquidity: row.get(4)?,
            spread: row.get(5)?,
        })
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MarketSummary {
    pub market_id: String,
    pub question: Option<String>,
    pub active: Option<bool>,
    pub volume_24h: Option<f64>,
    pub liquidity: Option<f64>,
    pub spread: Option<f64>,
}

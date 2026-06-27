use chrono::NaiveDate;

use crate::config::Table;
use crate::error::Result;
use crate::paths::LakePaths;

pub fn compute_accuracy_metrics(out: &std::path::Path, since: Option<NaiveDate>) -> Result<i64> {
    let paths = LakePaths::new(out);
    let markets_glob = paths.duckdb_parquet_glob(Table::Markets);
    let outcomes_glob = paths.duckdb_parquet_glob(Table::Outcomes);
    let prices_glob = paths.duckdb_parquet_glob(Table::Prices);
    let conn = crate::duckdb_engine::open_connection(None)?;
    let since_filter = since
        .map(|d| format!("AND m.resolution_time >= '{}'", d))
        .unwrap_or_default();
    let sql = format!(
        "SELECT m.market_id, o.token_id, o.is_winner, p.price
         FROM read_parquet('{markets_glob}') m
         JOIN read_parquet('{outcomes_glob}') o ON m.market_id = o.market_id
         LEFT JOIN (
           SELECT market_id, token_id, price,
                  ROW_NUMBER() OVER (PARTITION BY token_id ORDER BY ts DESC) AS rn
           FROM read_parquet('{prices_glob}')
         ) p ON p.token_id = o.token_id AND p.rn = 1
         WHERE m.resolved = true {since_filter}"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, bool>(2)?,
            row.get::<_, Option<f64>>(3)?,
        ))
    })?;

    let mut count = 0_i64;
    for row in rows.flatten() {
        let (_market, _token, winner, price) = row;
        if let Some(p) = price {
            let outcome = if winner { 1.0 } else { 0.0 };
            let brier = (p - outcome).powi(2);
            let log_loss = if outcome == 1.0 {
                -p.max(1e-9).ln()
            } else {
                -(1.0 - p).max(1e-9).ln()
            };
            let _ = (brier, log_loss);
            count += 1;
        }
    }
    Ok(count)
}

pub fn brier_score(probability: f64, outcome: f64) -> f64 {
    (probability - outcome).powi(2)
}

pub fn log_loss(probability: f64, outcome: f64) -> f64 {
    let p = probability.clamp(1e-9, 1.0 - 1e-9);
    if outcome >= 0.5 {
        -p.ln()
    } else {
        -(1.0 - p).ln()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn brier_perfect_prediction() {
        assert!((brier_score(1.0, 1.0) - 0.0).abs() < 1e-9);
    }

    #[test]
    fn log_loss_bounds() {
        assert!(log_loss(0.5, 1.0) > 0.0);
    }
}

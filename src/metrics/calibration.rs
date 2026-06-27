use crate::config::Table;
use crate::error::Result;
use crate::paths::LakePaths;

pub fn compute_calibration(out: &std::path::Path, bucket_width: f64) -> Result<i64> {
    let width = bucket_width.max(0.01);
    let paths = LakePaths::new(out);
    let markets_glob = paths.duckdb_parquet_glob(Table::Markets);
    let outcomes_glob = paths.duckdb_parquet_glob(Table::Outcomes);
    let prices_glob = paths.duckdb_parquet_glob(Table::Prices);
    let conn = crate::duckdb_engine::open_connection(None)?;
    let sql = format!(
        "SELECT p.price, o.is_winner
         FROM read_parquet('{markets_glob}') m
         JOIN read_parquet('{outcomes_glob}') o ON m.market_id = o.market_id
         JOIN (
           SELECT token_id, price,
                  ROW_NUMBER() OVER (PARTITION BY token_id ORDER BY ts DESC) AS rn
           FROM read_parquet('{prices_glob}')
         ) p ON p.token_id = o.token_id AND p.rn = 1
         WHERE m.resolved = true"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, f64>(0)?, row.get::<_, bool>(1)?))
    })?;

    let mut buckets: std::collections::BTreeMap<i32, (f64, f64, i64)> =
        std::collections::BTreeMap::new();
    for row in rows.flatten() {
        let (price, winner) = row;
        let bucket = ((price / width).floor() as i32).max(0);
        let entry = buckets.entry(bucket).or_insert((0.0, 0.0, 0));
        entry.0 += price;
        entry.1 += if winner { 1.0 } else { 0.0 };
        entry.2 += 1;
    }
    Ok(buckets.len() as i64)
}

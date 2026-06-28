use arrow::array::{
    Float64Builder, Int64Builder, RecordBatch, StringBuilder, TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use crate::config::Table;
use crate::duckdb_engine::{open_connection, read_parquet_sql};
use crate::error::Result;
use crate::paths::LakePaths;

pub fn compute_calibration(out: &std::path::Path, bucket_width: f64) -> Result<i64> {
    let width = bucket_width.max(0.01);
    let paths = LakePaths::new(out);
    let markets_glob = paths.duckdb_parquet_glob(Table::Markets);
    let outcomes_glob = paths.duckdb_parquet_glob(Table::Outcomes);
    let prices_glob = paths.duckdb_parquet_glob(Table::Prices);
    let markets_source = read_parquet_sql(&markets_glob);
    let outcomes_source = read_parquet_sql(&outcomes_glob);
    let prices_source = read_parquet_sql(&prices_glob);
    let conn = open_connection(None)?;
    let sql = format!(
        "SELECT p.price, o.is_winner
         FROM {markets_source} m
         JOIN {outcomes_source} o ON m.market_id = o.market_id
         JOIN (
           SELECT token_id, price,
                  ROW_NUMBER() OVER (PARTITION BY token_id ORDER BY ts DESC) AS rn
           FROM {prices_source}
         ) p ON p.token_id = o.token_id AND p.rn = 1
         WHERE m.resolved = true"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, f64>(0)?, row.get::<_, bool>(1)?))
    })?;

    let mut buckets: std::collections::BTreeMap<i32, (f64, f64, i64)> =
        std::collections::BTreeMap::new();
    for row in rows {
        let row = row?;
        let (price, winner) = row;
        let bucket = ((price / width).floor() as i32).max(0);
        let entry = buckets.entry(bucket).or_insert((0.0, 0.0, 0));
        entry.0 += price;
        entry.1 += if winner { 1.0 } else { 0.0 };
        entry.2 += 1;
    }
    let mut bucket_start = Float64Builder::new();
    let mut bucket_end = Float64Builder::new();
    let mut mean_prediction = Float64Builder::new();
    let mut observed_rate = Float64Builder::new();
    let mut sample_count = Int64Builder::new();
    let mut ts = TimestampMillisecondBuilder::new();
    let mut source_version = StringBuilder::new();
    let now = chrono::Utc::now().timestamp_millis();

    for (bucket, (prediction_sum, outcome_sum, count)) in buckets {
        let count_f64 = count as f64;
        let start = f64::from(bucket) * width;
        bucket_start.append_value(start);
        bucket_end.append_value((start + width).min(1.0));
        mean_prediction.append_value(prediction_sum / count_f64);
        observed_rate.append_value(outcome_sum / count_f64);
        sample_count.append_value(count);
        ts.append_value(now);
        source_version.append_value(crate::schema::schema_version());
    }

    let batch = RecordBatch::try_new(
        calibration_schema(),
        vec![
            std::sync::Arc::new(bucket_start.finish()),
            std::sync::Arc::new(bucket_end.finish()),
            std::sync::Arc::new(mean_prediction.finish()),
            std::sync::Arc::new(observed_rate.finish()),
            std::sync::Arc::new(sample_count.finish()),
            std::sync::Arc::new(ts.finish()),
            std::sync::Arc::new(source_version.finish()),
        ],
    )?;
    let rows = batch.num_rows() as i64;
    if rows > 0 {
        crate::parquet::write_gold(&paths, "calibration", "calibration", &[batch])?;
    }
    Ok(rows)
}

fn calibration_schema() -> std::sync::Arc<Schema> {
    std::sync::Arc::new(Schema::new(vec![
        Field::new("bucket_start", DataType::Float64, false),
        Field::new("bucket_end", DataType::Float64, false),
        Field::new("mean_prediction", DataType::Float64, false),
        Field::new("observed_rate", DataType::Float64, false),
        Field::new("sample_count", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("source_version", DataType::Utf8, false),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clob::rest::PriceHistoryPoint;
    use crate::gamma::GammaMarket;
    use crate::normalize::{markets_batch, outcomes_batch, prices_batch};
    use crate::parquet::write_snapshot;

    #[test]
    fn compute_calibration_writes_gold_buckets() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        paths.scaffold_dirs().unwrap();
        let market = resolved_market();
        write_snapshot(
            &paths,
            Table::Markets,
            "run-1",
            &[markets_batch(
                std::slice::from_ref(&market),
                "gamma",
                "http://test",
                "sha",
                "run-1",
            )
            .unwrap()],
        )
        .unwrap();
        write_snapshot(
            &paths,
            Table::Outcomes,
            "run-1",
            &[outcomes_batch(&[market], "gamma", "http://test", "sha", "run-1").unwrap()],
        )
        .unwrap();
        crate::parquet::write_token_series(
            &paths,
            Table::Prices,
            "tok-yes",
            &[prices_batch(
                "tok-yes",
                Some("m1"),
                &[PriceHistoryPoint { t: 1, p: 0.8 }],
                "test",
                Some(60),
                "run-1",
            )
            .unwrap()],
        )
        .unwrap();

        assert_eq!(compute_calibration(dir.path(), 0.05).unwrap(), 1);
        assert!(crate::duckdb_engine::glob_exists(
            &paths.layer_parquet_glob("gold", "calibration")
        ));
    }

    fn resolved_market() -> GammaMarket {
        GammaMarket {
            id: "m1".into(),
            event_id: Some("e1".into()),
            conditionId: None,
            questionID: None,
            slug: None,
            question: Some("Resolved?".into()),
            description: None,
            active: Some(false),
            closed: Some(true),
            resolved: Some(true),
            enableOrderBook: None,
            negRisk: None,
            liquidity: None,
            volume: None,
            volume24hr: None,
            openInterest: None,
            endDate: None,
            resolutionTime: Some("2024-01-01T00:00:00Z".into()),
            resolutionSource: Some("test".into()),
            outcomes: Some("[\"Yes\",\"No\"]".into()),
            outcomePrices: None,
            clobTokenIds: Some("[\"tok-yes\",\"tok-no\"]".into()),
            winningOutcome: None,
            winningOutcomeIndex: Some(0),
        }
    }
}

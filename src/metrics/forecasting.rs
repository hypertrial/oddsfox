use chrono::NaiveDate;

use arrow::array::{
    BooleanBuilder, Float64Builder, RecordBatch, StringBuilder, TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use crate::config::Table;
use crate::duckdb_engine::{bronze_source_sql, open_connection, read_parquet_sql};
use crate::error::Result;
use crate::paths::LakePaths;

pub fn compute_accuracy_metrics(out: &std::path::Path, since: Option<NaiveDate>) -> Result<i64> {
    let paths = LakePaths::new(out);
    let prices_glob = paths.duckdb_parquet_glob(Table::Prices);
    let markets_source = bronze_source_sql(&paths, Table::Markets);
    let outcomes_source = bronze_source_sql(&paths, Table::Outcomes);
    let prices_source = read_parquet_sql(&prices_glob);
    let conn = open_connection(None)?;
    let since_filter = since
        .map(|d| format!("AND m.resolution_time >= '{}'", d))
        .unwrap_or_default();
    let sql = format!(
        "SELECT m.market_id, o.token_id, o.is_winner, p.price
         FROM {markets_source} m
         JOIN {outcomes_source} o ON m.market_id = o.market_id
         LEFT JOIN (
           SELECT market_id, token_id, price,
                  ROW_NUMBER() OVER (PARTITION BY token_id ORDER BY ts DESC) AS rn
           FROM {prices_source}
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

    let mut market_id = StringBuilder::new();
    let mut token_id = StringBuilder::new();
    let mut ts = TimestampMillisecondBuilder::new();
    let mut price = Float64Builder::new();
    let mut outcome = BooleanBuilder::new();
    let mut brier = Float64Builder::new();
    let mut log_loss_values = Float64Builder::new();
    let mut source_version = StringBuilder::new();
    let now = chrono::Utc::now().timestamp_millis();

    for row in rows {
        let (market, token, winner, row_price) = row?;
        if let Some(p) = row_price {
            let outcome_score = if winner { 1.0 } else { 0.0 };
            market_id.append_value(market);
            token_id.append_value(token);
            ts.append_value(now);
            price.append_value(p);
            outcome.append_value(winner);
            brier.append_value(brier_score(p, outcome_score));
            log_loss_values.append_value(log_loss(p, outcome_score));
            source_version.append_value(crate::schema::schema_version());
        }
    }

    let batch = RecordBatch::try_new(
        accuracy_schema(),
        vec![
            std::sync::Arc::new(market_id.finish()),
            std::sync::Arc::new(token_id.finish()),
            std::sync::Arc::new(ts.finish()),
            std::sync::Arc::new(price.finish()),
            std::sync::Arc::new(outcome.finish()),
            std::sync::Arc::new(brier.finish()),
            std::sync::Arc::new(log_loss_values.finish()),
            std::sync::Arc::new(source_version.finish()),
        ],
    )?;
    let rows = batch.num_rows() as i64;
    if rows > 0 {
        crate::parquet::write_gold(&paths, "accuracy", "accuracy", &[batch])?;
    }
    Ok(rows)
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

fn accuracy_schema() -> std::sync::Arc<Schema> {
    std::sync::Arc::new(Schema::new(vec![
        Field::new("market_id", DataType::Utf8, false),
        Field::new("token_id", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("price", DataType::Float64, false),
        Field::new("outcome", DataType::Boolean, false),
        Field::new("brier_score", DataType::Float64, false),
        Field::new("log_loss", DataType::Float64, false),
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
    fn brier_perfect_prediction() {
        assert!((brier_score(1.0, 1.0) - 0.0).abs() < 1e-9);
    }

    #[test]
    fn log_loss_bounds() {
        assert!(log_loss(0.5, 1.0) > 0.0);
    }

    #[test]
    fn compute_accuracy_writes_gold_rows() {
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
        crate::manifest::ManifestStore::open(dir.path())
            .unwrap()
            .append_completed_run("test", "run-1", chrono::Utc::now(), 2)
            .unwrap();

        assert_eq!(compute_accuracy_metrics(dir.path(), None).unwrap(), 1);
        assert!(crate::duckdb_engine::glob_exists(
            &paths.layer_parquet_glob("gold", "accuracy")
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

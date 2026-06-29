use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::get;
use axum::Router;
use serde::Deserialize;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;

use crate::config::{parse_date, ServeOptions, TopBy, UserSource};
use crate::duckdb_engine::{open_connection, read_parquet_sql};
use crate::error::Result;
use crate::explore::{event_detail, market_detail, resolved_markets, search};
use crate::metrics::market_metrics;
use crate::snapshot::top_markets;

#[derive(Clone)]
pub struct AppState {
    pub out: PathBuf,
}

pub async fn serve(options: ServeOptions) -> Result<()> {
    let state = AppState {
        out: options.out.clone(),
    };

    let web_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/web/static");
    let app = Router::new()
        .route("/health", get(health))
        .route("/markets", get(list_markets))
        .route("/markets/{market_id}", get(get_market))
        .route("/events", get(list_events))
        .route("/events/{event_id}", get(get_event))
        .route("/tokens/{token_id}/prices", get(token_prices))
        .route(
            "/markets/{market_id}/orderbook/latest",
            get(latest_orderbook),
        )
        .route("/markets/{market_id}/metrics", get(market_metrics_handler))
        .route("/metrics/calibration", get(calibration))
        .route("/metrics/liquidity", get(liquidity))
        .route("/pnl", get(pnl_handler))
        .route("/users/{user_id}/pnl", get(user_pnl_handler))
        .route("/resolved", get(resolved))
        .route("/search", get(search_handler))
        .nest_service("/", ServeDir::new(web_dir))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(Arc::new(state));

    let addr = format!("127.0.0.1:{}", options.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("oddsfox serve listening on http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

#[derive(Deserialize)]
struct MarketsQuery {
    active: Option<bool>,
    tag: Option<String>,
    order: Option<String>,
}

async fn list_markets(
    State(state): State<Arc<AppState>>,
    Query(query): Query<MarketsQuery>,
) -> impl IntoResponse {
    let by = match query.order.as_deref() {
        Some("spread") => TopBy::Spread,
        Some("liquidity") => TopBy::Liquidity,
        Some("volume") => TopBy::Volume,
        _ => TopBy::Volume24h,
    };
    match top_markets(&state.out, by, 50, query.active, query.tag.as_deref()) {
        Ok(markets) => Json(serde_json::json!({ "markets": markets })).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

async fn get_market(
    State(state): State<Arc<AppState>>,
    Path(market_id): Path<String>,
) -> impl IntoResponse {
    match market_detail(&state.out, &market_id) {
        Ok(market) => Json(market).into_response(),
        Err(err) => (StatusCode::NOT_FOUND, err.to_string()).into_response(),
    }
}

async fn list_events(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let glob =
        crate::paths::LakePaths::new(&state.out).duckdb_parquet_glob(crate::config::Table::Events);
    let source = read_parquet_sql(&glob);
    let conn = match open_connection(None) {
        Ok(c) => c,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };
    let sql = format!("SELECT event_id, title, active, closed FROM {source} LIMIT 50");
    let mut stmt = match conn.prepare(&sql) {
        Ok(s) => s,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };
    let rows = stmt.query_map([], |row| {
        Ok(serde_json::json!({
            "event_id": row.get::<_, String>(0)?,
            "title": row.get::<_, Option<String>>(1)?,
            "active": row.get::<_, Option<bool>>(2)?,
            "closed": row.get::<_, Option<bool>>(3)?,
        }))
    });
    match rows {
        Ok(iter) => match iter.collect::<std::result::Result<Vec<_>, _>>() {
            Ok(events) => Json(serde_json::json!({ "events": events })).into_response(),
            Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
        },
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

async fn get_event(
    State(state): State<Arc<AppState>>,
    Path(event_id): Path<String>,
) -> impl IntoResponse {
    match event_detail(&state.out, &event_id) {
        Ok(event) => Json(event).into_response(),
        Err(err) => (StatusCode::NOT_FOUND, err.to_string()).into_response(),
    }
}

async fn token_prices(
    State(state): State<Arc<AppState>>,
    Path(token_id): Path<String>,
) -> impl IntoResponse {
    let glob =
        crate::paths::LakePaths::new(&state.out).duckdb_parquet_glob(crate::config::Table::Prices);
    let source = read_parquet_sql(&glob);
    let conn = match open_connection(None) {
        Ok(c) => c,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };
    let sql = format!("SELECT ts, price FROM {source} WHERE token_id = ? ORDER BY ts");
    let mut stmt = match conn.prepare(&sql) {
        Ok(s) => s,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };
    let rows = stmt.query_map([&token_id], |row| {
        Ok(serde_json::json!({ "ts": row.get::<_, i64>(0)?, "price": row.get::<_, f64>(1)? }))
    });
    match rows {
        Ok(iter) => match iter.collect::<std::result::Result<Vec<_>, _>>() {
            Ok(prices) => Json(serde_json::json!({
                "token_id": token_id,
                "prices": prices
            }))
            .into_response(),
            Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
        },
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

async fn latest_orderbook(
    State(state): State<Arc<AppState>>,
    Path(market_id): Path<String>,
) -> impl IntoResponse {
    let glob = crate::paths::LakePaths::new(&state.out)
        .duckdb_parquet_glob(crate::config::Table::Orderbooks);
    let source = read_parquet_sql(&glob);
    let conn = match open_connection(None) {
        Ok(c) => c,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };
    let sql = format!(
        "SELECT snapshot_id, best_bid, best_ask, spread, midpoint
         FROM {source}
         WHERE market_id = ?
         ORDER BY ts DESC LIMIT 1"
    );
    match conn.query_row(&sql, [&market_id], |row| {
        Ok(serde_json::json!({
            "snapshot_id": row.get::<_, String>(0)?,
            "best_bid": row.get::<_, Option<f64>>(1)?,
            "best_ask": row.get::<_, Option<f64>>(2)?,
            "spread": row.get::<_, Option<f64>>(3)?,
            "midpoint": row.get::<_, Option<f64>>(4)?,
        }))
    }) {
        Ok(book) => Json(book).into_response(),
        Err(err) => (StatusCode::NOT_FOUND, err.to_string()).into_response(),
    }
}

async fn market_metrics_handler(
    State(state): State<Arc<AppState>>,
    Path(market_id): Path<String>,
) -> impl IntoResponse {
    match market_metrics(&state.out, &market_id) {
        Ok(metrics) => Json(serde_json::json!({ "metrics": metrics })).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

async fn calibration(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let rows = crate::metrics::compute_calibration(&state.out, 0.05);
    match rows {
        Ok(count) => Json(serde_json::json!({ "buckets": count })).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

async fn liquidity(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match crate::metrics::compute_liquidity_metrics(&state.out, true) {
        Ok(count) => Json(serde_json::json!({ "metric_points": count })).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct PnlQuery {
    source: Option<String>,
    user: Option<String>,
}

async fn pnl_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<PnlQuery>,
) -> impl IntoResponse {
    let source = match parse_user_source(query.source.as_deref()) {
        Ok(source) => source,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    match crate::user::pnl_rows(&state.out, source, query.user.as_deref()) {
        Ok(rows) => Json(serde_json::json!({ "pnl": rows })).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

async fn user_pnl_handler(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
    Query(query): Query<PnlQuery>,
) -> impl IntoResponse {
    let source = match parse_user_source(query.source.as_deref()) {
        Ok(source) => source,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    match crate::user::pnl_rows(&state.out, source, Some(&user_id)) {
        Ok(rows) => Json(serde_json::json!({ "user_id": user_id, "pnl": rows })).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

fn parse_user_source(raw: Option<&str>) -> std::result::Result<UserSource, String> {
    match raw.unwrap_or("all").to_ascii_lowercase().as_str() {
        "all" => Ok(UserSource::All),
        "polymarket" => Ok(UserSource::Polymarket),
        "kalshi" => Ok(UserSource::Kalshi),
        other => Err(format!("invalid source `{other}`")),
    }
}

#[derive(Deserialize)]
struct ResolvedQuery {
    since: Option<String>,
}

async fn resolved(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ResolvedQuery>,
) -> impl IntoResponse {
    let since = match parse_resolved_since(query.since.as_deref()) {
        Ok(since) => since,
        Err(err) => return (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    };
    match resolved_markets(&state.out, since) {
        Ok(markets) => Json(serde_json::json!({ "markets": markets })).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

fn parse_resolved_since(raw: Option<&str>) -> Result<Option<chrono::NaiveDate>> {
    raw.map(parse_date).transpose()
}

#[derive(Deserialize)]
struct SearchQuery {
    q: String,
}

async fn search_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<SearchQuery>,
) -> impl IntoResponse {
    match search(&state.out, &query.q) {
        Ok(hits) => Json(serde_json::json!({ "results": hits })).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

pub fn index_html() -> Html<&'static str> {
    Html(include_str!("../web/static/index.html"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_resolved_since_rejects_invalid_dates() {
        assert!(parse_resolved_since(Some("not-a-date")).is_err());
        assert_eq!(
            parse_resolved_since(Some("2024-01-31")).unwrap(),
            Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 31).unwrap())
        );
    }

    #[test]
    fn parse_user_source_defaults_to_all() {
        assert_eq!(parse_user_source(None).unwrap(), UserSource::All);
        assert_eq!(
            parse_user_source(Some("polymarket")).unwrap(),
            UserSource::Polymarket
        );
        assert!(parse_user_source(Some("bad")).is_err());
    }
}

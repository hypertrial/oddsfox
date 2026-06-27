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

use crate::config::{ServeOptions, TopBy};
use crate::error::Result;
use crate::explore::{event_detail, market_detail, resolved_markets, search};
use crate::metrics::market_metrics;
use crate::snapshot::top_markets;

#[derive(Clone)]
pub struct AppState {
    pub out: PathBuf,
    pub db: PathBuf,
}

pub async fn serve(options: ServeOptions) -> Result<()> {
    let duckdb_options = crate::config::DuckDbOptions {
        out: options.out.clone(),
        db: options.db.clone(),
    };
    let _ = crate::duckdb::run(&duckdb_options);

    let state = AppState {
        out: options.out.clone(),
        db: options.db.clone(),
    };

    let web_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/web/static");
    let app = Router::new()
        .route("/health", get(health))
        .route("/markets", get(list_markets))
        .route("/markets/{market_id}", get(get_market))
        .route("/events", get(list_events))
        .route("/events/{event_id}", get(get_event))
        .route("/tokens/{token_id}/prices", get(token_prices))
        .route("/markets/{market_id}/orderbook/latest", get(latest_orderbook))
        .route("/markets/{market_id}/metrics", get(market_metrics_handler))
        .route("/metrics/calibration", get(calibration))
        .route("/metrics/liquidity", get(liquidity))
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
    let _ = (query.active, query.tag);
    let by = match query.order.as_deref() {
        Some("spread") => TopBy::Spread,
        Some("liquidity") => TopBy::Liquidity,
        Some("volume") => TopBy::Volume,
        _ => TopBy::Volume24h,
    };
    match top_markets(&state.out, by, 50) {
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
    let glob = crate::paths::LakePaths::new(&state.out).duckdb_parquet_glob(crate::config::Table::Events);
    let conn = match crate::duckdb_engine::open_connection(None) {
        Ok(c) => c,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };
    let sql = format!("SELECT event_id, title, active, closed FROM read_parquet('{glob}') LIMIT 50");
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
        Ok(iter) => Json(serde_json::json!({
            "events": iter.filter_map(|r| r.ok()).collect::<Vec<_>>()
        }))
        .into_response(),
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
    let glob = crate::paths::LakePaths::new(&state.out)
        .duckdb_parquet_glob(crate::config::Table::Prices);
    let conn = match crate::duckdb_engine::open_connection(None) {
        Ok(c) => c,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };
    let sql = format!(
        "SELECT ts, price FROM read_parquet('{glob}') WHERE token_id = ? ORDER BY ts"
    );
    let mut stmt = match conn.prepare(&sql) {
        Ok(s) => s,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };
    let rows = stmt.query_map([&token_id], |row| {
        Ok(serde_json::json!({ "ts": row.get::<_, i64>(0)?, "price": row.get::<_, f64>(1)? }))
    });
    match rows {
        Ok(iter) => Json(serde_json::json!({
            "token_id": token_id,
            "prices": iter.filter_map(|r| r.ok()).collect::<Vec<_>>()
        }))
        .into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

async fn latest_orderbook(
    State(state): State<Arc<AppState>>,
    Path(market_id): Path<String>,
) -> impl IntoResponse {
    let glob = crate::paths::LakePaths::new(&state.out)
        .duckdb_parquet_glob(crate::config::Table::Orderbooks);
    let conn = match crate::duckdb_engine::open_connection(None) {
        Ok(c) => c,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };
    let sql = format!(
        "SELECT snapshot_id, best_bid, best_ask, spread, midpoint
         FROM read_parquet('{glob}')
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
struct ResolvedQuery {
    since: Option<String>,
}

async fn resolved(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ResolvedQuery>,
) -> impl IntoResponse {
    match resolved_markets(&state.out, query.since.as_deref()) {
        Ok(markets) => Json(serde_json::json!({ "markets": markets })).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
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

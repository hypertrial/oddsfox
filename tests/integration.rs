use std::fs;

use oddsfox::contract::build_contract;
use oddsfox::gamma::GammaEvent;
use oddsfox::normalize::{events_batch, markets_batch, outcomes_batch};
use oddsfox::paths::LakePaths;
use oddsfox::quarantine::sha256_hex;

#[test]
fn contract_matches_golden_file() {
    let contract = build_contract();
    let json = serde_json::to_string_pretty(&contract).unwrap();
    let golden_path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/contract.golden.json");
    if std::env::var("UPDATE_GOLDEN").ok().as_deref() == Some("1") {
        fs::write(&golden_path, &json).unwrap();
    }
    let golden = fs::read_to_string(golden_path).unwrap();
    assert_eq!(json, golden);
}

#[test]
fn gamma_fixture_normalizes_to_batches() {
    let raw = fs::read_to_string("tests/fixtures/gamma_event_response.json").unwrap();
    let events: Vec<GammaEvent> = serde_json::from_str(&raw).unwrap();
    let sha = sha256_hex(raw.as_bytes());
    let events_batch = events_batch(&events, "gamma", "http://test/events", &sha, "run-1").unwrap();
    assert_eq!(events_batch.num_rows(), 1);

    let markets: Vec<_> = events.iter().flat_map(|e| e.markets.clone()).collect();
    let markets_batch =
        markets_batch(&markets, "gamma", "http://test/events", &sha, "run-1").unwrap();
    assert_eq!(markets_batch.num_rows(), 1);

    let outcomes_batch =
        outcomes_batch(&markets, "gamma", "http://test/events", &sha, "run-1").unwrap();
    assert_eq!(outcomes_batch.num_rows(), 2);
}

#[test]
fn lake_paths_scaffold() {
    let dir = tempfile::tempdir().unwrap();
    let paths = LakePaths::new(dir.path());
    paths.scaffold_dirs().unwrap();
    assert!(paths.metadata_dir().exists());
    assert!(paths.bronze_dir().exists());
}

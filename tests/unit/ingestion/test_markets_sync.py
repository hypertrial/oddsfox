from oddsfox.ingestion.polymarket.odds import sync as odds_sync


def test_odds_sync_no_longer_accepts_compat_process_hook():
    assert "process_token_fn" not in odds_sync.sync_odds.__code__.co_varnames

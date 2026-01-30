from pathlib import Path


def test_etl_local_small():
    metrics_path = Path("data/output/quality_metrics.json")
    assert metrics_path.exists()

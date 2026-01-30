import json
from pathlib import Path


def test_quality_metrics_json():
    path = Path("data/output/quality_metrics.json")
    assert path.exists()
    data = json.loads(path.read_text(encoding="utf-8"))
    assert "run_ts" in data
    assert "counts" in data
    assert "completeness" in data
    assert "anomalies" in data
    assert "completeness_avg" in data

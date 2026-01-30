import os
import subprocess
from pathlib import Path


def test_etl_local_small():
    cmd = ["python", "-m", "etl.main", "--mode", "sample", "--config", "conf/config.yaml", "--input", "data/input/sample.jsonl"]
    env = os.environ.copy()
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    metrics_path = Path("data/output/quality_metrics.json")
    assert metrics_path.exists()

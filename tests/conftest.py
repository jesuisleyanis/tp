import os
import sys
import time
import subprocess
import mysql.connector
import pytest

_ran = False


def wait_mysql(env):
    host = env.get("MYSQL_HOST")
    port = int(env.get("MYSQL_PORT", "3306"))
    db = env.get("MYSQL_DB")
    user = env.get("MYSQL_USER")
    password = env.get("MYSQL_PASSWORD")
    for _ in range(30):
        try:
            conn = mysql.connector.connect(host=host, port=port, user=user, password=password, database=db)
            conn.close()
            return True
        except Exception:
            time.sleep(2)
    return False


def run_etl_once():
    global _ran
    if _ran:
        return
    env = os.environ.copy()
    if not wait_mysql(env):
        raise RuntimeError("MySQL not ready")
    cmd = [sys.executable, "-m", "etl.main", "--mode", "sample", "--config", "conf/config.yaml", "--input", "data/input/sample.jsonl"]
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stdout + result.stderr)
    _ran = True


@pytest.fixture(scope="session", autouse=True)
def ensure_etl():
    run_etl_once()

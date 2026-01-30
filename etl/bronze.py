import os
import json
import gzip
import requests
from pyspark.sql.types import StructType


def load_schema(schema_path):
    with open(schema_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return StructType.fromJson(data)


def download_file(url, dest_path):
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    with open(dest_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)


def download_sample_api(url, dest_path, size):
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    data = r.json()
    products = data.get("products", [])
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with open(dest_path, "w", encoding="utf-8") as f:
        for p in products[:size]:
            f.write(json.dumps(p, ensure_ascii=False))
            f.write("\n")


def ensure_sample(config):
    path = config["data"]["sample_path"]
    if os.path.exists(path):
        return path
    url = config["data"]["sample_url"]
    size = int(config["data"]["sample_size"])
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if url.endswith(".gz"):
        gz_path = path + ".gz"
        download_file(url, gz_path)
        with gzip.open(gz_path, "rb") as f_in:
            with open(path, "wb") as f_out:
                f_out.write(f_in.read())
        os.remove(gz_path)
    elif url.endswith(".jsonl"):
        download_file(url, path)
    else:
        download_sample_api(url, path, size)
    return path


def ensure_full(config, input_path=None):
    path = input_path or config["data"]["full_path"]
    if os.path.exists(path):
        return path
    url = config["data"].get("full_url")
    if not url:
        raise FileNotFoundError("Full export file not found and no full_url configured")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if url.endswith(".gz"):
        gz_path = path + ".gz"
        download_file(url, gz_path)
        with gzip.open(gz_path, "rb") as f_in:
            with open(path, "wb") as f_out:
                f_out.write(f_in.read())
        os.remove(gz_path)
    else:
        download_file(url, path)
    return path


def read_bronze(spark, config, mode, input_path=None):
    schema = load_schema("conf/schema_off.json")
    if mode == "sample":
        path = input_path or ensure_sample(config)
    else:
        path = ensure_full(config, input_path)
    fmt = config["data"]["format"]
    if fmt == "json":
        df = spark.read.schema(schema).json(path)
    elif fmt == "csv":
        df = spark.read.schema(schema).option("header", "true").option("sep", "\t").csv(path)
    else:
        raise ValueError("Unsupported format")
    return df

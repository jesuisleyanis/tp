import argparse
import os
import yaml
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from . import bronze, silver, gold, quality, scd2, taxonomies, io_mysql


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_mysql_driver(config):
    url = config.get("mysql", {}).get("jdbc_jar_url")
    jar_dir = os.path.join("data", "jars")
    jar_name = "mysql-connector-j-8.3.0.jar"
    jar_path = os.path.join(jar_dir, jar_name)
    if os.path.exists(jar_path):
        return jar_path
    if not url:
        raise ValueError("mysql.jdbc_jar_url is not configured")
    os.makedirs(jar_dir, exist_ok=True)
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    with open(jar_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    return jar_path


def spark_session(config):
    jar_path = ensure_mysql_driver(config)
    builder = SparkSession.builder.appName(config["spark"]["app_name"]).master(config["spark"]["master"])
    builder = builder.config("spark.sql.shuffle.partitions", str(config["spark"]["shuffle_partitions"]))
    builder = builder.config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
    builder = builder.config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
    builder = builder.config("spark.jars", jar_path)
    builder = builder.config("spark.driver.extraClassPath", jar_path)
    builder = builder.config("spark.executor.extraClassPath", jar_path)
    return builder.getOrCreate()


def ensure_dirs(paths):
    for p in paths:
        os.makedirs(os.path.dirname(p), exist_ok=True)


def upsert_dimension(df, config, table, key_cols, update_cols):
    stg = f"{table}_stg"
    io_mysql.write_staging(df, config, stg)
    io_mysql.upsert_from_staging(config, table, stg, key_cols, update_cols)
    io_mysql.drop_table(config, stg)


def upsert_bridge(df, config, table, key_cols):
    stg = f"{table}_stg"
    io_mysql.write_staging(df, config, stg)
    io_mysql.upsert_keys_from_staging(config, table, stg, key_cols)
    io_mysql.drop_table(config, stg)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["sample", "full"], default="sample")
    parser.add_argument("--config", default="conf/config.yaml")
    parser.add_argument("--input", default=None)
    args = parser.parse_args()

    config = load_config(args.config)

    ensure_dirs([config["quality"]["metrics_path"], config["quality"]["anomalies_path"]])

    spark = spark_session(config)

    try:
        io_mysql.run_sql_file(config, "sql/ddl_mysql.sql")
    except Exception as e:
        print("MySQL not ready or DDL failed", str(e))
        spark.stop()
        raise

    df_bronze = bronze.read_bronze(spark, config, args.mode, args.input)
    bronze_count = df_bronze.count()

    df_silver_pre = silver.transform(df_bronze, config, dedup=False)
    silver_pre_count = df_silver_pre.count()

    df_silver = silver.transform(df_bronze, config, dedup=True)
    silver_count = df_silver.count()

    filtered_invalid = max(bronze_count - silver_pre_count, 0)
    dedup_removed = max(silver_pre_count - silver_count, 0)

    df_quality = quality.add_quality_columns(df_silver, config)

    cat_url = config.get("taxonomy", {}).get("categories_url")
    country_url = config.get("taxonomy", {}).get("countries_url")
    categories_taxo = taxonomies.load_categories(spark, "conf/taxonomies/categories.json", cat_url)
    countries_taxo = taxonomies.load_countries(spark, "conf/taxonomies/countries.json", country_url)

    df_gold, dim_time, dim_brand, dim_category, dim_country = gold.build_dimensions(df_quality, config, categories_taxo, countries_taxo)
    bridge_product_category, bridge_product_country = gold.build_bridges(df_gold)
    dim_product_current = gold.build_dim_product_current(df_gold)

    upsert_dimension(dim_time, config, "dim_time", ["time_sk"], ["date", "year", "month", "week", "iso_week", "day"])
    upsert_dimension(dim_brand, config, "dim_brand", ["brand_sk"], ["brand_name"])
    upsert_dimension(dim_category, config, "dim_category", ["category_sk"], ["category_code", "category_name_fr", "level", "parent_category_sk", "category_level2"])
    upsert_dimension(dim_country, config, "dim_country", ["country_sk"], ["country_code", "country_name_fr"])

    scd2_counts = scd2.apply_scd2(spark, dim_product_current, config)

    mapping = scd2.current_product_mapping(spark, config)

    bridge_category = bridge_product_category.join(mapping, "code", "left").select("product_sk", "category_sk").where(F.col("product_sk").isNotNull())
    bridge_country = bridge_product_country.join(mapping, "code", "left").select("product_sk", "country_sk").where(F.col("product_sk").isNotNull())

    upsert_bridge(bridge_category, config, "bridge_product_category", ["product_sk", "category_sk"])
    upsert_bridge(bridge_country, config, "bridge_product_country", ["product_sk", "country_sk"])

    fact = gold.build_fact_snapshot(df_gold, dim_time)
    fact = fact.join(mapping, "code", "left")
    fact = fact.withColumn("quality_issues_json", F.to_json(F.col("quality_issues")))
    fact = fact.drop("quality_issues")
    fact = fact.select(
        "product_sk",
        "time_sk",
        "sugars_100g",
        "salt_100g",
        "sodium_100g",
        "fat_100g",
        "saturated_fat_100g",
        "proteins_100g",
        "fiber_100g",
        "energy_kcal_100g",
        "nutriscore_grade",
        "nova_group",
        "ecoscore_grade",
        "completeness_score",
        "quality_issues_json"
    )
    fact = fact.filter(F.col("product_sk").isNotNull() & F.col("time_sk").isNotNull())

    io_mysql.write_staging(fact, config, "fact_nutrition_snapshot_stg")
    io_mysql.upsert_from_staging(
        config,
        "fact_nutrition_snapshot",
        "fact_nutrition_snapshot_stg",
        ["product_sk", "time_sk"],
        [
            "sugars_100g",
            "salt_100g",
            "sodium_100g",
            "fat_100g",
            "saturated_fat_100g",
            "proteins_100g",
            "fiber_100g",
            "energy_kcal_100g",
            "nutriscore_grade",
            "nova_group",
            "ecoscore_grade",
            "completeness_score",
            "quality_issues_json"
        ]
    )
    io_mysql.drop_table(config, "fact_nutrition_snapshot_stg")

    gold_count = fact.count()

    counts = {
        "bronze_read": bronze_count,
        "filtered_invalid": filtered_invalid,
        "silver_after_dedup": silver_count,
        "dedup_removed": dedup_removed,
        "gold_fact_rows": gold_count,
        "scd2_updates": scd2_counts["scd2_updates"],
        "scd2_inserts": scd2_counts["scd2_inserts"]
    }

    metrics = quality.compute_metrics(df_quality, counts, config)
    quality.write_metrics(metrics, config["quality"]["metrics_path"])
    quality.anomalies_sample(df_quality, config["quality"]["anomalies_path"])

    spark.stop()


if __name__ == "__main__":
    main()

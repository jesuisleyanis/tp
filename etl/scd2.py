from datetime import datetime
from pyspark.sql import functions as F
from . import io_mysql


def read_table(spark, config, table):
    url, user, password = io_mysql.jdbc_url(config)
    return spark.read.format("jdbc").option("url", url).option("dbtable", table).option("user", user).option("password", password).option("driver", "com.mysql.cj.jdbc.Driver").load()


def apply_scd2(spark, df_current, config):
    load_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    tracked_cols = ["product_name_resolved", "brand_sk", "category_sk", "country_sk", "nutriscore_grade", "nova_group", "ecoscore_grade"]
    df_h = df_current
    df_h = df_h.withColumn("attr_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked_cols]), 256))
    df_h = df_h.withColumn("effective_from", F.lit(load_ts))
    df_h = df_h.withColumn("effective_to", F.lit(None).cast("timestamp"))
    df_h = df_h.withColumn("is_current", F.lit(1))

    existing = read_table(spark, config, "dim_product")
    existing_current = existing.filter(F.col("is_current") == 1).select(
        F.col("code"),
        F.col("attr_hash").alias("attr_hash_existing"),
        F.col("product_sk")
    )

    joined = df_h.join(existing_current, "code", "left")

    changed = joined.filter(F.col("product_sk").isNotNull() & (F.col("attr_hash") != F.col("attr_hash_existing")))
    new_rows = joined.filter(F.col("product_sk").isNull())

    to_update = changed.select("product_sk").withColumn("effective_to", F.lit(load_ts))
    to_insert = new_rows.select(df_h.columns).unionByName(changed.select(df_h.columns))

    updates_count = to_update.count()
    inserts_count = to_insert.count()

    if updates_count > 0:
        io_mysql.write_staging(to_update, config, "dim_product_updates")
        conn = io_mysql.connect_mysql(config)
        io_mysql.run_sql(conn, "UPDATE dim_product dp JOIN dim_product_updates u ON dp.product_sk=u.product_sk SET dp.effective_to=u.effective_to, dp.is_current=0")
        conn.close()
        io_mysql.drop_table(config, "dim_product_updates")

    if inserts_count > 0:
        io_mysql.write_staging(to_insert.select(
            "code",
            "product_name_resolved",
            "brand_sk",
            "category_sk",
            "country_sk",
            "nutriscore_grade",
            "nova_group",
            "ecoscore_grade",
            "attr_hash",
            "effective_from",
            "effective_to",
            "is_current"
        ), config, "dim_product_inserts")
        conn = io_mysql.connect_mysql(config)
        sql = "INSERT INTO dim_product (code, product_name_resolved, brand_sk, category_sk, country_sk, nutriscore_grade, nova_group, ecoscore_grade, attr_hash, effective_from, effective_to, is_current) SELECT code, product_name_resolved, brand_sk, category_sk, country_sk, nutriscore_grade, nova_group, ecoscore_grade, attr_hash, effective_from, effective_to, is_current FROM dim_product_inserts"
        io_mysql.run_sql(conn, sql)
        conn.close()
        io_mysql.drop_table(config, "dim_product_inserts")

    return {"scd2_updates": updates_count, "scd2_inserts": inserts_count}


def current_product_mapping(spark, config):
    df = read_table(spark, config, "dim_product")
    return df.filter(F.col("is_current") == 1).select("product_sk", "code")

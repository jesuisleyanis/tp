import os
import requests
from pyspark.sql import functions as F


def ensure_taxonomy_file(path, url=None):
    if os.path.exists(path):
        return path
    if not url:
        return None
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(r.content)
    return path


def load_categories(spark, path, url=None):
    p = ensure_taxonomy_file(path, url)
    if not p:
        return None
    return spark.read.option("multiLine", "true").json(p)


def load_countries(spark, path, url=None):
    p = ensure_taxonomy_file(path, url)
    if not p:
        return None
    return spark.read.option("multiLine", "true").json(p)


def enrich_categories(df, categories_taxo):
    if categories_taxo is None:
        return df.withColumn("category_level2", F.col("category_primary")).withColumn("category_parent", F.lit(None))
    cat = F.broadcast(categories_taxo.select(
        F.col("tag").alias("category_tag"),
        F.col("name").alias("category_name"),
        F.col("level").alias("category_level"),
        F.col("parent_tag").alias("category_parent"),
        F.col("level2").alias("category_level2")
    ))
    df2 = df.withColumn("category_tag", F.col("category_primary"))
    df2 = df2.join(cat, df2.category_tag == cat.category_tag, "left").drop(cat.category_tag)
    df2 = df2.withColumn("category_level2", F.coalesce(F.col("category_level2"), F.col("category_primary")))
    return df2


def enrich_countries(df, countries_taxo):
    if countries_taxo is None:
        return df.withColumn("country_name", F.col("country_primary"))
    c = F.broadcast(countries_taxo.select(F.col("tag").alias("country_tag"), F.col("name").alias("country_name")))
    df2 = df.withColumn("country_tag", F.col("country_primary"))
    df2 = df2.join(c, df2.country_tag == c.country_tag, "left").drop(c.country_tag)
    df2 = df2.withColumn("country_name", F.coalesce(F.col("country_name"), F.col("country_primary")))
    return df2

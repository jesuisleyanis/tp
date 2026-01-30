from pyspark.sql import functions as F
from . import taxonomies


def stable_sk(col):
    return F.when(col.isNull(), F.lit(None)).otherwise(F.abs(F.xxhash64(col)))


def build_dimensions(df, config, categories_taxo=None, countries_taxo=None):
    df2 = taxonomies.enrich_categories(df, categories_taxo)
    df2 = taxonomies.enrich_countries(df2, countries_taxo)

    dim_time = df2.select(F.to_date("last_modified_ts").alias("date")).where(F.col("date").isNotNull()).distinct()
    dim_time = dim_time.withColumn("time_sk", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))
    dim_time = dim_time.withColumn("year", F.year("date"))
    dim_time = dim_time.withColumn("month", F.month("date"))
    dim_time = dim_time.withColumn("week", F.weekofyear("date"))
    dim_time = dim_time.withColumn("day", F.dayofmonth("date"))

    dim_brand = df2.select(F.col("brand_primary").alias("brand_name")).where(F.col("brand_name").isNotNull()).distinct()
    dim_brand = dim_brand.withColumn("brand_sk", stable_sk(F.col("brand_name")))

    categories = df2.select(F.explode_outer("categories_list").alias("category_tag")).where(F.col("category_tag").isNotNull()).distinct()
    if categories_taxo is not None:
        cat = categories_taxo.select(
            F.col("tag").alias("category_tag"),
            F.col("name").alias("category_name"),
            F.col("level").alias("category_level"),
            F.col("parent_tag").alias("category_parent"),
            F.col("level2").alias("category_level2")
        )
        dim_category = categories.join(cat, "category_tag", "left")
        dim_category = dim_category.withColumn("category_name", F.coalesce(F.col("category_name"), F.col("category_tag")))
        dim_category = dim_category.withColumn("category_level", F.coalesce(F.col("category_level"), F.lit(1)))
        dim_category = dim_category.withColumn("category_level2", F.coalesce(F.col("category_level2"), F.col("category_tag")))
    else:
        dim_category = categories.withColumn("category_name", F.col("category_tag")).withColumn("category_level", F.lit(1)).withColumn("category_parent", F.lit(None)).withColumn("category_level2", F.col("category_tag"))
    dim_category = dim_category.withColumn("category_sk", stable_sk(F.col("category_tag")))

    countries = df2.select(F.explode_outer("countries_list").alias("country_tag")).where(F.col("country_tag").isNotNull()).distinct()
    if countries_taxo is not None:
        c = countries_taxo.select(F.col("tag").alias("country_tag"), F.col("name").alias("country_name"))
        dim_country = countries.join(c, "country_tag", "left")
        dim_country = dim_country.withColumn("country_name", F.coalesce(F.col("country_name"), F.col("country_tag")))
    else:
        dim_country = countries.withColumn("country_name", F.col("country_tag"))
    dim_country = dim_country.withColumn("country_sk", stable_sk(F.col("country_tag")))

    return df2, dim_time, dim_brand, dim_category, dim_country


def build_bridges(df):
    product_category = df.select(F.col("code"), F.explode_outer("categories_list").alias("category_tag")).where(F.col("category_tag").isNotNull())
    product_category = product_category.withColumn("category_sk", stable_sk(F.col("category_tag")))

    product_country = df.select(F.col("code"), F.explode_outer("countries_list").alias("country_tag")).where(F.col("country_tag").isNotNull())
    product_country = product_country.withColumn("country_sk", stable_sk(F.col("country_tag")))

    return product_category, product_country


def build_dim_product_current(df):
    df2 = df.withColumn("brand_sk", stable_sk(F.col("brand_primary")))
    df2 = df2.withColumn("category_sk", stable_sk(F.col("category_primary")))
    df2 = df2.withColumn("country_sk", stable_sk(F.col("country_primary")))

    cols = [
        "code",
        "product_name_resolved",
        "brand_sk",
        "category_sk",
        "country_sk",
        "nutriscore_grade",
        "nova_group",
        "ecoscore_grade"
    ]
    return df2.select(*cols)


def build_fact_snapshot(df, dim_time):
    df2 = df.withColumn("date", F.to_date("last_modified_ts"))
    df2 = df2.join(dim_time.select("time_sk", "date"), "date", "left")

    fact = df2.select(
        "code",
        "time_sk",
        "sugars_100g",
        "salt_100g",
        "fat_100g",
        "saturated_fat_100g",
        "proteins_100g",
        "fiber_100g",
        "energy_kcal_100g",
        "nutriscore_grade",
        "nova_group",
        "ecoscore_grade",
        "completeness_score",
        "quality_issues"
    )
    return fact

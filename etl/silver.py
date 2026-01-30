from pyspark.sql import functions as F
from pyspark.sql.window import Window


def parse_list(col_name, sep=","):
    return F.when(F.col(col_name).isNull(), F.array()).otherwise(F.expr(f"transform(filter(split({col_name}, '{sep}'), x -> x is not null and trim(x) != ''), x -> trim(x))"))


def resolve_name(config):
    pref = config.get("data", {}).get("language_preference", ["fr", "en", "default"])
    cols = []
    for p in pref:
        if p == "fr":
            cols.append("product_name_fr")
        elif p == "en":
            cols.append("product_name_en")
        elif p == "default":
            cols.append("product_name")
    if not cols:
        cols = ["product_name_fr", "product_name_en", "product_name"]
    return F.coalesce(*[F.col(c) for c in cols])


def transform(df, config, dedup=True):
    name = F.trim(resolve_name(config))

    df2 = df.select(
        F.col("code").cast("string").alias("code"),
        name.alias("product_name_resolved"),
        F.col("product_name").alias("product_name_raw"),
        F.col("product_name_fr"),
        F.col("product_name_en"),
        F.col("brands").alias("brands_raw"),
        F.col("brands_tags"),
        F.col("categories").alias("categories_raw"),
        F.col("categories_tags"),
        F.col("countries").alias("countries_raw"),
        F.col("countries_tags"),
        F.col("nutriscore_grade"),
        F.col("nova_group"),
        F.col("ecoscore_grade"),
        F.col("last_modified_t"),
        F.col("nutriments")
    )

    df2 = df2.withColumn("brands_list_tmp", F.when(F.col("brands_tags").isNotNull(), F.col("brands_tags")).otherwise(parse_list("brands_raw")))
    df2 = df2.withColumn("categories_list_tmp", F.when(F.col("categories_tags").isNotNull(), F.col("categories_tags")).otherwise(parse_list("categories_raw")))
    df2 = df2.withColumn("countries_list_tmp", F.when(F.col("countries_tags").isNotNull(), F.col("countries_tags")).otherwise(parse_list("countries_raw")))

    df2 = df2.withColumn("brands_list", F.expr("filter(transform(brands_list_tmp, x -> lower(trim(x))), x -> x != '')"))
    df2 = df2.withColumn("categories_list", F.expr("filter(transform(categories_list_tmp, x -> lower(trim(x))), x -> x != '')"))
    df2 = df2.withColumn("countries_list", F.expr("filter(transform(countries_list_tmp, x -> lower(trim(x))), x -> x != '')"))

    nutriments = F.col("nutriments")
    df2 = df2.withColumn("sugars_100g", nutriments.getField("sugars_100g"))
    df2 = df2.withColumn("salt_100g", nutriments.getField("salt_100g"))
    df2 = df2.withColumn("sodium_100g", nutriments.getField("sodium_100g"))
    df2 = df2.withColumn("fat_100g", nutriments.getField("fat_100g"))
    df2 = df2.withColumn("saturated_fat_100g", F.coalesce(nutriments.getField("saturated-fat_100g"), nutriments.getField("saturated_fat_100g")))
    df2 = df2.withColumn("proteins_100g", nutriments.getField("proteins_100g"))
    df2 = df2.withColumn("fiber_100g", nutriments.getField("fiber_100g"))
    df2 = df2.withColumn("energy_kcal_100g", F.coalesce(nutriments.getField("energy-kcal_100g"), nutriments.getField("energy_kcal_100g")))

    df2 = df2.drop("nutriments")

    df2 = df2.withColumn("last_modified_ts", F.to_timestamp(F.from_unixtime(F.col("last_modified_t"))))
    df2 = df2.withColumn("code", F.trim(F.col("code")))
    df2 = df2.filter(F.col("code").isNotNull() & (F.col("code") != ""))

    df2 = df2.withColumn("salt_100g", F.when(F.col("salt_100g").isNull() & F.col("sodium_100g").isNotNull(), F.col("sodium_100g") * F.lit(2.5)).otherwise(F.col("salt_100g")))
    df2 = df2.withColumn("sodium_100g", F.when(F.col("sodium_100g").isNull() & F.col("salt_100g").isNotNull(), F.col("salt_100g") / F.lit(2.5)).otherwise(F.col("sodium_100g")))

    if dedup:
        window = Window.partitionBy("code").orderBy(F.col("last_modified_ts").desc_nulls_last())
        df2 = df2.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1).drop("rn")

    df2 = df2.withColumn("brand_primary", F.expr("case when size(brands_list) > 0 then brands_list[0] else null end"))
    df2 = df2.withColumn("category_primary", F.expr("case when size(categories_list) > 0 then categories_list[0] else null end"))
    df2 = df2.withColumn("country_primary", F.expr("case when size(countries_list) > 0 then countries_list[0] else null end"))

    return df2

import json
import os
import shutil
from datetime import datetime
from pyspark.sql import functions as F


def add_quality_columns(df, config):
    nutriment_keys = config["data"]["nutriment_keys"]
    min_n = int(config["quality"]["min_nutriments"])
    tol = float(config["quality"]["sodium_salt_tolerance"])

    present_nutriments = sum([F.when(F.col(k).isNotNull(), F.lit(1)).otherwise(F.lit(0)) for k in nutriment_keys])
    df2 = df.withColumn("nutriments_present", present_nutriments)

    name_ok = F.when(F.col("product_name_resolved").isNotNull() & (F.col("product_name_resolved") != ""), F.lit(1.0)).otherwise(F.lit(0.0))
    brand_ok = F.when(F.col("brand_primary").isNotNull(), F.lit(1.0)).otherwise(F.lit(0.0))
    category_ok = F.when(F.col("category_primary").isNotNull(), F.lit(1.0)).otherwise(F.lit(0.0))
    country_ok = F.when(F.col("country_primary").isNotNull(), F.lit(1.0)).otherwise(F.lit(0.0))
    nutr_ok = F.when(F.col("nutriments_present") >= F.lit(min_n), F.lit(1.0)).otherwise(F.lit(0.0))

    completeness_score = (name_ok + brand_ok + category_ok + country_ok + nutr_ok) / F.lit(5.0)
    df2 = df2.withColumn("completeness_score", completeness_score)

    issues = [
        F.when(F.col("product_name_resolved").isNull() | (F.col("product_name_resolved") == ""), F.lit("missing_name")),
        F.when(F.col("brand_primary").isNull(), F.lit("missing_brand")),
        F.when(F.col("category_primary").isNull(), F.lit("missing_category")),
        F.when(F.col("country_primary").isNull(), F.lit("missing_country")),
        F.when(F.col("nutriments_present") < F.lit(min_n), F.lit("insufficient_nutriments")),
        F.when(F.col("sugars_100g").isNotNull() & ((F.col("sugars_100g") < 0) | (F.col("sugars_100g") > 100)), F.lit("sugars_out_of_range")),
        F.when(F.col("salt_100g").isNotNull() & ((F.col("salt_100g") < 0) | (F.col("salt_100g") > 100)), F.lit("salt_out_of_range")),
        F.when(F.col("fat_100g").isNotNull() & ((F.col("fat_100g") < 0) | (F.col("fat_100g") > 100)), F.lit("fat_out_of_range")),
        F.when(F.col("proteins_100g").isNotNull() & ((F.col("proteins_100g") < 0) | (F.col("proteins_100g") > 100)), F.lit("proteins_out_of_range")),
        F.when(F.col("fiber_100g").isNotNull() & ((F.col("fiber_100g") < 0) | (F.col("fiber_100g") > 100)), F.lit("fiber_out_of_range")),
        F.when(F.col("sodium_100g").isNotNull() & F.col("salt_100g").isNotNull() & (F.abs(F.col("salt_100g") - (F.col("sodium_100g") * F.lit(2.5))) > F.lit(tol)), F.lit("salt_sodium_incoherent")),
        F.when(F.col("nutriscore_grade").isNotNull() & (~F.lower(F.col("nutriscore_grade")).isin(["a","b","c","d","e"])), F.lit("nutriscore_invalid"))
    ]

    df2 = df2.withColumn("quality_issues", F.array(*issues))
    df2 = df2.withColumn("quality_issues", F.expr("filter(quality_issues, x -> x is not null)"))

    return df2


def compute_metrics(df, counts, config):
    nutriment_keys = config["data"]["nutriment_keys"]
    rules = {
        "missing_name": F.when(F.col("product_name_resolved").isNull() | (F.col("product_name_resolved") == ""), 1).otherwise(0),
        "missing_brand": F.when(F.col("brand_primary").isNull(), 1).otherwise(0),
        "missing_category": F.when(F.col("category_primary").isNull(), 1).otherwise(0),
        "missing_country": F.when(F.col("country_primary").isNull(), 1).otherwise(0),
        "insufficient_nutriments": F.when(F.col("nutriments_present") < F.lit(int(config["quality"]["min_nutriments"])), 1).otherwise(0),
        "sugars_out_of_range": F.when(F.col("sugars_100g").isNotNull() & ((F.col("sugars_100g") < 0) | (F.col("sugars_100g") > 100)), 1).otherwise(0),
        "salt_out_of_range": F.when(F.col("salt_100g").isNotNull() & ((F.col("salt_100g") < 0) | (F.col("salt_100g") > 100)), 1).otherwise(0),
        "fat_out_of_range": F.when(F.col("fat_100g").isNotNull() & ((F.col("fat_100g") < 0) | (F.col("fat_100g") > 100)), 1).otherwise(0),
        "proteins_out_of_range": F.when(F.col("proteins_100g").isNotNull() & ((F.col("proteins_100g") < 0) | (F.col("proteins_100g") > 100)), 1).otherwise(0),
        "fiber_out_of_range": F.when(F.col("fiber_100g").isNotNull() & ((F.col("fiber_100g") < 0) | (F.col("fiber_100g") > 100)), 1).otherwise(0),
        "salt_sodium_incoherent": F.when(F.col("sodium_100g").isNotNull() & F.col("salt_100g").isNotNull() & (F.abs(F.col("salt_100g") - (F.col("sodium_100g") * F.lit(2.5))) > F.lit(float(config["quality"]["sodium_salt_tolerance"]))), 1).otherwise(0),
        "nutriscore_invalid": F.when(F.col("nutriscore_grade").isNotNull() & (~F.lower(F.col("nutriscore_grade")).isin(["a","b","c","d","e"])), 1).otherwise(0)
    }

    agg_exprs = [F.sum(v).alias(k) for k, v in rules.items()]
    agg_exprs.append(F.avg(F.col("completeness_score")).alias("completeness_avg"))
    row = df.agg(*agg_exprs).collect()[0].asDict()

    completeness_fields = {
        "product_name_resolved": F.avg(F.when(F.col("product_name_resolved").isNotNull() & (F.col("product_name_resolved") != ""), 1).otherwise(0)),
        "brand_primary": F.avg(F.when(F.col("brand_primary").isNotNull(), 1).otherwise(0)),
        "category_primary": F.avg(F.when(F.col("category_primary").isNotNull(), 1).otherwise(0)),
        "country_primary": F.avg(F.when(F.col("country_primary").isNotNull(), 1).otherwise(0))
    }
    for k in nutriment_keys:
        completeness_fields[k] = F.avg(F.when(F.col(k).isNotNull(), 1).otherwise(0))

    comp_row = df.agg(*[v.alias(k) for k, v in completeness_fields.items()]).collect()[0].asDict()

    metrics = {
        "run_ts": datetime.utcnow().isoformat(),
        "counts": counts,
        "completeness": {k: float(v) if v is not None else 0.0 for k, v in comp_row.items()},
        "anomalies": {k: int(row.get(k, 0) or 0) for k in rules.keys()},
        "completeness_avg": float(row.get("completeness_avg") or 0.0)
    }
    return metrics


def write_metrics(metrics, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)


def anomalies_sample(df, path):
    tmp_dir = path + "_tmp"
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
    sample = df.filter(F.size(F.col("quality_issues")) > 0).select("code", "product_name_resolved", "quality_issues")
    sample.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_dir)
    part_files = [p for p in os.listdir(tmp_dir) if p.startswith("part-")]
    if part_files:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if os.path.exists(path):
            os.remove(path)
        shutil.move(os.path.join(tmp_dir, part_files[0]), path)
    shutil.rmtree(tmp_dir)

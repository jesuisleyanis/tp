Dictionnaire de donnees

Binome: Yanis Capelle, Louis Nectoux

dim_time
- time_sk INT, PK
- date DATE
- year SMALLINT
- month TINYINT
- week TINYINT
- iso_week TINYINT
- day TINYINT

dim_brand
- brand_sk BIGINT, PK
- brand_name VARCHAR

dim_category
- category_sk BIGINT, PK
- category_code VARCHAR
- category_name_fr VARCHAR
- level INT
- parent_category_sk BIGINT
- category_level2 VARCHAR

dim_country
- country_sk BIGINT, PK
- country_code VARCHAR
- country_name_fr VARCHAR

dim_product
- product_sk BIGINT, PK
- code VARCHAR, cle naturelle
- product_name VARCHAR
- brand_sk BIGINT, FK dim_brand
- category_sk BIGINT, FK dim_category
- country_sk BIGINT, FK dim_country
- countries_multi JSON
- nutriscore_grade CHAR(1)
- nova_group INT
- ecoscore_grade CHAR(1)
- attr_hash CHAR(64)
- effective_from DATETIME
- effective_to DATETIME
- is_current TINYINT

bridge_product_category
- product_sk BIGINT, FK dim_product
- category_sk BIGINT, FK dim_category

bridge_product_country
- product_sk BIGINT, FK dim_product
- country_sk BIGINT, FK dim_country

fact_nutrition_snapshot
- fact_id BIGINT, PK
- product_sk BIGINT, FK dim_product
- time_sk INT, FK dim_time
- sugars_100g DOUBLE
- salt_100g DOUBLE
- sodium_100g DOUBLE
- fat_100g DOUBLE
- saturated_fat_100g DOUBLE
- proteins_100g DOUBLE
- fiber_100g DOUBLE
- energy_kcal_100g DOUBLE
- nutriscore_grade CHAR(1)
- nova_group INT
- ecoscore_grade CHAR(1)
- completeness_score DOUBLE
- quality_issues_json JSON

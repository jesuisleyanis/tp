CREATE TABLE IF NOT EXISTS dim_time (
  time_sk INT NOT NULL,
  date DATE,
  year SMALLINT,
  month TINYINT,
  week TINYINT,
  day TINYINT,
  PRIMARY KEY (time_sk)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_brand (
  brand_sk BIGINT NOT NULL,
  brand_name VARCHAR(255),
  PRIMARY KEY (brand_sk),
  UNIQUE KEY ux_brand_name (brand_name)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_category (
  category_sk BIGINT NOT NULL,
  category_tag VARCHAR(255),
  category_name VARCHAR(255),
  category_level INT,
  category_parent VARCHAR(255),
  category_level2 VARCHAR(255),
  PRIMARY KEY (category_sk),
  UNIQUE KEY ux_category_tag (category_tag)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_country (
  country_sk BIGINT NOT NULL,
  country_tag VARCHAR(255),
  country_name VARCHAR(255),
  PRIMARY KEY (country_sk),
  UNIQUE KEY ux_country_tag (country_tag)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_product (
  product_sk BIGINT NOT NULL AUTO_INCREMENT,
  code VARCHAR(64) NOT NULL,
  product_name_resolved VARCHAR(512),
  brand_sk BIGINT,
  category_sk BIGINT,
  country_sk BIGINT,
  nutriscore_grade CHAR(1),
  nova_group INT,
  ecoscore_grade CHAR(1),
  attr_hash CHAR(64),
  effective_from DATETIME,
  effective_to DATETIME,
  is_current TINYINT,
  current_code VARCHAR(64) GENERATED ALWAYS AS (CASE WHEN is_current = 1 THEN code ELSE NULL END) STORED,
  PRIMARY KEY (product_sk),
  UNIQUE KEY ux_current_code (current_code),
  KEY ix_product_code (code),
  KEY ix_product_brand (brand_sk),
  KEY ix_product_category (category_sk),
  KEY ix_product_country (country_sk),
  CONSTRAINT fk_product_brand FOREIGN KEY (brand_sk) REFERENCES dim_brand(brand_sk) ON DELETE SET NULL,
  CONSTRAINT fk_product_category FOREIGN KEY (category_sk) REFERENCES dim_category(category_sk) ON DELETE SET NULL,
  CONSTRAINT fk_product_country FOREIGN KEY (country_sk) REFERENCES dim_country(country_sk) ON DELETE SET NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS bridge_product_category (
  product_sk BIGINT NOT NULL,
  category_sk BIGINT NOT NULL,
  PRIMARY KEY (product_sk, category_sk),
  KEY ix_bridge_category (category_sk),
  CONSTRAINT fk_bridge_category_product FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk) ON DELETE CASCADE,
  CONSTRAINT fk_bridge_category_category FOREIGN KEY (category_sk) REFERENCES dim_category(category_sk) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS bridge_product_country (
  product_sk BIGINT NOT NULL,
  country_sk BIGINT NOT NULL,
  PRIMARY KEY (product_sk, country_sk),
  KEY ix_bridge_country (country_sk),
  CONSTRAINT fk_bridge_country_product FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk) ON DELETE CASCADE,
  CONSTRAINT fk_bridge_country_country FOREIGN KEY (country_sk) REFERENCES dim_country(country_sk) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS fact_nutrition_snapshot (
  product_sk BIGINT NOT NULL,
  time_sk INT NOT NULL,
  sugars_100g DOUBLE,
  salt_100g DOUBLE,
  fat_100g DOUBLE,
  saturated_fat_100g DOUBLE,
  proteins_100g DOUBLE,
  fiber_100g DOUBLE,
  energy_kcal_100g DOUBLE,
  nutriscore_grade CHAR(1),
  nova_group INT,
  ecoscore_grade CHAR(1),
  completeness_score DOUBLE,
  quality_issues_json JSON,
  PRIMARY KEY (product_sk, time_sk),
  KEY ix_fact_time (time_sk),
  KEY ix_fact_nutriscore (nutriscore_grade),
  CONSTRAINT fk_fact_product FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk) ON DELETE CASCADE,
  CONSTRAINT fk_fact_time FOREIGN KEY (time_sk) REFERENCES dim_time(time_sk) ON DELETE CASCADE
) ENGINE=InnoDB;

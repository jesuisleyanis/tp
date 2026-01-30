SELECT b.brand_name, ROUND(SUM(CASE WHEN LOWER(f.nutriscore_grade) IN ('a','b') THEN 1 ELSE 0 END) / COUNT(*), 4) AS proportion_ab, COUNT(*) AS total_products
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_brand b ON p.brand_sk = b.brand_sk
GROUP BY b.brand_name
ORDER BY proportion_ab DESC, total_products DESC
LIMIT 10;

SELECT c.category_level2, f.nutriscore_grade, COUNT(*) AS cnt
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN bridge_product_category bc ON p.product_sk = bc.product_sk
JOIN dim_category c ON bc.category_sk = c.category_sk
GROUP BY c.category_level2, f.nutriscore_grade
ORDER BY c.category_level2, f.nutriscore_grade;

SELECT co.country_name, c.category_level2, AVG(f.sugars_100g) AS avg_sugars_100g
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN bridge_product_country bco ON p.product_sk = bco.product_sk
JOIN dim_country co ON bco.country_sk = co.country_sk
JOIN bridge_product_category bc ON p.product_sk = bc.product_sk
JOIN dim_category c ON bc.category_sk = c.category_sk
GROUP BY co.country_name, c.category_level2
ORDER BY co.country_name, c.category_level2;

SELECT b.brand_name, ROUND(AVG(f.completeness_score), 4) AS completeness_avg
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_brand b ON p.brand_sk = b.brand_sk
GROUP BY b.brand_name
ORDER BY completeness_avg DESC;

SELECT p.code, p.product_name_resolved, f.salt_100g, f.sugars_100g
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
WHERE (f.salt_100g > 25) OR (f.sugars_100g > 80)
ORDER BY f.salt_100g DESC, f.sugars_100g DESC;

SELECT t.year, t.week, ROUND(AVG(f.completeness_score), 4) AS completeness_avg, COUNT(*) AS rows_count
FROM fact_nutrition_snapshot f
JOIN dim_time t ON f.time_sk = t.time_sk
GROUP BY t.year, t.week
ORDER BY t.year, t.week;

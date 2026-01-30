SELECT COUNT(*) AS fact_rows, ROUND(AVG(completeness_score), 4) AS completeness_avg
FROM fact_nutrition_snapshot;

SELECT
  SUM(CASE WHEN sugars_100g > 100 OR sugars_100g < 0 THEN 1 ELSE 0 END) AS sugars_out_of_range,
  SUM(CASE WHEN salt_100g > 100 OR salt_100g < 0 THEN 1 ELSE 0 END) AS salt_out_of_range,
  SUM(CASE WHEN fat_100g > 100 OR fat_100g < 0 THEN 1 ELSE 0 END) AS fat_out_of_range
FROM fact_nutrition_snapshot;

SELECT t.year, t.iso_week, ROUND(AVG(f.completeness_score), 4) AS completeness_avg
FROM fact_nutrition_snapshot f
JOIN dim_time t ON f.time_sk = t.time_sk
GROUP BY t.year, t.iso_week
ORDER BY t.year, t.iso_week;

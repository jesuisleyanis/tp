Cahier Qualite

Binome: Yanis Capelle, Louis Nectoux

Regles
- Unicite du code en silver apres dedoublonnage
- Completude ponderee: nom, marque, categorie, pays, nutriments cles
- Bornes nutriments: 0 a 100
- Coherence sel/sodium quand sodium disponible
- Validite nutriscore_grade dans {a,b,c,d,e}

Metriques generees
- data/output/quality_metrics.json
- Comptages: bronze_read, filtered_invalid, silver_after_dedup, dedup_removed, gold_fact_rows, scd2_updates, scd2_inserts
- Completude globale et par champ
- Anomalies par regle

Avant et apres
- Avant: bronze_read
- Apres: silver_after_dedup et gold_fact_rows

Anomalies
- data/output/anomalies.csv
- Colonnes: code, product_name_resolved, quality_issues

Tableau de bord SQL
- sql/quality_dashboard.sql

Exemples d anomalies
- sugars_100g hors bornes
- salt_100g hors bornes
- nutriments insuffisants

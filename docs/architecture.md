Architecture

Binome: Yanis Capelle, Louis Nectoux

Pipeline
- Bronze: lecture JSONL/CSV avec schema explicite depuis conf/schema_off.json
- Silver: normalisation des types, nettoyage chaines, selection langue, dedoublonnage par code et last_modified_t
- Gold: dimensions, faits et bridges
- Chargement MySQL via JDBC avec upsert et SCD2 produit

Bronze
- Entree sample par defaut via l API OpenFoodFacts, ecriture JSONL local
- Entree full via chemin fourni

Silver
- Choix langue via data.language_preference
- Sel/sodium normalises, nutriments aplatis
- Dedoublonnage: row_number par code, last_modified_t decroissant

Gold et modele
- dim_time: date, annee, mois, semaine, jour
- dim_brand, dim_category, dim_country avec cles stables
- dim_product en SCD2 avec product_sk auto, attr_hash, is_current
- fact_nutrition_snapshot par produit et date
- bridges N-N: produit-categorie, produit-pays

Strategie SCD2
- Calcul attr_hash sur attributs suivis
- Si changement: fermeture de la ligne courante (effective_to), insertion nouvelle ligne
- Unicite courante garantie par index sur current_code

Strategie upsert
- Staging JDBC puis INSERT ... ON DUPLICATE KEY UPDATE
- Idempotence: relancer l ETL ne duplique pas les faits

Performance
- Broadcast des taxonomies categories et pays
- Partitions Spark via spark.sql.shuffle.partitions

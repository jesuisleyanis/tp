README Documentation

Binome: Yanis Capelle, Louis Nectoux

Architecture pipeline
- Bronze: ingestion JSONL/CSV avec schema explicite
- Silver: nettoyage, typage, normalisation unites, dedoublonnage par code et last_modified_t
- Gold: modele etoile avec dimensions, faits et bridges
- Chargement MySQL via JDBC avec upsert

Mode sample
- Telechargement automatique d un seed via l API OpenFoodFacts
- Fichier local par defaut: data/input/sample.jsonl

Mode full
- Telechargement d un export complet si data.full_url est defini
- Fichier local par defaut: data/input/full.jsonl

Commandes
- make up
- make etl
- make test
- make sql
- make quality

Sorties qualite
- data/output/quality_metrics.json
- data/output/anomalies.csv

Tableau de bord SQL
- sql/quality_dashboard.sql

Reglages principaux
- conf/config.yaml
- data.language_preference
- quality.min_nutriments

Execution full
- python -m etl.main --mode full --config conf/config.yaml --input /chemin/vers/export.jsonl

Schemas et choix
- docs/architecture.md
- docs/data-dictionary.md
- docs/schemas/schema.mmd
- docs/quality-notebook.md

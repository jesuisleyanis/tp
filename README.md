Atelier Integration des Donnees - OpenFoodFacts Nutrition & Qualite

Binome: Yanis Capelle, Louis Nectoux

Objectif
Pipeline Spark (PySpark) bronze -> silver -> gold avec chargement MySQL 8, qualite, SCD2 produit, metriques et requetes analytiques.

Prerequis
- Docker et Docker Compose
- Python 3.10+
- Java 11+

Installation
1. Creer un environnement virtuel Python et installer les dependances
   pip install -r requirements.txt
2. Creer un fichier .env depuis .env.example si besoin
   cp .env.example .env

Execution rapide
1. Demarrer MySQL
   make up
2. Lancer l ETL en mode sample
   make etl
3. Lancer les tests
   make test

Script tout-en-un
   sh tests/run_all.sh

Requetes analytiques
   make sql

Fichiers de sortie
- data/output/quality_metrics.json
- data/output/anomalies.csv

Changer le nombre de nutriments cles
- Modifier conf/config.yaml -> quality.min_nutriments

Changer la langue de preference
- Modifier conf/config.yaml -> data.language_preference

Mode full
- Deposer un export OpenFoodFacts JSONL/CSV dans data/input/full.jsonl ou definir un chemin avec --input
- Exemple
  python -m etl.main --mode full --config conf/config.yaml --input /chemin/vers/export.jsonl

Details d architecture
Voir docs/README.md, docs/architecture.md et docs/schemas/schema.mmd

Journal des prompts

Prompt principal
Tu es Codex. Tu dois produire un dépôt Git COMPLET, exécutable de bout en bout, répondant à l’exercice “Atelier Intégration des Données – OpenFoodFacts Nutrition & Qualité” (Spark → MySQL). Le dépôt doit être prêt à rendre, sans intervention manuelle après génération.

Contraintes non négociables :
	•	Pas de commentaires dans le code (aucun #, //, /* */).
	•	Aucun placeholder type [VOTRE NOM], TODO, TBD.
	•	Tout doit tourner localement via commandes documentées.
	•	Tous les livrables demandés doivent exister, être fonctionnels, et être testés.
	•	L’ETL doit utiliser Apache Spark (PySpark) et charger un datamart MySQL 8 via JDBC.
	•	Lecture JSON/CSV avec schéma explicite (pas d’inférence).
	•	Bronze/Silver/Gold + qualité + métriques + SCD2 produit + requêtes SQL analytiques.
	•	Repo structuré : /docs, /etl, /sql, /tests, /conf.

Identité projet (à mettre dans la doc uniquement, pas dans le code) :
	•	Binôme : Yanis Capelle, Louis Nectoux.

1) Résultat attendu (ce que tu dois générer)

Crée un repo avec :
	•	/docs
	•	README.md (how-to run complet, commandes exactes)
	•	architecture.md (note d’archi : choix, schémas, stratégie upsert, SCD2)
	•	data-dictionary.md (dictionnaire de données dim/facts)
	•	quality-notebook.md (cahier qualité : règles, coverage, anomalies, before/after)
	•	schemas/ (schéma étoile ou flocon, en mermaid + image si possible)
	•	prompts-journal.md (journal des prompts : liste de ce prompt + sous-prompts internes si tu en utilises)
	•	/etl
	•	main.py (orchestrateur Spark : bronze→silver→gold→mysql)
	•	bronze.py, silver.py, gold.py (pipeline en modules)
	•	quality.py (règles qualité + métriques)
	•	scd2.py (gestion SCD2 dim_product)
	•	taxonomies.py (chargement taxonomies OFF + broadcast joins)
	•	io_mysql.py (écriture JDBC + upsert patterns)
	•	/sql
	•	ddl_mysql.sql (création tables, PK/FK, index)
	•	dml_seed.sql (facultatif si besoin)
	•	analytics.sql (requêtes métiers demandées)
	•	/tests
	•	test_repo_structure.py
	•	test_etl_local_small.py (exécution sur petit sample)
	•	test_mysql_loaded.py (vérifie tables + lignes + contraintes)
	•	test_quality_metrics.py (vérifie présence JSON métriques + champs)
	•	/conf
	•	config.yaml (paths, mysql, options spark, taille seed, langue)
	•	schema_off.json (schéma explicite source)
	•	Fichiers racine
	•	docker-compose.yml (MySQL 8 + adminer optionnel)
	•	Makefile (cibles run, test, up, down, reset)
	•	requirements.txt (pyspark, mysql connector, pyyaml, pytest)
	•	.env.example (MYSQL_USER/PASS/DB/PORT)
	•	.gitignore

2) Données OpenFoodFacts : stratégie ingestion

Tu dois implémenter une ingestion “bulk seed” reproductible basée sur un export complet OpenFoodFacts (JSONL ou CSV).
Exigences :
	•	Téléchargement automatisé d’un “seed” (un extrait) pour tests rapides + possibilité de pointer vers export complet si l’utilisateur le fournit.
	•	Pour rendre le repo testable sans gros volume : implémente par défaut un mode “sample” téléchargeant un fichier raisonnable (ou générant un mini échantillon à partir d’un export si présent).
	•	Bronze lit JSONL/CSV avec schéma explicite.
	•	Champs minimaux à extraire en bronze (au moins) :
	•	code, product_name, product_name_fr, product_name_en
	•	brands / brands_tags
	•	categories / categories_tags
	•	countries / countries_tags
	•	nutriments.sugars_100g, salt_100g, fat_100g, saturated-fat_100g, proteins_100g, fiber_100g, energy-kcal_100g (gère variantes)
	•	nutriscore_grade, nova_group, ecoscore_grade
	•	last_modified_t (source de la dimension temps + dédoublonnage)
	•	Silver :
	•	normalisation types (float, int, timestamps)
	•	normalisation unités : gérer sel/sodium (sodium_100g optionnel) et cohérence (sel ≈ 2.5 × sodium quand dispo)
	•	nettoyage string (trim, lower si utile)
	•	choix langue : fr > en > fallback product_name
	•	dédoublonnage par code en gardant l’enregistrement le plus récent (last_modified_t)
	•	flatten struct nutriments
	•	Gold :
	•	modélisation étoile/flocon contrôlé
	•	dimensions : dim_time, dim_brand, dim_category (avec level + parent), dim_country, dim_product (SCD2), optionnel dim_nutri
	•	faits : fact_nutrition_snapshot (snapshot par produit et temps)
	•	optionnel : bridge_product_category si N-N

3) Modèle MySQL (DDL strict)

Tu dois créer le datamart MySQL 8 (scripts /sql/ddl_mysql.sql) avec :
	•	PK/FK cohérentes
	•	index sur clés de jointure et champs fréquents
	•	dim_product en SCD2 :
	•	product_sk surrogate PK auto
	•	code nat key
	•	effective_from, effective_to, is_current
	•	un hash d’attributs suivis (ex: attr_hash) pour détecter changement
	•	unicité : un seul is_current=1 par code
	•	fact_nutrition_snapshot :
	•	product_sk, time_sk
	•	mesures nutriments 100g
	•	attributs : nutriscore_grade, nova_group, ecoscore_grade
	•	completeness_score float 0..1
	•	quality_issues_json JSON

4) Stratégie de chargement MySQL (JDBC)

Implémente dans Spark :
	•	petites dimensions : truncate/insert (si pertinent) ou upsert si nécessaire
	•	faits + bridge : upsert via INSERT ... ON DUPLICATE KEY UPDATE
	•	batch size JDBC configuré
	•	idempotence : relancer l’ETL ne doit pas dupliquer les lignes (les faits doivent se mettre à jour ou rester stables)

5) Qualité des données : règles + métriques + rapports

Tu dois implémenter un “cahier qualité” exécutable :
	•	règles minimales (au moins) :
	•	unicité : code unique en silver (après dédoublonnage)
	•	complétude pondérée : présence product_name_resolved, brand, ≥N nutriments clés, ≥1 catégorie normalisée, ≥1 pays
	•	bornes : 0 ≤ sugars_100g ≤ 100 ; 0 ≤ salt_100g ≤ 100 ; 0 ≤ fat_100g ≤ 100 ; etc.
	•	cohérence sel/sodium quand sodium dispo
	•	validité des grades : nutriscore dans {a,b,c,d,e} si présent
	•	produire un fichier métriques JSON par run :
	•	nb lus, nb filtrés, nb dédupliqués, nb produits gold, nb insert/update SCD2
	•	% complétude global + par champ
	•	anomalies par règle (counts)
	•	produire aussi un CSV anomalies (échantillons) et référencer dans la doc
	•	alimenter quality_issues_json dans la fact (liste des règles en échec par ligne)

6) Requêtes analytiques SQL (livrable)

Dans /sql/analytics.sql, fournis et rends exécutables les requêtes :
	•	Top 10 marques par proportion de produits Nutri-Score A/B
	•	Distribution Nutri-Score par niveau 2 de catégorie
	•	Heatmap pays × catégorie : moyenne sugars_100g (table)
	•	Taux de complétude des nutriments par marque
	•	Liste anomalies (salt_100g > 25 ou sugars_100g > 80)
	•	Évolution hebdo de la complétude via dim_time
Chaque requête doit fonctionner sur le schéma créé.

7) Tests : obligatoire, automatisés

Tu dois fournir des tests PyTest qui valident :
	•	présence de tous les livrables et structure repo
	•	exécution ETL sur un petit sample en local
	•	présence tables MySQL + au moins X lignes dans dimensions/faits
	•	contraintes SCD2 respectées (un seul current par code)
	•	existence du JSON de métriques + champs attendus
Les tests doivent être exécutables par : make test.

8) Exécution locale : obligatoire via Makefile + Docker

Fournis :
	•	docker-compose.yml lançant MySQL 8 (et adminer si tu veux)
	•	Makefile avec :
	•	make up (démarre mysql)
	•	make down
	•	make reset (drop volume)
	•	make etl (run pipeline)
	•	make test (pytest)
	•	make sql (exécute analytics.sql si possible)
	•	README : instructions exactes, aucune ambiguïté
	•	Config via .env + conf/config.yaml

9) Implémentation : décisions imposées (pour garantir un rendu stable)

Tu dois choisir PySpark.
Tu dois inclure un mode --mode sample par défaut pour limiter volume, et --mode full si un export complet est fourni.
Tu dois fournir une commande unique reproductible :
	•	make up
	•	make etl
	•	make test
Doit passer.

10) Règles d’écriture du code
	•	Pas de commentaires.
	•	Code clair via noms de fonctions/classes, typage Python si possible.
	•	Logs OK (print ou logging) autorisés, mais pas de commentaires.
	•	Gestion erreurs : messages clairs si MySQL pas prêt ou fichier source absent.
	•	Aucun secret en dur : tout via env/config.

11) Livrables de docs : contenu attendu

Dans docs :
	•	README : architecture pipeline, prérequis, commandes, où sont les outputs (metrics/anomalies), comment changer N nutriments clés, comment changer langue.
	•	data-dictionary : colonnes, types, description, clés, règles.
	•	architecture : diagramme bronze/silver/gold + schéma étoile + stratégie upsert + SCD2 + partition/broadcast.
	•	quality-notebook : tableau des règles, définition, seuils, mesures avant/après, exemples anomalies.
	•	prompts-journal : inclure ce prompt intégral.

12) Vérifications finales que tu dois faire toi-même (dans le repo)

Ajoute un script tests/run_all.sh (ou via Makefile) qui enchaîne :
	•	up mysql
	•	run etl sample
	•	run tests
Et le README doit pointer dessus.

Ta sortie finale doit être le contenu complet du repo (arborescence + fichiers), prêt à commit.

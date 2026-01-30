.PHONY: up down reset etl run test sql quality
SHELL=/bin/sh

up:
	@if [ ! -f .env ]; then cp .env.example .env; fi
	docker compose up -d

down:
	docker compose down

reset:
	docker compose down -v

etl:
	@if [ ! -f .env ]; then cp .env.example .env; fi
	set -a; . ./.env; set +a; python3 -m etl.main --mode sample --config conf/config.yaml --input data/input/sample.jsonl

run: etl

test:
	@if [ ! -f .env ]; then cp .env.example .env; fi
	set -a; . ./.env; set +a; pytest -q

sql:
	@if [ ! -f .env ]; then cp .env.example .env; fi
	set -a; . ./.env; set +a; docker compose exec -T mysql mysql -u$$MYSQL_USER -p$$MYSQL_PASSWORD $$MYSQL_DB < sql/analytics.sql

quality:
	@if [ ! -f .env ]; then cp .env.example .env; fi
	set -a; . ./.env; set +a; docker compose exec -T mysql mysql -u$$MYSQL_USER -p$$MYSQL_PASSWORD $$MYSQL_DB < sql/quality_dashboard.sql

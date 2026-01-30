from pathlib import Path


def test_repo_structure():
    required = [
        "docs",
        "docs/README.md",
        "docs/architecture.md",
        "docs/data-dictionary.md",
        "docs/quality-notebook.md",
        "docs/prompts-journal.md",
        "docs/schemas/schema.mmd",
        "etl/main.py",
        "etl/bronze.py",
        "etl/silver.py",
        "etl/gold.py",
        "etl/quality.py",
        "etl/scd2.py",
        "etl/taxonomies.py",
        "etl/io_mysql.py",
        "sql/ddl_mysql.sql",
        "sql/analytics.sql",
        "tests/test_etl_local_small.py",
        "tests/test_mysql_loaded.py",
        "tests/test_quality_metrics.py",
        "conf/config.yaml",
        "conf/schema_off.json",
        "docker-compose.yml",
        "Makefile",
        "requirements.txt",
        ".env.example",
        ".gitignore",
        "tests/run_all.sh"
    ]
    for p in required:
        assert Path(p).exists(), f"Missing {p}"

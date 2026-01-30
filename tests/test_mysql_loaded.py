import os
import mysql.connector


def test_mysql_loaded():
    host = os.environ.get("MYSQL_HOST")
    port = int(os.environ.get("MYSQL_PORT", "3306"))
    db = os.environ.get("MYSQL_DB")
    user = os.environ.get("MYSQL_USER")
    password = os.environ.get("MYSQL_PASSWORD")
    assert host and db and user and password

    conn = mysql.connector.connect(host=host, port=port, user=user, password=password, database=db)
    cur = conn.cursor()

    cur.execute("SHOW TABLES")
    tables = {r[0] for r in cur.fetchall()}
    required = {"dim_time", "dim_brand", "dim_category", "dim_country", "dim_product", "bridge_product_category", "bridge_product_country", "fact_nutrition_snapshot"}
    assert required.issubset(tables)

    cur.execute("SELECT COUNT(*) FROM dim_product")
    assert cur.fetchone()[0] > 0

    cur.execute("SELECT COUNT(*) FROM fact_nutrition_snapshot")
    assert cur.fetchone()[0] > 0

    cur.execute("SELECT code, COUNT(*) FROM dim_product WHERE is_current=1 GROUP BY code HAVING COUNT(*) > 1")
    assert cur.fetchall() == []

    cur.close()
    conn.close()

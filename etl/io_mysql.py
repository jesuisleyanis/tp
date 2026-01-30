import os
import mysql.connector


def resolve_env(value):
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        return os.getenv(value[2:-1])
    return value


def mysql_params(config):
    cfg = config["mysql"]
    host = resolve_env(cfg["host"])
    port = int(resolve_env(cfg["port"]))
    database = resolve_env(cfg["database"])
    user = os.getenv(cfg["user_env"])
    password = os.getenv(cfg["password_env"])
    if not user or not password or not host or not database:
        raise ValueError("MySQL configuration is missing or incomplete")
    return host, port, database, user, password


def jdbc_url(config):
    host, port, database, user, password = mysql_params(config)
    url = f"jdbc:mysql://{host}:{port}/{database}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
    return url, user, password


def spark_write(df, config, table, mode, extra_options=None):
    url, user, password = jdbc_url(config)
    writer = df.write.format("jdbc").option("url", url).option("dbtable", table).option("user", user).option("password", password).option("driver", "com.mysql.cj.jdbc.Driver")
    batch = config["spark"]["jdbc_batch_size"]
    writer = writer.option("batchsize", str(batch))
    if extra_options:
        for k, v in extra_options.items():
            writer = writer.option(k, v)
    writer.mode(mode).save()


def connect_mysql(config):
    host, port, database, user, password = mysql_params(config)
    return mysql.connector.connect(host=host, port=port, user=user, password=password, database=database)


def run_sql(conn, sql):
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    cur = conn.cursor()
    for stmt in statements:
        cur.execute(stmt)
    conn.commit()
    cur.close()


def run_sql_file(config, path):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    conn = connect_mysql(config)
    run_sql(conn, sql)
    conn.close()


def table_exists(conn, table):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=%s", (table,))
    res = cur.fetchone()[0]
    cur.close()
    return res > 0


def write_truncate(df, config, table):
    conn = connect_mysql(config)
    run_sql(conn, f"TRUNCATE TABLE {table}")
    conn.close()
    write_append(df, config, table)


def write_append(df, config, table):
    spark_write(df, config, table, "append")


def write_staging(df, config, table):
    spark_write(df, config, table, "overwrite")


def upsert_from_staging(config, target_table, staging_table, key_cols, update_cols):
    cols = key_cols + update_cols
    col_list = ",".join(cols)
    updates = ",".join([f"{c}=VALUES({c})" for c in update_cols])
    sql = f"INSERT INTO {target_table} ({col_list}) SELECT {col_list} FROM {staging_table} ON DUPLICATE KEY UPDATE {updates}"
    conn = connect_mysql(config)
    run_sql(conn, sql)
    conn.close()


def drop_table(config, table):
    conn = connect_mysql(config)
    run_sql(conn, f"DROP TABLE IF EXISTS {table}")
    conn.close()

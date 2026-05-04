def get_table(
    spark,
    table_name,
    dbutils,
    secret_scope="postgres-creds"
):
    host = dbutils.secrets.get(secret_scope, "postgres-host")
    port = dbutils.secrets.get(secret_scope, "postgres-port")
    db = dbutils.secrets.get(secret_scope, "postgres-db")
    user = dbutils.secrets.get(secret_scope, "postgres-user")
    password = dbutils.secrets.get(secret_scope, "postgres-password")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"

    df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    return df


def get_bronze(delta_bronze_path, spark):
    df_bronze = (
        spark.read
        .format("delta")
        .load(delta_bronze_path)
    )
    return df_bronze
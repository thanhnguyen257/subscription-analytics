def get_table(jdbc_url, table_name, spark):
    # Example
    # jdbc_url = "jdbc:postgresql://localhost:5432/staging_area"
    # table_name = "public.users"
    df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", table_name)
    .option("user", "postgres")
    .option("password", "postgres")
    .option("driver", "org.postgresql.Driver")
    .load())
    return df
def get_bronze(delta_bronze_path,spark):
    #Example: user_bronze = "/home/thuyttd11/subscription-analytics/spark-data/bronze/users"
    df_bronze = (
        spark.read
        .format("delta")
        .load(delta_bronze_path)
    )
    return df_bronze
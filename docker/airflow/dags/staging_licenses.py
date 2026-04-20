from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from datetime import datetime
import io
import csv

BATCH_SIZE = 10000

@dag(
    dag_id="create_licenses",
    start_date=datetime(2026, 4, 1),
    schedule="@monthly",
    catchup=False,
    tags=["staging"]
)
def create_licenses():

    config = Variable.get("dag_config", default_var=None, deserialize_json=True)
    if not config:
        raise AirflowFailException("Missing 'dag_config'. Run DAG 'config' first.")

    POSTGRES_CONN_ID = config["postgres_conn_id"]
    AZURE_SQL_CONN_ID = config["azure_sql_conn_id"]

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    azure = OdbcHook(
        odbc_conn_id=AZURE_SQL_CONN_ID,
        driver="ODBC Driver 18 for SQL Server",
        database="sqlserver-subscriptions"
    )

    # -------------------------
    # LICENSE KEYS (INCREMENTAL APPEND)
    # -------------------------
    @task
    def stage_license_keys():
        last_id = int(Variable.get("lk_watermark", 0))
        batch_id = datetime.now().isoformat()

        conn_pg = pg.get_conn()
        cur_pg = conn_pg.cursor()

        conn_az = azure.get_conn()
        cur_az = conn_az.cursor()

        while True:
            cur_az.execute(f"""
                SELECT TOP {BATCH_SIZE}
                    license_id,
                    subscription_id,
                    max_seats,
                    issued_date,
                    expiry_date
                FROM license_keys
                WHERE license_id > ?
                ORDER BY license_id
            """, (last_id,))

            rows = cur_az.fetchall()
            if not rows:
                break

            buffer = io.StringIO()
            writer = csv.writer(buffer)

            ingestion_time = datetime.now()

            for r in rows:
                writer.writerow([
                    r[0],
                    r[1],
                    r[2],
                    r[3],
                    r[4],
                    ingestion_time,
                    batch_id
                ])

            buffer.seek(0)

            cur_pg.copy_expert("""
                COPY license_keys (
                    license_id,
                    subscription_id,
                    max_seats,
                    issued_date,
                    expiry_date,
                    ingestion_time,
                    batch_id
                )
                FROM STDIN WITH CSV
            """, buffer)

            conn_pg.commit()
            last_id = max(r[0] for r in rows)

        Variable.set("lk_watermark", str(last_id))

        cur_az.close()
        conn_az.close()
        cur_pg.close()
        conn_pg.close()

    # -------------------------
    # LICENSE ALLOCATIONS (EVENT TABLE)
    # -------------------------
    @task
    def stage_license_allocations():
        last_id = int(Variable.get("la_watermark", 0))
        batch_id = datetime.now().isoformat()

        conn_pg = pg.get_conn()
        cur_pg = conn_pg.cursor()

        conn_az = azure.get_conn()
        cur_az = conn_az.cursor()

        while True:
            cur_az.execute(f"""
                SELECT TOP {BATCH_SIZE}
                    allocation_id,
                    license_id,
                    seat_number,
                    status,
                    allocation_date
                FROM license_allocations
                WHERE allocation_id > ?
                ORDER BY allocation_id
            """, (last_id,))

            rows = cur_az.fetchall()
            if not rows:
                break

            buffer = io.StringIO()
            writer = csv.writer(buffer)

            ingestion_time = datetime.now()

            for r in rows:
                writer.writerow([
                    r[0],
                    r[1],
                    r[2],
                    r[3],
                    r[4],
                    ingestion_time,
                    batch_id
                ])

            buffer.seek(0)

            cur_pg.copy_expert("""
                COPY license_allocations (
                    allocation_id,
                    license_id,
                    seat_number,
                    status,
                    allocation_date,
                    ingestion_time,
                    batch_id
                )
                FROM STDIN WITH CSV
            """, buffer)

            conn_pg.commit()
            last_id = max(r[0] for r in rows)

        Variable.set("la_watermark", str(last_id))

        cur_az.close()
        conn_az.close()
        cur_pg.close()
        conn_pg.close()

    # DAG FLOW
    stage_license_keys() >> stage_license_allocations()


create_licenses()
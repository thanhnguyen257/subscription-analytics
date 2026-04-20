from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

@dag(
    dag_id="create_tables",
    start_date=datetime(2026, 4, 1),
    schedule=None, #Only run when need
    catchup=False,
    tags=["staging"]
)
def create_tables():
    config = Variable.get("dag_config", default_var=None, deserialize_json=True)
    if not config:
        raise AirflowFailException("Missing 'dag_config'. Run DAG 'config' first.")
    CONN_ID = config["conn_id"]
    STAGING_DB = config["databases"]
    tables = SQLExecuteQueryOperator(
        task_id="create_tables_staging_1",
        conn_id=CONN_ID,
        database=STAGING_DB,
        sql="""
    CREATE TABLE IF NOT EXISTS public.payments (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        payment_id BIGINT,
        subscription_id BIGINT NOT NULL,
        amount DECIMAL(10,2),
        currency VARCHAR(10),
        payment_status VARCHAR(20),
        payment_date DATE,
        payment_method VARCHAR(20)
    );

    CREATE TABLE IF NOT EXISTS public.users(
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        user_id INTEGER,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        email VARCHAR(255),
        country VARCHAR(100),
        age INTEGER,
        gender VARCHAR(20),
        acquisition_channel VARCHAR(100),
        is_enterprise BOOLEAN,
        created_at TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS public.subscriptions (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        subscription_id BIGINT,
        user_id BIGINT NOT NULL,
        plan_id BIGINT NOT NULL,
        start_date DATE,
        end_date DATE,
        status VARCHAR(20),
        current_mrr DECIMAL(10,2),
        created_at TIMESTAMP WITHOUT TIME ZONE
    );

    CREATE TABLE IF NOT EXISTS public.subscription_plans (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        plan_id BIGINT,
        product_id BIGINT NOT NULL,
        tier VARCHAR(30),
        billing_cycle VARCHAR(20),
        price DECIMAL(10,2),
        currency VARCHAR(10),
        created_at TIMESTAMP WITHOUT TIME ZONE)
    ;

    CREATE TABLE IF NOT EXISTS public.products(
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        product_id INTEGER,
        product_name VARCHAR(100),
        category VARCHAR(100),
        created_at TIMESTAMP
    );     

    CREATE TABLE IF NOT EXISTS public.subscription_changes (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        subscription_id BIGINT NOT NULL,
        old_plan_id INT,
        new_plan_id INT,
        change_type VARCHAR(20),
        change_date DATE
    );

    CREATE TABLE IF NOT EXISTS public.license_keys (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        subscription_id BIGINT NOT NULL,
        max_seats INT,
        issued_date DATE,
        expiry_date DATE
    );

    CREATE TABLE IF NOT EXISTS public.license_allocations (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        license_id BIGINT NOT NULL,
        seat_number INT,
        status VARCHAR(20),
        allocation_date DATE
    );

    CREATE TABLE IF NOT EXISTS public.support_tickets (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        user_id BIGINT NOT NULL,
        category VARCHAR(50),
        description TEXT,
        created_at TIMESTAMP WITHOUT TIME ZONE
    );

    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
    """
    )

    tables
create_tables()
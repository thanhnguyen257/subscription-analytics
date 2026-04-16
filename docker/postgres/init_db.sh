#!/bin/bash

set -e

CONN_STR="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/postgres"

echo "Initializing PostgreSQL database..."

psql --set ON_ERROR_STOP=1 "$CONN_STR" -tc \
"SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DB}'" | grep -q 1 || \
psql "$CONN_STR" -c "CREATE DATABASE ${POSTGRES_DB}"

POSTGRES_CONN_STR="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"

psql "$POSTGRES_CONN_STR" \
-c "CREATE SCHEMA ${POSTGRES_SCHEMA_SOURCE};"
psql "$POSTGRES_CONN_STR" \
-c "CREATE SCHEMA ${POSTGRES_SCHEMA_LANDING};"

psql "$POSTGRES_CONN_STR" <<-EOSQL

CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA_SOURCE}.users (
    user_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    country VARCHAR(50),
    age INTEGER,
    gender VARCHAR(10),
    acquisition_channel VARCHAR(30),
    is_enterprise BOOLEAN,
    created_at TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA_SOURCE}.products (
    product_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    created_at TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA_SOURCE}.subscription_plans (
    plan_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id BIGINT NOT NULL,
    tier VARCHAR(30),
    billing_cycle VARCHAR(20),
    price DECIMAL(10,2),
    currency VARCHAR(10),
    created_at TIMESTAMP WITHOUT TIME ZONE,
    FOREIGN KEY (product_id) REFERENCES ${POSTGRES_SCHEMA_SOURCE}.products(product_id)
);

CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA_SOURCE}.subscriptions (
    subscription_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id BIGINT NOT NULL,
    plan_id BIGINT NOT NULL,
    start_date DATE,
    end_date DATE,
    status VARCHAR(20),
    current_mrr DECIMAL(10,2),
    created_at TIMESTAMP WITHOUT TIME ZONE,
    FOREIGN KEY (user_id) REFERENCES ${POSTGRES_SCHEMA_SOURCE}.users(user_id),
    FOREIGN KEY (plan_id) REFERENCES ${POSTGRES_SCHEMA_SOURCE}.subscription_plans(plan_id)
);

CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA_SOURCE}.subscription_changes (
    change_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subscription_id BIGINT NOT NULL,
    old_plan_id INT,
    new_plan_id INT,
    change_type VARCHAR(20),
    change_date DATE,
    FOREIGN KEY (subscription_id) REFERENCES ${POSTGRES_SCHEMA_SOURCE}.subscriptions(subscription_id)
);

CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA_SOURCE}.payments (
    payment_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subscription_id BIGINT NOT NULL,
    amount DECIMAL(10,2),
    currency VARCHAR(10),
    payment_status VARCHAR(20),
    payment_date DATE,
    payment_method VARCHAR(20),
    FOREIGN KEY (subscription_id) REFERENCES ${POSTGRES_SCHEMA_SOURCE}.subscriptions(subscription_id)
);

CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA_SOURCE}.license_keys (
    license_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subscription_id BIGINT NOT NULL,
    max_seats INT,
    issued_date DATE,
    expiry_date DATE,
    FOREIGN KEY (subscription_id) REFERENCES ${POSTGRES_SCHEMA_SOURCE}.subscriptions(subscription_id)
);

CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA_SOURCE}.license_allocations (
    allocation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    license_id BIGINT NOT NULL,
    seat_number INT,
    status VARCHAR(20),
    allocation_date DATE,
    FOREIGN KEY (license_id) REFERENCES ${POSTGRES_SCHEMA_SOURCE}.license_keys(license_id)
);

CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA_SOURCE}.support_tickets (
    ticket_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id BIGINT NOT NULL,
    category VARCHAR(50),
    description TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE,
    FOREIGN KEY (user_id) REFERENCES ${POSTGRES_SCHEMA_SOURCE}.users(user_id)
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ${POSTGRES_SCHEMA_SOURCE} TO ${POSTGRES_USER};

EOSQL

psql "$POSTGRES_CONN_STR" \
-c "\copy ${POSTGRES_SCHEMA_SOURCE}.users FROM '/master_db/users.csv' CSV HEADER"
psql "$POSTGRES_CONN_STR" \
-c "\copy ${POSTGRES_SCHEMA_SOURCE}.products FROM '/master_db/products.csv' CSV HEADER"
psql "$POSTGRES_CONN_STR" \
-c "\copy ${POSTGRES_SCHEMA_SOURCE}.subscription_plans FROM '/master_db/plans.csv' CSV HEADER"
psql "$POSTGRES_CONN_STR" \
-c "\copy ${POSTGRES_SCHEMA_SOURCE}.subscriptions FROM '/master_db/subscriptions.csv' CSV HEADER"
psql "$POSTGRES_CONN_STR" \
-c "\copy ${POSTGRES_SCHEMA_SOURCE}.subscription_changes FROM '/master_db/changes.csv' CSV HEADER"
psql "$POSTGRES_CONN_STR" \
-c "\copy ${POSTGRES_SCHEMA_SOURCE}.payments FROM '/master_db/payments.csv' CSV HEADER"
psql "$POSTGRES_CONN_STR" \
-c "\copy ${POSTGRES_SCHEMA_SOURCE}.license_keys FROM '/master_db/licenses.csv' CSV HEADER"
psql "$POSTGRES_CONN_STR" \
-c "\copy ${POSTGRES_SCHEMA_SOURCE}.license_allocations FROM '/master_db/allocations.csv' CSV HEADER"
psql "$POSTGRES_CONN_STR" \
-c "\copy ${POSTGRES_SCHEMA_SOURCE}.support_tickets FROM '/master_db/support_tickets.csv' CSV HEADER"

psql "$POSTGRES_CONN_STR" <<-EOSQL

CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA_LANDING}.staging_usage_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    data JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE
);

EOSQL


echo "PostgreSQL initialization completed!"
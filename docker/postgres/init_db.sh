#!/bin/bash

set -e

CONN_STR="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/postgres"

echo "Initializing PostgreSQL database..."

psql --set ON_ERROR_STOP=1 "$CONN_STR" -tc \
"SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DB}'" | grep -q 1 || \
psql "$CONN_STR" -c "CREATE DATABASE ${POSTGRES_DB}"

POSTGRES_CONN_STR="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"

psql "$POSTGRES_CONN_STR" <<-EOSQL

CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    country VARCHAR(50),
    age INTEGER,
    gender VARCHAR(10),
    acquisition_channel VARCHAR(30),
    is_enterprise BOOLEAN,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    product_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ${POSTGRES_USER};

EOSQL

psql "$POSTGRES_CONN_STR" \
-c "\copy users FROM '/master_db/users.csv' CSV HEADER"
psql "$POSTGRES_CONN_STR" \
-c "\copy products FROM '/master_db/products.csv' CSV HEADER"


echo "PostgreSQL initialization completed!"
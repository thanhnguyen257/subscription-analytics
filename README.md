# Subscription Analytics Mock Project

## Table of Contents
- Project Overview
- Objectives
- Architecture
- Tech Stack
- Repository Structure
- Data Model
- Data Pipeline
- Data Transformation (Bronze → Gold)
- Airflow Orchestration
- Databricks Notebooks
- Machine Learning
- Azure AI Integration
- Analytics & Metrics
- Getting Started
- Testing
- Demo & Screenshots
- Team & Contributions
- Limitations & Future Work
- License

## Project Overview

This project builds a full end-to-end data platform for a subscription-based business.

It:
- Ingests multi-source data (CSV, API, PostgreSQL)
- Processes data using Lakehouse architecture
- Applies Machine Learning models
- Uses Azure AI services for NLP
- Produces business dashboards for SaaS metrics

## Objectives

- Build a real-world Data Engineering pipeline
- Implement Bronze / Silver / Gold architecture
- Use Apache Airflow for orchestration
- Apply ML for churn prediction, forecasting, segmentation
- Integrate Azure AI services
- Deliver insights

## Architecture
```
Sources → Ingestion → Bronze → Silver → Gold → ML → Dashboard
```

## Tech Stack

- Git
- PostgreSQL
- Apache Spark
- Databricks
- Apache Airflow
- scikit-learn
- Azure Cognitive Services

## Repository Structure
```
.
├── data/
├── airflow/dags/
├── notebooks/
├── src/
├── sql/
├── dashboards/
├── tests/
└── README.md
```

## Data Model

Core tables:
- users
- subscriptions
- payments
- usage_events
- support_tickets
- license_keys
- license_allocations

## Data Pipeline

Sources:
- CSV (batch)
- API JSON (stream)
- PostgreSQL (DB-to-DB)

## Data Transformation (Bronze → Gold)

Bronze:
- Raw data

Silver:
- Cleaned data

Gold:
- Aggregated data

Example:
```
SELECT * FROM bronze.subscriptions;

SELECT DISTINCT user_id FROM silver.subscriptions;

SELECT SUM(amount) FROM gold.revenue;
```

Before vs After:
- Bronze: raw, noisy
- Silver: cleaned
- Gold: business-ready

## Airflow Orchestration

DAGs:
- Ingestion
- Transformation
- ML

Add screenshots:
- docs/images/airflow_dag_1.png
- docs/images/airflow_dag_2.png

## Databricks Notebooks

- Bronze: ingestion
- Silver: cleaning
- Gold: aggregation

Add links:
- Bronze notebook link
- Silver notebook link
- Gold notebook link

## Machine Learning

Models:
- Churn prediction
- Forecasting
- Segmentation

Metrics:
- Accuracy: 0.85
- F1-score: 0.82
- RMSE: 1200
- Silhouette: 0.67

## Azure AI Integration

- Sentiment analysis
- Key phrase extraction

## Analytics & Metrics

- MRR
- ARR
- Churn Rate
- Retention Rate
- LTV
- CAC Ratio
- NRR

## Getting Started
```
git clone <repo>
cd project
pip install -r requirements.txt
```
Run Airflow:
```airflow standalone```

Run pipeline:
```python src/main.py```

## Testing

```pytest tests/```

## Demo & Screenshots

- Dashboard screenshots
- Pipeline screenshots

## Team & Contributions

- Data Engineer
- ML Engineer
- Data Analyst

## Limitations & Future Work

Limitations:
- Synthetic data
- Limited real-time

Future:
- Kafka
- CI/CD
- Full cloud deployment

## License

Educational project 

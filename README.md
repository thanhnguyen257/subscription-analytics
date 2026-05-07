# Azure Cloud-Based Subscription & License Analytics Platform

An end-to-end SaaS analytics platform built with Azure, Databricks, Apache Airflow, PySpark, and Machine Learning to process subscription, payment, usage, and license data into business-ready insights and predictive analytics.

## Table of Contents

- [Project Overview](#project-overview)
- [Business Objectives](#business-objectives)
- [Architecture](#architecture)
- [Lakehouse Design](#lakehouse-design)
- [Tech Stack](#tech-stack)
- [Data Sources](#data-sources)
- [Data Pipeline](#data-pipeline)
- [Data Transformation](#data-transformation)
- [Data Modeling](#data-modeling)
- [Machine Learning Pipeline](#machine-learning-pipeline)
- [Airflow Orchestration](#airflow-orchestration)
- [Analytics & Dashboards](#analytics--dashboards)
- [Getting Started](#getting-started)
- [Challenges](#challenges)
- [Future Improvements](#future-improvements)
- [Team](#team)
- [Project Report](#project-report)
- [License](#license)

---

# Project Overview

Modern SaaS businesses generate large volumes of subscription, payment, product usage, license allocation, and support ticket data. Without a centralized analytics platform, it becomes difficult to monitor critical KPIs such as:

- Monthly Recurring Revenue (MRR)
- Annual Recurring Revenue (ARR)
- Churn Rate
- Retention Rate
- Net Revenue Retention (NRR)
- License Utilization
- Customer Segmentation
- Revenue Forecasting

This project builds a cloud-based analytics platform that integrates data engineering, machine learning, orchestration, and BI reporting into a single end-to-end architecture.

The platform:
- Ingests data from multiple heterogeneous sources
- Processes data using a Medallion Lakehouse Architecture
- Orchestrates pipelines with Apache Airflow
- Uses Databricks + Spark for scalable transformation
- Applies ML models for churn prediction and forecasting
- Integrates Azure AI for sentiment analysis
- Delivers business dashboards using Power BI

---

# Business Objectives

The main objectives of this project are:

- Build a real-world modern data platform
- Implement Bronze / Silver / Gold Lakehouse architecture
- Automate pipelines using Apache Airflow
- Process batch and streaming-style data
- Create business-ready analytical datasets
- Apply machine learning models for predictive analytics
- Generate actionable SaaS business insights
- Support executive KPI monitoring and decision-making

---

# Architecture

## High-Level Architecture

```text
CSV / SQL / API Sources
            ↓
      Apache Airflow
            ↓
 PostgreSQL Staging Layer
            ↓
 Azure Data Lake Storage
            ↓
     Databricks + Spark
            ↓
 Bronze → Silver → Gold
            ↓
 ML Models / Power BI
```

## Core Architecture Components

### Data Sources
- CSV datasets
- Azure SQL Database
- FastAPI-generated usage events
- PostgreSQL staging database

### Processing Layer
- Apache Spark
- Databricks
- Delta Lake
- Medallion Architecture

### Consumption Layer
- Power BI dashboards
- Machine Learning pipelines
- Azure AI Language Services

---

# Lakehouse Design

The platform follows the Medallion Architecture pattern:

## Bronze Layer
Raw ingested data with minimal transformation.

Features:
- Source fidelity preservation
- Incremental ingestion
- Metadata tracking
- Append-only processing

## Silver Layer
Cleaned and validated datasets.

Features:
- Deduplication
- Schema enforcement
- Data quality validation
- Referential integrity checks
- Business rule implementation

## Gold Layer
Business-ready analytical tables optimized for reporting and ML.

Features:
- Aggregated KPIs
- Star-schema modeling
- Cohort analytics
- Revenue analytics
- License utilization metrics

---

# Tech Stack

## Data Engineering
- Apache Spark
- Databricks
- Delta Lake
- Apache Airflow
- PostgreSQL

## Cloud & Storage
- Microsoft Azure
- Azure Blob Storage
- Azure Data Lake Storage (ADLS)

## Machine Learning
- scikit-learn
- Pandas
- NumPy

## AI & NLP
- Azure AI Language Service
- Azure Cognitive Services

## BI & Visualization
- Power BI

## DevOps & Collaboration
- GitHub
- GitLab Workflow
- Jira

---

# Data Sources

The platform integrates multiple datasets representing SaaS business operations.

## Core Tables

| Table | Description |
|---|---|
| users | Customer information |
| subscriptions | Subscription lifecycle data |
| payments | Revenue transactions |
| products | SaaS products |
| plans | Subscription plans |
| usage_events | User activity events |
| support_tickets | Customer support interactions |
| license_keys | License management |
| license_allocations | Seat allocation tracking |

---

# Data Pipeline

## Batch Ingestion
Handled using Apache Airflow:
- CSV ingestion
- Azure SQL extraction
- Incremental batch loading

## Streaming-Style Ingestion
Handled through FastAPI:
- Real-time usage events
- JSON payload ingestion
- PostgreSQL staging writes

## Processing Flow

```text
Sources
   ↓
Staging
   ↓
Bronze
   ↓
Silver
   ↓
Gold
   ↓
Dashboards & ML
```

---

# Data Transformation

Transformation logic is implemented using Databricks notebooks and Apache Spark.

## Bronze Processing
- Raw ingestion
- Watermark tracking
- Metadata enrichment

## Silver Processing
- Data cleaning
- Deduplication
- Validation
- Standardization
- Quarantine handling

## Gold Processing
- KPI aggregation
- Cohort calculations
- Revenue modeling
- Retention metrics
- License utilization analytics

## Incremental Processing
The platform uses Delta Lake `MERGE` operations for efficient updates and incremental transformations.

---

# Data Modeling

The Gold layer follows a star-schema analytical design.

## Dimension Tables
- dim_users
- dim_products
- dim_plans

## Fact Tables
- fact_subscriptions
- fact_payments
- fact_usage_events
- fact_support_tickets

## Gold Analytical Tables

| Gold Table | Purpose |
|---|---|
| gold_monthly_mrr | Revenue analytics |
| gold_license_ut | License utilization |
| gold_daily_actv_df | User engagement |
| gold_cohort_retention_overall | Cohort analysis |
| gold_cohort_retention_by_product | Product retention |
| gold_cohort_retention_by_plan | Plan retention |
| gold_cohort_retention_by_country | Geographic retention |
| gold_cohort_retention_by_acquisition_channel | Marketing retention |

---

# Machine Learning Pipeline

## ML Use Cases

### Churn Prediction
Classification models used:
- Logistic Regression
- Random Forest Classifier
- Gradient Boosting Classifier

### MRR Forecasting
Regression models used:
- Linear Regression
- Ridge Regression
- Lasso Regression
- Random Forest Regressor
- Gradient Boosting Regressor

### Customer Segmentation
- K-Means Clustering

### Sentiment Analysis
- Azure AI Language Service
- NLP-based support ticket analysis

---

## Feature Engineering

Example engineered features:
- tenure_days
- total_spend
- support_ticket_count
- usage_frequency
- churn indicators
- subscription type
- payment method count
- device count
- revenue metrics

---

## Model Evaluation

### Churn Prediction
Best model: **Gradient Boosting**

| Metric | Score |
|---|---|
| Accuracy | 0.8353 |
| Recall | 0.8881 |
| F1-score | 0.8572 |

### MRR Prediction
Best model: **Lasso Regression**

| Metric | Score |
|---|---|
| MAE | ~5.3k |
| RMSE | ~8.0k |
| R² | ~0.93 |

### K-Means Clustering
- PCA visualization
- Elbow method
- Silhouette score evaluation

### Sentiment Analysis
Processed:
- 10,000 support ticket reviews
- Negative sentiment dominant (~46%)

---

# Airflow Orchestration

Apache Airflow orchestrates the full pipeline lifecycle.

## DAG Responsibilities
- Data ingestion
- Databricks job execution
- Bronze/Silver/Gold transformations
- ML pipeline execution

## Scheduling Strategy
- Weekly static dataset pipelines
- Daily transactional pipelines
- Incremental processing
- Dependency-based execution

## Pipeline Design Principles
- Modular DAGs
- Independent pipelines
- Reduced recomputation
- Incremental updates

---

# Analytics & Dashboards

The platform delivers Power BI dashboards for SaaS business monitoring.

## Dashboard Areas

### Executive KPI Dashboard
Tracks:
- MRR
- ARR
- NRR
- Churn Rate
- Retention Rate
- Active Subscriptions

### Revenue Analytics
- MRR movement waterfall
- Revenue trends
- Expansion/contraction tracking

### Cohort Retention Analysis
- Retention heatmaps
- Country-level retention
- Product-level retention
- Plan-level retention

### License Utilization Dashboard
- Active seats
- Unused seats
- Utilization percentage
- Enterprise customer tracking

### User Engagement Dashboard
- Daily active users
- Feature activity
- Event tracking
- User engagement trends

---

# Getting Started

## Clone Repository

```bash
git clone https://github.com/thanhnguyen257/subscription-analytics.git
cd subscription-analytics
```

## Install Dependencies

```bash
pip install -r requirements.txt
```

## Run Airflow

```bash
airflow standalone
```

## Execute Pipeline

```bash
python src/main.py
```

---

# Challenges

## Synthetic Data Limitations
The project uses generated datasets, which may not fully reflect real-world SaaS behavioral patterns.

## Resource Constraints
Databricks compute limitations affected:
- Large-scale processing
- Model experimentation
- Pipeline optimization

## License Allocation Granularity
Users are not directly mapped to license allocation records, limiting detailed user-level utilization analysis.

---

# Future Improvements

Potential future enhancements include:

- Kafka-based streaming architecture
- CI/CD automation
- Real-time analytics
- Advanced model tuning
- Hyperparameter optimization
- Production ML deployment
- AI-powered dashboard assistants
- Improved SaaS domain simulation
- Full cloud-native deployment

---

# Team

## Team Members

| Member | Responsibilities |
|---|---|
| Nguyen Le Tuan Thanh | Data Architecture, Data Engineering, Airflow |
| Tran Thi Diem Thuy | ML Pipeline, Dashboards, DevOps |

---

# Project Report

- Full technical documentation included in project report
- Covers architecture, ML pipeline, dashboards, orchestration, and business logic

## GitHub Repository

GitHub:
https://github.com/thanhnguyen257/subscription-analytics

---

# License

This project is developed for educational and portfolio purposes.
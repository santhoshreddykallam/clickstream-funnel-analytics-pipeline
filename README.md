# Clickstream Funnel Analytics Pipeline

An end-to-end **Data Engineering pipeline** that processes large-scale e-commerce clickstream data using **PySpark**, automates ETL workflows with **Apache Airflow**, stores transformed analytics in **PostgreSQL**, and visualizes customer funnel insights through **Power BI**.

---

## Project Overview

This project demonstrates how raw clickstream data can be transformed into meaningful business insights through an automated data pipeline.

The pipeline processes monthly customer clickstream events, calculates funnel metrics such as customer drop-off and cart abandonment, stores the results in PostgreSQL, and presents interactive dashboards in Power BI.

---

## Architecture

<p align="center">
<img src="screenshots/architecture.png" width="900">
</p>

---

## Technology Stack

| Category | Technology |
|----------|------------|
| Programming | Python |
| Big Data Processing | PySpark |
| Workflow Orchestration | Apache Airflow |
| Containerization | Docker, Docker Compose |
| Database | PostgreSQL |
| Visualization | Power BI |
| Version Control | Git & GitHub |

---

## Pipeline Workflow

```
Raw Clickstream CSV Files
            │
            ▼
     PySpark ETL Pipeline
            │
            ▼
 Data Cleaning & Transformation
            │
            ▼
 Funnel Metrics Calculation
            │
            ▼
      PostgreSQL Database
            │
            ▼
 Apache Airflow Orchestration
            │
            ▼
      Power BI Dashboard
```

---

## Features

- Automated ETL pipeline using Apache Airflow
- Distributed data processing with PySpark
- Dockerized development environment
- PostgreSQL data warehouse
- Monthly funnel analysis
- Customer drop-off analysis
- Cart abandonment analysis
- Interactive Power BI dashboard
- Modular Python scripts

---

## Project Structure

```
clickstream-funnel-analytics-pipeline
│
├── airflow/
│   ├── dags/
│   │   └── clickstream_pipeline_dag.py
│   ├── logs/
│   ├── plugins/
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   └── requirements.txt
│
├── screenshots/
│   ├── architecture.png
│   ├── airflow_dag.png
│   └── dashboard.png
│
├── ingest.py
├── transform.py
├── quality_check.py
├── load_postgres.py
├── Clickstream_Funnel_Analytics.pbix
├── .gitignore
└── README.md
```

---

## ETL Pipeline

### 1. Data Ingestion

- Reads monthly clickstream CSV datasets
- Loads data into PySpark DataFrames
- Infers schema automatically

---

### 2. Data Cleaning

- Filters relevant customer events
- Removes invalid records
- Standardizes funnel stages

---

### 3. Transformation

Calculates:

- Unique Sessions
- Funnel Stages
- Previous Stage Sessions
- Drop-off Rate
- Cart Removal Sessions
- Monthly Metrics

---

### 4. Data Loading

The transformed dataset is loaded into PostgreSQL where it serves as the reporting layer for Power BI.

---

## Airflow DAG

Apache Airflow orchestrates the complete ETL workflow.

<p align="center">
<img src="screenshots/airflow_dag.png" width="900">
</p>

Pipeline Tasks:

1. Ingest Data
2. Transform Data
3. Load into PostgreSQL

---

## Dashboard

<p align="center">
<img src="screenshots/dashboard.png" width="900">
</p>

---

## Dashboard KPIs

- Total View Sessions
- Total Cart Sessions
- Total Purchase Sessions
- Cart Abandonments

Visualizations include:

- Customer Conversion Funnel
- Monthly Drop-off Rate Analysis
- Interactive Month Filter

---

## Key Business Insights

### High Customer Drop-off

A significant percentage of users leave between the **View** and **Cart** stages, indicating an opportunity to improve product engagement.

---

### Cart Abandonment

A large number of customers remove products from their carts before purchasing, highlighting friction in the purchase journey.

---

### Monthly Funnel Performance

Tracking funnel metrics across multiple months enables comparison of customer behavior and purchasing trends.

---

## Challenges Solved

- Designed an automated ETL pipeline using Apache Airflow.
- Integrated PySpark, PostgreSQL, Docker, and Power BI into a single workflow.
- Automated monthly processing using dynamic Airflow DAG generation.
- Calculated reusable funnel metrics for business reporting.

---

## Dataset

This project uses the **eCommerce Events History in Cosmetics Shop** dataset from Kaggle.

**Kaggle Dataset:**
https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop

> **Note:** The dataset is not included in this repository due to GitHub's file size limitations. Please download it from Kaggle and place the monthly CSV files in the project root before executing the pipeline.

---

## Prerequisites

Before running the project, ensure you have:

- Docker Desktop
- Python 3.10+
- PostgreSQL
- Power BI Desktop
- Kaggle account (to download the dataset)

---

## How to Run

### Clone the repository

```bash
git clone https://github.com/santhoshreddykallam/clickstream-funnel-analytics-pipeline.git
```

---

### Build Docker Containers

```bash
docker compose up --build
```

---

### Access Airflow

```
http://localhost:8080
```

Default credentials

```
Username: admin

Password: admin
```

---

### Execute the DAG

Trigger the DAG from the Airflow UI.

The pipeline will

- Read CSV files
- Process them using PySpark
- Load results into PostgreSQL

---

### Open Dashboard

Open

```
Clickstream_Funnel_Analytics.pbix
```

using Power BI Desktop.

---

## Future Improvements

- Migrate PostgreSQL to a cloud data warehouse.
- Store raw data in Amazon S3 or Azure Data Lake.
- Deploy Airflow on Kubernetes.
- Add data quality monitoring.
- Integrate CI/CD using GitHub Actions.
- Replace batch processing with streaming using Apache Kafka.

---

## Author

**Santhosh Reddy Kallam**

GitHub

https://github.com/santhoshreddykallam

---

## If you found this project useful

⭐ Star the repository.

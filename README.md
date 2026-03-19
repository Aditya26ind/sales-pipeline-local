# Target Data Pipeline

## Overview

This project demonstrates a local end-to-end data engineering pipeline for retail sales data. It simulates real-time ingestion, processing, analysis, and storage using a modern data stack, all running locally with Docker.

## Architecture

```text
CSV -> Kafka Producer -> Kafka Topic -> Kafka Consumer -> Pandas -> PySpark -> Hive Table
```

## Tech Stack

- Kafka for real-time data streaming
- Pandas for initial data ingestion and handling
- PySpark for transformation and analysis
- Hive for local structured storage
- Docker for containerized local execution

## Pipeline Flow

### 1. Data Ingestion

- Retail sales data is read from a CSV file using Pandas
- Records are sent row by row to a Kafka topic through a producer

### 2. Streaming Layer

- Kafka acts as the messaging layer between producer and consumer
- The consumer continuously reads incoming records from the Kafka topic

### 3. Data Processing

- Streamed records are collected into a Pandas DataFrame
- The dataset is converted into a PySpark DataFrame for scalable processing
- Cleaning and transformation steps include:
  - fixing data types
  - removing null values
  - preparing data for analysis and storage

### 4. Analysis

- Exploratory data analysis is performed on the processed dataset
- Business insights include:
  - sales by city
  - delivery time analysis
  - correlation-based observations

### 5. Storage

- The final processed dataset is written into a local Hive table
- The stored data can be queried for downstream reporting and analysis

## Running Locally

Start the full environment with Docker Compose:

```bash
docker-compose up --build
```

This brings up the required services such as Kafka, Zookeeper, and the Python application containers. Spark runs inside the consumer container, and Hive support is enabled through Spark's local metastore and warehouse directory.

## Project Structure

```text
.
├── app
│   ├── consumer.py
│   ├── pipeline.py
│   └── producer.py
├── data
│   └── retail_sales.csv
├── output
├── Dockerfile
├── docker-compose.yml
├── README.md
└── requirements.txt
```

## Output Artifacts

After the pipeline runs, the `output/` directory contains:

- `cleaned_retail_sales.csv` with transformed records
- `insights.json` with summary business metrics
- `sales_by_city_report/` with Spark-generated city-level aggregates
- `spark-warehouse/` containing the local Hive-managed table data

## Key Features

- Local end-to-end data pipeline
- Kafka-based real-time ingestion simulation
- Scalable transformation with PySpark
- Structured storage using Hive
- Fully containerized setup with Docker

## Project Goal

This project is designed to demonstrate the ability to:

- build a local data pipeline
- combine streaming and batch-style processing
- generate meaningful business insights from retail data

## Future Enhancements

- Deploy the pipeline on AWS using services like S3, Glue, Athena, and Redshift
- Add Airflow for orchestration and scheduling
- Integrate real-time dashboards for business monitoring

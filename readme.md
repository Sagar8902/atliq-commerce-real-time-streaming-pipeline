# AtliQ Commerce – Real-Time Data Streaming Pipeline

**An End-to-End Data Engineering Capstone Project**

An end-to-end data engineering project that demonstrates how e-commerce order and payment events can be ingested, processed, and transformed into business-ready data using Apache Kafka, Databricks, PySpark, Delta Lake, and Apache Airflow.

---

## 1. Project Overview

E-commerce platforms generate a continuous flow of data whenever customers place orders, make payments, or interact with products. Processing these events efficiently is important for businesses that need timely and reliable insights.

In this project, I built an end-to-end real-time data streaming pipeline for **AtliQ Commerce**. The pipeline collects order events through Apache Kafka, processes them using Databricks and Spark Structured Streaming, and stores the results in Delta Lake.

To organise the data, I implemented the **Medallion Architecture (Bronze, Silver, and Gold)**. Each layer has a specific responsibility, from storing raw events to preparing analytics-ready datasets.

I also integrated Apache Airflow to manage supporting operational tasks, including data freshness checks, Delta table optimisation, and daily summary refreshes.

### Business Objectives

- Ingest e-commerce order and payment events through a streaming pipeline.
- Collect and process incoming events from Apache Kafka.
- Preserve raw event data in the Bronze layer.
- Clean, validate, and deduplicate records in the Silver layer.
- Build business-level aggregations in the Gold layer.
- Monitor incoming data freshness and handle data-quality issues.
- Prepare reliable, analytics-ready datasets for business reporting.

---

## 2. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Event generation and data processing |
| Apache Kafka | Real-time event streaming |
| Confluent Cloud | Managed Kafka infrastructure |
| Databricks | Data engineering and processing platform |
| PySpark | Distributed data processing |
| Spark Structured Streaming | Streaming data ingestion and transformation |
| Delta Lake | Reliable storage and table management |
| Apache Airflow | Workflow orchestration and scheduling |
| Databricks SQL Warehouse | SQL-based data operations |
| Docker & Docker Compose | Local Airflow environment |
| Git & GitHub | Version control and project management |

---

## 3. System Architecture

The pipeline follows this flow:

```text
                 E-COMMERCE EVENTS
                        |
                        v
              PYTHON EVENT PRODUCER
                        |
                        v
                  APACHE KAFKA
                 (Confluent Cloud)
                        |
                        v
             DATABRICKS / PYSPARK
              STRUCTURED STREAMING
                        |
                        v
                 BRONZE LAYER
                Raw Kafka Events
                        |
                        v
                 SILVER LAYER
            Cleaned & Validated Events
                        |
                        v
                  GOLD LAYER
             Business Aggregations
                        |
                        v
              ANALYTICS-READY DATA
                        |
                        v
                BUSINESS INSIGHTS


                 APACHE AIRFLOW
                        |
           +------------+------------+
           |            |            |
           v            v            v
         DATA        OPTIMIZE      REFRESH
       FRESHNESS      DELTA         DAILY
         CHECK       TABLES       SUMMARY
```

---

## 4. Medallion Architecture

The pipeline uses three data layers to separate raw data ingestion, data cleaning, and business-level transformations.

### Bronze Layer – Raw Data

The Bronze layer is the entry point for incoming Kafka events. It stores the raw streaming records along with relevant Kafka metadata.

**Key operations:**

- Read incoming events from Apache Kafka.
- Capture raw message payloads and Kafka metadata.
- Write incoming records to Delta tables.
- Maintain streaming checkpoints for recovery.

### Silver Layer – Cleaned and Validated Data

The Silver layer converts raw events into structured and reliable records that can be used for further analysis.

**Key operations:**

- Apply an explicit data schema.
- Parse JSON event payloads.
- Handle missing and invalid records.
- Remove duplicate events.
- Apply event-time processing and watermarking.
- Store cleaned and validated records in Delta tables.

### Gold Layer – Business Aggregations

The Gold layer focuses on preparing processed data for business reporting and analytics.

**Key operations:**

- Aggregate order and payment events.
- Perform event-time window calculations.
- Calculate business metrics such as revenue.
- Generate analytics-ready summary tables.

---

## 5. Real-Time Streaming Features

This project covers several important concepts used in modern streaming data engineering:

- **Kafka Event Ingestion:** Receive e-commerce events through Apache Kafka.
- **Structured Streaming:** Process incoming events using PySpark.
- **Event-Time Processing:** Work with event timestamps rather than relying only on processing time.
- **Watermarking:** Handle late-arriving events within a defined event-time threshold.
- **Duplicate Handling:** Prevent duplicate events from affecting downstream results.
- **Delta Lake Integration:** Write streaming data into Delta tables.
- **Checkpointing:** Maintain streaming progress and support fault recovery.
- **Window-Based Aggregations:** Calculate business metrics such as revenue over time.
- **Medallion Architecture:** Organise data into Bronze, Silver, and Gold layers.

---

## 6. Apache Airflow Orchestration

A streaming pipeline also needs operational monitoring and maintenance.

For this project, I used Apache Airflow to automate the supporting workflows around the Databricks streaming pipeline.

I created an hourly DAG named:

**DAG ID:** `atliq_streaming_ops`

The DAG contains three tasks:

| Task | Description |
|---|---|
| `check_fresh_events` | Checks whether recent streaming events have arrived |
| `optimize_tables` | Optimises Silver and Gold Delta tables |
| `refresh_daily_summary` | Refreshes the daily business summary |

### Task Dependencies

```text
check_fresh_events
        |
        v
optimize_tables
        |
        v
refresh_daily_summary
```

The tasks execute sequentially, ensuring that event freshness is checked before table maintenance and summary refresh operations begin.

The DAG also demonstrates data-quality failure handling when incoming events do not meet the configured freshness requirement.

---

## 7. Project Structure

```text
atliq-commerce-real-time-streaming-pipeline/
│
├── producer/
│   ├── order_event_producer.py
│   ├── .env.example
│   └── requirements.txt
│
├── databricks/
│   └── 01_kafka_stream_processing.py
│
├── atliq-airflow/
│   ├── dags/
│   │   └── atliq_streaming_ops_dag.py
│   ├── docker-compose.yaml
│   └── .env.example
│
├── screenshots/
│   ├── kafka/
│   ├── databricks/
│   └── airflow/
│
├── docs/
│   └── project_writeup.md
│
├── requirements.txt
├── .gitignore
└── README.md
```

*Note: Update the folder and file names above to match your final repository structure.*

---

## 8. Project Setup and Execution

Follow these steps to set up and run the project locally.

### Prerequisites

Before getting started, make sure you have:

- Python 3.10+
- Docker Desktop
- Git
- A Confluent Cloud account
- A Databricks workspace
- A Databricks Personal Access Token (PAT)
- An available Databricks SQL Warehouse

### Step 1 – Clone the Repository

```bash
git clone YOUR_GITHUB_REPOSITORY_URL

cd atliq-commerce-real-time-streaming-pipeline
```

### Step 2 – Create a Python Virtual Environment

Create the environment:

```bash
python -m venv .venv
```

Activate it on Windows:

```bash
.venv\Scripts\activate
```

Install the project dependencies:

```bash
pip install -r requirements.txt
```

### Step 3 – Configure Environment Variables

Create your `.env` file using the provided `.env.example` template.

Add the required credentials and configuration values:

- Kafka bootstrap server
- Kafka API key
- Kafka API secret
- Kafka topic
- Databricks workspace URL
- Databricks access token

**Important:** Never upload actual credentials, API keys, access tokens, or other secrets to GitHub. Keep your `.env` files in `.gitignore` and use the example files as configuration templates.

### Step 4 – Start the Kafka Event Producer

Run the Python producer:

```bash
python producer/order_event_producer.py
```

The producer generates e-commerce order events and publishes them to the configured Kafka topic.

Verify that messages are arriving successfully in your Confluent Cloud Kafka topic.

### Step 5 – Run the Databricks Streaming Notebook

1. Open your Databricks workspace.
2. Import the streaming processing notebook.
3. Configure the required Kafka credentials and storage locations.
4. Execute the notebook.
5. Verify that the Bronze, Silver, and Gold Delta tables are being populated.

### Step 6 – Start Apache Airflow

Navigate to the Airflow project directory:

```bash
cd atliq-airflow
```

Start the Docker containers:

```bash
docker compose up -d
```

Check the container status:

```bash
docker compose ps
```

Open the Airflow web interface:

```text
http://localhost:8080
```

Configure the `databricks_default` Airflow connection with the required Databricks workspace credentials and SQL Warehouse details.

Finally, enable and trigger the `atliq_streaming_ops` DAG.

---

## 9. Expected Results

After completing the setup and execution, the project should demonstrate the following:

1. Order events are generated and successfully published to Kafka.
2. Databricks ingests incoming events through Spark Structured Streaming.
3. Raw Kafka records are stored in the Bronze layer.
4. Invalid and duplicate records are handled in the Silver layer.
5. Business aggregations are generated in the Gold layer.
6. All three Airflow tasks execute successfully when their conditions are met.
7. The data freshness validation fails when incoming events do not satisfy the configured requirement.

---

## 10. Screenshots and Project Evidence

Screenshots from the implementation and execution will be included here to demonstrate the different components of the pipeline.

| Screenshot | What It Demonstrates |
|---|---|
| Kafka Producer | Generated order events |
| Kafka Topic | Events arriving in Confluent Cloud |
| Bronze Layer | Raw streaming records |
| Silver Layer | Cleaned and validated records |
| Gold Layer | Aggregated business metrics |
| Airflow Success | Successful DAG execution |
| Airflow Failure | Data freshness validation failure |

---

## 11. Key Learning Outcomes

Working on this project helped me strengthen my practical understanding of modern data engineering and streaming architecture.

Some of the key areas I worked on include:

- Designing and implementing an end-to-end real-time data pipeline.
- Generating and publishing events using Python and Apache Kafka.
- Working with Confluent Cloud for managed Kafka infrastructure.
- Processing streaming data using PySpark and Spark Structured Streaming.
- Implementing the Medallion Architecture with Delta Lake.
- Handling late-arriving events and duplicate records.
- Working with streaming checkpoints and fault recovery.
- Building event-time window aggregations.
- Automating operational workflows using Apache Airflow.
- Running local orchestration services with Docker and Docker Compose.

---

## 12. Author

**Sagar Soni**

Data Engineer | SQL | Python | PySpark | Apache Kafka | Databricks | Delta Lake | Microsoft Azure | Apache Airflow

**GitHub:** https://github.com/Sagar8902

**LinkedIn:** www.linkedin.com/in/sagar7502

---

⭐ If you find this project useful, feel free to explore the repository, share your feedback, or connect with me on LinkedIn.


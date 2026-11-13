"""
AtliQ Phase 2 (LEARNER STARTER) — Streaming Ops DAG
====================================================
The stream never stops — but the SCHEDULED work around it is your job:
data-quality gate, table maintenance, daily rollup. Build an hourly DAG:

    check_fresh_events  ->  optimize_tables  ->  refresh_daily_summary

Setup:
1. In docker-compose.yaml:  _PIP_ADDITIONAL_REQUIREMENTS: apache-airflow-providers-databricks
   then: docker compose down && docker compose up -d
2. Airflow UI -> Admin -> Connections -> +
   Conn Id: databricks_default | Type: Databricks
   Host: https://<workspace>.cloud.databricks.com | Password: <PAT token>
   (Azure Databricks: Host is https://adb-<workspace-id>.<n>.azuredatabricks.net)
3. Paste your SQL warehouse HTTP path below.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

DATABRICKS_CONN_ID = "databricks_default"

SQL_WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/5b2a7a5e295443e9"   # <-- yours

default_args = {
    "owner": "atliq-data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="atliq_streaming_ops",
    start_date=datetime(2026, 11, 10),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["atliq", "phase2", "streaming"],
) as dag:

    # TASK 1: Data Quality Gate
    # Check whether recent events have arrived in Silver.

    check_fresh_events = DatabricksSqlOperator(
        task_id="check_fresh_events",

        databricks_conn_id="databricks_default",

        http_path=SQL_WAREHOUSE_HTTP_PATH,

        sql="""
            SELECT assert_true(
                COUNT(*) > 0,
                'Data quality failed: No recent Silver events found.'
            )
            FROM atliq.streaming.silver_order_events
            WHERE event_ts >= current_timestamp() - INTERVAL 2 HOURS
        """
    )


    # TASK 2: Optimize Silver and Gold Delta tables

    optimize_tables = DatabricksSqlOperator(
        task_id="optimize_tables",

        databricks_conn_id="databricks_default",

        http_path=SQL_WAREHOUSE_HTTP_PATH,

        sql=[
            "OPTIMIZE atliq.streaming.silver_order_events",
            "OPTIMIZE atliq.streaming.gold_revenue_5min"
        ]
    )


    # TASK 3: Generate daily business summary

    refresh_daily_summary = DatabricksSqlOperator(
        task_id="refresh_daily_summary",

        databricks_conn_id="databricks_default",

        http_path=SQL_WAREHOUSE_HTTP_PATH,

        sql="""
            CREATE OR REPLACE TABLE
            atliq.streaming.gold_daily_summary AS

            SELECT
                TO_DATE(event_ts) AS event_date,

                COUNT_IF(
                    event_type = 'order_placed'
                ) AS orders_placed,

                COUNT_IF(
                    event_type = 'payment_received'
                ) AS orders_paid,

                COUNT_IF(
                    event_type = 'order_cancelled'
                ) AS orders_cancelled,

                SUM(
                    CASE
                        WHEN event_type = 'payment_received'
                        THEN CAST(order_amount AS DECIMAL(18,2))
                        ELSE 0
                    END
                ) AS revenue

            FROM atliq.streaming.silver_order_events

            GROUP BY TO_DATE(event_ts)
        """
    )


    # TASK 4: Define execution order

    check_fresh_events >> optimize_tables >> refresh_daily_summary
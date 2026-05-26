from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging
import json

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
   'retries': 2,
   'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
}

# DAG definition
dag = DAG(
    dag_id='sigma_transaction_pipeline',
    default_args=default_args,
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    sla_miss_callback=lambda context: logging.warning(
        f"SLA miss for DAG {context['dag'].dag_id} at {context['execution_date']}"
    ),
    on_failure_callback=lambda context: logging.error(
        f"Failure in DAG {context['dag'].dag_id}, Task {context['task'].task_id} at {context['execution_date']}: {context['exception']}"
    ),
    tags=['sigma', 'transactions', 'daily'],
    description="Daily Bronze->Silver->Gold pipeline for Sigma DataTech transactions"
)

def log_task_status(context):
    """Logs the start and end of a task with task instance info."""
    task_instance = context['task_instance']
    logging.info(f"Task {task_instance.task_id} started at {task_instance.start_date}")
    logging.info(f"Task {task_instance.task_id} ended at {task_instance.end_date}")

def extract_bronze(**context):
    """Ingest raw CSVs to Bronze Parquet."""
    log_task_status(context)
    # Placeholder for actual data extraction logic
    raise NotImplementedError("Bronze extraction logic not implemented")

def transform_silver(**context):
    """Clean, enrich, deduplicate to Silver."""
    log_task_status(context)
    # Placeholder for actual data transformation logic
    raise NotImplementedError("Silver transformation logic not implemented")

def build_gold(**context):
    """Generate the 3 Gold aggregation tables."""
    log_task_status(context)
    # Placeholder for actual data aggregation logic
    raise NotImplementedError("Gold aggregation logic not implemented")

# Task definitions with on_failure_callback
extract_bronze_task = PythonOperator(
    task_id='extract_bronze',
    python_callable=extract_bronze,
    on_failure_callback=lambda context: logging.error(
        f"Failure in Task {context['task'].task_id} at {context['execution_date']}: {context['exception']}"
    ),
    dag=dag,
)

transform_silver_task = PythonOperator(
    task_id='transform_silver',
    python_callable=transform_silver,
    on_failure_callback=lambda context: logging.error(
        f"Failure in Task {context['task'].task_id} at {context['execution_date']}: {context['exception']}"
    ),
    dag=dag,
)

build_gold_task = PythonOperator(
    task_id='build_gold',
    python_callable=build_gold,
    on_failure_callback=lambda context: logging.error(
        f"Failure in Task {context['task'].task_id} at {context['execution_date']}: {context['exception']}"
    ),
    dag=dag,
)

# Task dependencies
extract_bronze_task >> transform_silver_task >> build_gold_task

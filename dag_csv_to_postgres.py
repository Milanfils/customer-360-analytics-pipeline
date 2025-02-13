from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging
import os
import sys

# Set up logging
logger = logging.getLogger(__name__)

# Default DAG arguments with proper error handling and retries
default_args = {
    'owner': 'customer360_admin',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Constants and configurations
DATA_PATH = '/opt/data/raw'
FILE_PATHS = {
    'dim_date': os.path.join(DATA_PATH, 'dim_date.csv'),
    'customers': os.path.join(DATA_PATH, 'customers.csv'),
    'orders': os.path.join(DATA_PATH, 'orders.csv'),
    'campaigns': os.path.join(DATA_PATH, 'campaigns.csv'),
    'campaign_costs': os.path.join(DATA_PATH, 'campaign_costs.csv'),
    'product_costs': os.path.join(DATA_PATH, 'product_costs.csv'),
}

POSTGRES_CONN_ID = 'postgres_customer360'

def load_csv_to_postgres(table_name: str, csv_file: str) -> None:
    """
    Load data from a CSV file into a PostgreSQL table using COPY command.
    
    Args:
        table_name: Name of the target PostgreSQL table
        csv_file: Path to the source CSV file
        
    Raises:
        FileNotFoundError: If CSV file doesn't exist
        Exception: For any database operation errors
    """
    logger.info(f"Starting data load for table: {table_name}")
    
    if not os.path.exists(csv_file):
        error_msg = f"CSV file not found: {csv_file}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Truncate existing data
        logger.info(f"Truncating table: {table_name}")
        truncate_query = f"TRUNCATE {table_name} RESTART IDENTITY CASCADE;"
        cursor.execute(truncate_query)
        
        # Load new data
        logger.info(f"Loading data from {csv_file}")
        with open(csv_file, 'r') as f:
            next(f)  # skip header
            cursor.copy_expert(f"COPY {table_name} FROM STDIN CSV", f)
        
        # Verify row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        logger.info(f"Loaded {row_count} rows into {table_name}")
        
        conn.commit()
        
    except Exception as e:
        error_msg = f"Error processing {table_name}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            
    logger.info(f"Completed data load for table: {table_name}")

# Define the DAG
# DAG definition
with DAG(
    dag_id='csv_to_postgres_etl',
    default_args=default_args,
    description='ETL pipeline to load CSV data into PostgreSQL and perform transformations',
    schedule_interval='@daily',
    catchup=False,
    tags=['customer360', 'analytics'],
    max_active_runs=1
) as dag:

    # Group 1: Reference Data Loading
    with TaskGroup("load_reference_data") as ref_data_group:
        task_ingest_dim_date = PythonOperator(
            task_id='load_dim_date',
            python_callable=lambda: load_csv_to_postgres('dim_date', FILE_PATHS['dim_date']),
            retries=3,
        )

    # Group 2: Customer Data Loading
    with TaskGroup("load_customer_data") as customer_data_group:
        task_ingest_customers = PythonOperator(
            task_id='load_customers',
            python_callable=lambda: load_csv_to_postgres('customers', FILE_PATHS['customers']),
            retries=3,
        )

        task_ingest_orders = PythonOperator(
            task_id='load_orders',
            python_callable=lambda: load_csv_to_postgres('orders', FILE_PATHS['orders']),
            retries=3,
        )

    # Group 3: Marketing Data Loading
    with TaskGroup("load_marketing_data") as marketing_data_group:
        task_ingest_campaigns = PythonOperator(
            task_id='load_campaigns',
            python_callable=lambda: load_csv_to_postgres('campaigns', FILE_PATHS['campaigns']),
            retries=3,
        )

        task_ingest_campaign_costs = PythonOperator(
            task_id='load_campaign_costs',
            python_callable=lambda: load_csv_to_postgres('campaign_costs', FILE_PATHS['campaign_costs']),
            retries=3,
        )

    # Group 4: Product Data Loading
    with TaskGroup("load_product_data") as product_data_group:
        task_ingest_product_costs = PythonOperator(
            task_id='load_product_costs',
            python_callable=lambda: load_csv_to_postgres('product_costs', FILE_PATHS['product_costs']),
            retries=3,
        )

    # Group 5: Transformations
    with TaskGroup("transform_data") as transform_group:
        # Load and validate transformations
        # Load and validate transformations
        run_transformations = PostgresOperator(
            task_id='run_transformations',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='scripts/sql/transformations.sql',
            autocommit=True,
            retries=3,
        )
    # Set up dependencies
    ref_data_group >> customer_data_group >> [marketing_data_group, product_data_group] >> transform_group

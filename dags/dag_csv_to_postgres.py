from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from contextlib import contextmanager
import logging
import os
import time
import random

# Set up logging
logger = logging.getLogger(__name__)

class PostgresTransactionHook(PostgresHook):
    """Extended PostgresHook with better transaction management."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.isolation_level = "SERIALIZABLE"
        self.max_retries = 5
        self.initial_backoff = 1
        self.max_backoff = 30
        self.schema = kwargs.get('schema', 'public')
        
    @contextmanager
    def transaction_with_retry(self):
        retry_count = 0
        backoff = self.initial_backoff
        
        while True:
            try:
                conn = self.get_conn()
                conn.set_isolation_level(self.isolation_level)
                cursor = conn.cursor()
                
                try:
                    yield cursor
                    conn.commit()
                    break
                except Exception as e:
                    conn.rollback()
                    if "deadlock detected" in str(e).lower() and retry_count < self.max_retries:
                        retry_count += 1
                        logger.warning(f"Deadlock detected (attempt {retry_count}/{self.max_retries}). Retrying in {backoff} seconds...")
                        time.sleep(backoff)
                        backoff = min(backoff * 2 + random.uniform(0, 1), self.max_backoff)
                        continue
                    raise
                finally:
                    cursor.close()
            except Exception as e:
                if retry_count >= self.max_retries:
                    logger.error(f"Max retries ({self.max_retries}) exceeded. Last error: {str(e)}")
                    raise
            finally:
                if 'conn' in locals():
                    conn.close()

    def acquire_table_lock(self, cursor, table_name):
        """Acquire an exclusive lock on the table."""
        logger.info(f"Acquiring exclusive lock on table {table_name}")
        cursor.execute(f"LOCK TABLE {self.schema}.{table_name} IN EXCLUSIVE MODE")

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
SQL_SCRIPTS_PATH = '/opt/airflow/scripts/sql'
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
        pg_hook = PostgresTransactionHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        with pg_hook.transaction_with_retry() as cursor:
            # Acquire table lock
            pg_hook.acquire_table_lock(cursor, table_name)
            
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
        
    except Exception as e:
        error_msg = f"Error processing {table_name}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)
            
    logger.info(f"Completed data load for table: {table_name}")

# Define the DAG
with DAG(
    dag_id='customer360_analytics_pipeline',
    default_args=default_args,
    description='Load and transform customer 360 analytics data',
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
        # Read SQL file content
        with open(os.path.join(SQL_SCRIPTS_PATH, 'transformations.sql'), 'r') as file:
            sql_content = file.read()
        
        run_transformations = PostgresOperator(
            task_id='run_transformations',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=sql_content,
            autocommit=True,
            retries=3,
        )

    # Set up dependencies
    ref_data_group >> customer_data_group >> [marketing_data_group, product_data_group] >> transform_group

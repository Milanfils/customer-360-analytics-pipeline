from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import pandas as pd
import logging

# Setup logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
}

# Function to load CSV file into PostgreSQL
def load_csv_to_postgres(csv_path, table_name, **context):
    logger.info(f"Loading data from {csv_path} into {table_name}")
    try:
        # Read CSV file
        df = pd.read_csv(csv_path)
        logger.info(f"Successfully read {len(df)} rows from {csv_path}")
        
        # Get Postgres connection
        pg_hook = PostgresHook(postgres_conn_id='postgres_customer360')
        engine = pg_hook.get_sqlalchemy_engine()
        
        # Load data into PostgreSQL
        df.to_sql(table_name, engine, if_exists='append', index=False)
        logger.info(f"Successfully loaded {len(df)} rows into {table_name}")
        
    except Exception as e:
        logger.error(f"Error loading {csv_path} into {table_name}: {str(e)}")
        raise

# DAG definition
with DAG(
    'customer360_pipeline',
    default_args=default_args,
    description='Customer 360 Analytics Pipeline',
    schedule_interval='0 6 * * *',  # Run daily at 6 AM
    catchup=False,
    max_active_runs=1,
    template_searchpath=['/opt/airflow/scripts/sql']
) as dag:
    
    # File sensors to check if source files exist
    file_sensors = {}
    for file_name in ['dim_date.csv', 'customers.csv', 'orders.csv', 
                    'campaigns.csv', 'campaign_costs.csv', 'product_costs.csv']:
        file_sensors[file_name] = FileSensor(
            task_id=f'check_{file_name.replace(".csv", "")}',
            filepath=f'/opt/airflow/data/raw/{file_name}',
            fs_conn_id='fs_default',
            poke_interval=300,  # Check every 5 minutes
            timeout=3600,  # Timeout after 1 hour
            mode='reschedule'  # Release worker slot while waiting
        )
    
    # Task to load dimension and reference data
    load_dim_date = PythonOperator(
        task_id='load_reference_data.load_dim_date',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_path': '/opt/airflow/data/raw/dim_date.csv',
            'table_name': 'dim_date'
        }
    )
    
    load_product_costs = PythonOperator(
        task_id='load_reference_data.load_product_costs',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_path': '/opt/airflow/data/raw/product_costs.csv',
            'table_name': 'product_costs'
        }
    )
    
    # Task to load customer data
    load_customers = PythonOperator(
        task_id='load_customers',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_path': '/opt/airflow/data/raw/customers.csv',
            'table_name': 'customers'
        }
    )
    
    # Tasks to load transactional data
    load_orders = PythonOperator(
        task_id='load_orders',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_path': '/opt/airflow/data/raw/orders.csv',
            'table_name': 'orders'
        }
    )
    
    # Tasks to load marketing data
    load_campaigns = PythonOperator(
        task_id='load_campaigns',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_path': '/opt/airflow/data/raw/campaigns.csv',
            'table_name': 'campaigns'
        }
    )
    
    load_campaign_costs = PythonOperator(
        task_id='load_campaign_costs',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_path': '/opt/airflow/data/raw/campaign_costs.csv',
            'table_name': 'campaign_costs'
        }
    )
    
    # Task to run data quality checks
    data_quality_checks = PostgresOperator(
        task_id='run_data_quality_checks',
        postgres_conn_id='postgres_customer360',
        sql="""
            SELECT 
                CASE 
                    WHEN EXISTS (SELECT 1 FROM customers WHERE customer_id IS NULL) THEN 'Null customer_id found'
                    WHEN EXISTS (SELECT 1 FROM orders WHERE order_id IS NULL) THEN 'Null order_id found'
                    WHEN EXISTS (SELECT 1 FROM campaigns WHERE campaign_id IS NULL) THEN 'Null campaign_id found'
                    ELSE 'All checks passed'
                END AS quality_check_result;
        """
    )
    
    # Task to run transformations
    run_transformations = PostgresOperator(
        task_id='run_transformations',
        postgres_conn_id='postgres_customer360',
        sql='transformations.sql'
    )
    
    # Set up task dependencies
    file_sensors['dim_date.csv'] >> load_dim_date
    file_sensors['product_costs.csv'] >> load_product_costs
    file_sensors['customers.csv'] >> load_customers
    file_sensors['orders.csv'] >> load_orders
    file_sensors['campaigns.csv'] >> load_campaigns
    file_sensors['campaign_costs.csv'] >> load_campaign_costs
    
    [load_dim_date, load_product_costs] >> load_customers >> load_orders
    load_customers >> load_campaigns >> load_campaign_costs
    
    [load_orders, load_campaign_costs] >> data_quality_checks >> run_transformations


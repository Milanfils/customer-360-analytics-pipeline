from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os

# Default DAG arguments
default_args = {
    'owner': 'yourname',
    'start_date': datetime(2023, 1, 1),  # any static date in the past
}

# Define file paths for your CSV data
# Define file paths for your CSV data - using Docker mounted paths
CUSTOMERS_CSV = '/opt/data/crm/customers.csv'
ORDERS_CSV = '/opt/data/transactions/orders.csv'
CAMPAIGNS_CSV = '/opt/data/marketing/campaigns.csv'

def load_csv_to_postgres(table_name, csv_file):
    """
    Enhanced function to copy a CSV file into a Postgres table using PostgresHook.
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_customer360')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Truncate with CASCADE
        truncate_query = f"TRUNCATE {table_name} RESTART IDENTITY CASCADE;"
        cursor.execute(truncate_query)

        # Copy the CSV contents into the table
        with open(csv_file, 'r') as f:
            next(f)  # skip header
            cursor.copy_expert(f"COPY {table_name} FROM STDIN CSV", f)
        
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error processing {table_name}: {str(e)}")
        raise

def ingest_customers():
    load_csv_to_postgres('customers', CUSTOMERS_CSV)

def ingest_orders():
    load_csv_to_postgres('orders', ORDERS_CSV)

def ingest_campaigns():
    load_csv_to_postgres('campaigns', CAMPAIGNS_CSV)

# Define the DAG
with DAG(
    dag_id='csv_to_postgres_dag',
    default_args=default_args,
    schedule_interval=None,  # no scheduled runs; run on-demand
    catchup=False
) as dag:

    task_ingest_customers = PythonOperator(
        task_id='ingest_customers',
        python_callable=ingest_customers
    )

    task_ingest_orders = PythonOperator(
        task_id='ingest_orders',
        python_callable=ingest_orders
    )

    task_ingest_campaigns = PythonOperator(
        task_id='ingest_campaigns',
        python_callable=ingest_campaigns
    )

    # Define transformation task
    run_transformations = PostgresOperator(
        task_id='run_transformations',
        postgres_conn_id='postgres_customer360',
        sql='transformations.sql',  # The path to your .sql file
        autocommit=True
    )

    # Define the sequence: customers -> orders -> campaigns -> transformations
    task_ingest_customers >> task_ingest_orders >> task_ingest_campaigns >> run_transformations

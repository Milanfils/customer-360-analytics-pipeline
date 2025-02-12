from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
import os

# Default DAG arguments
default_args = {
    'owner': 'yourname',
    'start_date': datetime(2023, 1, 1),  # any static date in the past
}

# Update these credentials/params if your Postgres is set differently
POSTGRES_HOST = 'localhost'
POSTGRES_PORT = 5432
POSTGRES_DB = 'customer360'
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'postgres'

# Define file paths for your CSV data
# Adjust the path if your data folder is in a different location
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
CUSTOMERS_CSV = os.path.join(BASE_DIR, 'data', 'crm', 'customers.csv')
ORDERS_CSV = os.path.join(BASE_DIR, 'data', 'transactions', 'orders.csv')
CAMPAIGNS_CSV = os.path.join(BASE_DIR, 'data', 'marketing', 'campaigns.csv')

def load_csv_to_postgres(table_name, csv_file):
    """
    Enhanced function to copy a CSV file into a Postgres table using psycopg2.
    """
    try:
        with psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        ) as conn:
            with conn.cursor() as cursor:
                # Truncate with CASCADE
                truncate_query = f"TRUNCATE {table_name} RESTART IDENTITY CASCADE;"
                cursor.execute(truncate_query)

                # Copy the CSV contents into the table
                with open(csv_file, 'r') as f:
                    next(f)  # skip header
                    cursor.copy_expert(f"COPY {table_name} FROM STDIN CSV", f)
                
                conn.commit()
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

    # Define the sequence: customers -> orders -> campaigns
    task_ingest_customers >> task_ingest_orders >> task_ingest_campaigns

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
import pandas as pd
import logging

# Setup logging
logger = logging.getLogger(__name__)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
}

# SQL statement to create tables with proper constraints and indexes
CREATE_TABLES_SQL = """
-- Create dim_date table with date_id as primary key and indexes on common query fields
DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    week INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);
CREATE INDEX idx_dim_date_key_full_date ON dim_date(full_date);
CREATE INDEX idx_dim_date_key_year_month ON dim_date(year, month);

-- Create product_costs table with product_category as primary key
DROP TABLE IF EXISTS product_costs;
CREATE TABLE product_costs (
    product_category VARCHAR(100) PRIMARY KEY,
    average_cogs DECIMAL(10,2) NOT NULL
);

-- Create customers table with customer_id as primary key and email uniqueness
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    signup_date DATE NOT NULL,
    region VARCHAR(100),
    CONSTRAINT uq_customer_email UNIQUE (email)
);
CREATE INDEX idx_customer_signup_date ON customers(signup_date);

-- Create orders table with order_id as primary key and foreign key constraints
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    amount DECIMAL(10,2) NOT NULL CHECK (amount >= 0),
    product_category VARCHAR(100) NOT NULL,
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    CONSTRAINT fk_orders_product FOREIGN KEY (product_category) REFERENCES product_costs(product_category)
);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_product_category ON orders(product_category);

-- Create campaigns table with campaign_id as primary key
DROP TABLE IF EXISTS campaigns;
CREATE TABLE campaigns (
    campaign_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    channel VARCHAR(100) NOT NULL,
    sent_date DATE NOT NULL,
    response BOOLEAN,
    CONSTRAINT fk_campaigns_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
CREATE INDEX idx_campaigns_customer_id ON campaigns(customer_id);
CREATE INDEX idx_campaigns_sent_date ON campaigns(sent_date);

-- Create campaign_costs table with campaign_id as primary key
DROP TABLE IF EXISTS campaign_costs;
CREATE TABLE campaign_costs (
    campaign_id INTEGER PRIMARY KEY,
    campaign_cost DECIMAL(10,2) NOT NULL CHECK (campaign_cost >= 0),
    CONSTRAINT fk_campaign_costs_campaign FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id)
);
"""

# Function to create tables with proper constraints
def create_tables(postgres_conn_id, **context):
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    logger.info("Creating tables with proper constraints and indexes")
    pg_hook.run(CREATE_TABLES_SQL)
    logger.info("Tables created successfully with constraints and indexes")

# Function to load CSV file into PostgreSQL
def load_csv_to_postgres(csv_path, table_name, postgres_conn_id, **context):
    logger.info(f"Loading data from {csv_path} into {table_name} using connection {postgres_conn_id}")
    try:
        # Read CSV file
        df = pd.read_csv(csv_path)
        logger.info(f"Successfully read {len(df)} rows from {csv_path}")
        
        # Get Postgres connection
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        engine = pg_hook.get_sqlalchemy_engine()
        
        # Load data into PostgreSQL
        logger.info(f"Loading {len(df)} rows into {table_name}")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logger.info(f"Successfully loaded {len(df)} rows")
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
    
    # Tasks for airflow database
    load_dim_date_airflow = PythonOperator(
        task_id='load_dim_date_airflow',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_path': '/opt/airflow/data/raw/dim_date.csv',
            'table_name': 'dim_date',
            'postgres_conn_id': 'postgres_default'
        }
    )

    load_product_costs_airflow = PythonOperator(
        task_id='load_product_costs_airflow',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_path': '/opt/airflow/data/raw/product_costs.csv',
            'table_name': 'product_costs',
            'postgres_conn_id': 'postgres_default'
        }
    )

    load_customers_airflow = PythonOperator(
        task_id='load_customers_airflow',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_path': '/opt/airflow/data/raw/customers.csv',
            'table_name': 'customers',
            'postgres_conn_id': 'postgres_default'
        }
    )

    load_orders_airflow = PythonOperator(
        task_id='load_orders_airflow',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_path': '/opt/airflow/data/raw/orders.csv',
            'table_name': 'orders',
            'postgres_conn_id': 'postgres_default'
        }
    )

    load_campaigns_airflow = PythonOperator(
        task_id='load_campaigns_airflow',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_path': '/opt/airflow/data/raw/campaigns.csv',
            'table_name': 'campaigns',
            'postgres_conn_id': 'postgres_default'
        }
    )

    load_campaign_costs_airflow = PythonOperator(
        task_id='load_campaign_costs_airflow',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_path': '/opt/airflow/data/raw/campaign_costs.csv',
            'table_name': 'campaign_costs',
            'postgres_conn_id': 'postgres_default'
        }
    )

    # Checkpoint between airflow and customer360 databases
    airflow_tasks_completed = EmptyOperator(
        task_id='airflow_tasks_completed'
    )

    # Tasks for customer360 database
    load_dim_date_customer360 = PythonOperator(
        task_id='load_dim_date_customer360',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_path': '/opt/airflow/data/raw/dim_date.csv',
            'table_name': 'dim_date',
            'postgres_conn_id': 'postgres_customer360'
        }
    )

    load_product_costs_customer360 = PythonOperator(
        task_id='load_product_costs_customer360',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_path': '/opt/airflow/data/raw/product_costs.csv',
            'table_name': 'product_costs',
            'postgres_conn_id': 'postgres_customer360'
        }
    )

    load_customers_customer360 = PythonOperator(
        task_id='load_customers_customer360',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_path': '/opt/airflow/data/raw/customers.csv',
            'table_name': 'customers',
            'postgres_conn_id': 'postgres_customer360'
        }
    )

    load_orders_customer360 = PythonOperator(
        task_id='load_orders_customer360',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_path': '/opt/airflow/data/raw/orders.csv',
            'table_name': 'orders',
            'postgres_conn_id': 'postgres_customer360'
        }
    )

    load_campaigns_customer360 = PythonOperator(
        task_id='load_campaigns_customer360',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_path': '/opt/airflow/data/raw/campaigns.csv',
            'table_name': 'campaigns',
            'postgres_conn_id': 'postgres_customer360'
        }
    )

    load_campaign_costs_customer360 = PythonOperator(
        task_id='load_campaign_costs_customer360',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_path': '/opt/airflow/data/raw/campaign_costs.csv',
            'table_name': 'campaign_costs',
            'postgres_conn_id': 'postgres_customer360'
        }
    )
    
    # Task to run data quality checks
    data_quality_checks = PostgresOperator(
        task_id='run_data_quality_checks',
        postgres_conn_id='postgres_customer360',
        sql="""
        WITH quality_checks AS (
            -- Primary Key Uniqueness Checks
            SELECT 'Primary Key violations in customers' as check_name 
            WHERE EXISTS (SELECT customer_id FROM customers GROUP BY customer_id HAVING COUNT(*) > 1)
            UNION ALL
            SELECT 'Primary Key violations in orders' 
            WHERE EXISTS (SELECT order_id FROM orders GROUP BY order_id HAVING COUNT(*) > 1)
            UNION ALL
            SELECT 'Primary Key violations in campaigns'
            WHERE EXISTS (SELECT campaign_id FROM campaigns GROUP BY campaign_id HAVING COUNT(*) > 1)
            UNION ALL
            -- Foreign Key Relationship Checks
            SELECT 'Invalid customer_id in orders'
            WHERE EXISTS (SELECT 1 FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id WHERE c.customer_id IS NULL)
            UNION ALL
            SELECT 'Invalid product_category in orders'
            WHERE EXISTS (SELECT 1 FROM orders o LEFT JOIN product_costs p ON o.product_category = p.product_category WHERE p.product_category IS NULL)
            UNION ALL
            SELECT 'Invalid campaign_id in campaign_costs'
            WHERE EXISTS (SELECT 1 FROM campaign_costs cc LEFT JOIN campaigns c ON cc.campaign_id = c.campaign_id WHERE c.campaign_id IS NULL)
            UNION ALL
            -- Data Type and Range Validation
            SELECT 'Invalid dates in orders'
            WHERE EXISTS (SELECT 1 FROM orders WHERE CAST(order_date AS date) > CURRENT_DATE)
            UNION ALL
            SELECT 'Invalid negative amounts in orders'
            WHERE EXISTS (SELECT 1 FROM orders WHERE amount < 0)
            UNION ALL
            -- Basic Data Quality Rules
            SELECT 'Missing customer details'
            WHERE EXISTS (SELECT 1 FROM customers WHERE first_name IS NULL OR last_name IS NULL OR email IS NULL)
            UNION ALL
            SELECT 'Missing order details'
            WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id IS NULL OR order_date IS NULL OR product_category IS NULL OR amount IS NULL)
            UNION ALL
            SELECT 'Invalid product category format'
            WHERE EXISTS (
                SELECT 1 FROM orders o 
                WHERE product_category = '' 
                OR length(product_category) > 100 
                OR product_category NOT IN (SELECT product_category FROM product_costs)
            )
        )
        SELECT 
            CASE 
                WHEN EXISTS (SELECT 1 FROM quality_checks) 
                THEN array_agg(check_name)::text
                ELSE 'All quality checks passed'
            END as quality_check_results
        FROM quality_checks;
        """
    )
    
    # Task to run transformations
    run_transformations = PostgresOperator(
        task_id='run_transformations',
        postgres_conn_id='postgres_customer360',
        sql='transformations.sql'
    )
    
    # Set up task dependencies for airflow database
    file_sensors['dim_date.csv'] >> load_dim_date_airflow
    file_sensors['product_costs.csv'] >> load_product_costs_airflow
    file_sensors['customers.csv'] >> load_customers_airflow
    file_sensors['orders.csv'] >> load_orders_airflow
    file_sensors['campaigns.csv'] >> load_campaigns_airflow
    file_sensors['campaign_costs.csv'] >> load_campaign_costs_airflow

    # Dependencies for airflow database tasks
    [load_dim_date_airflow, load_product_costs_airflow] >> load_customers_airflow >> load_orders_airflow
    load_customers_airflow >> load_campaigns_airflow >> load_campaign_costs_airflow

    # All airflow tasks must complete before moving to customer360 tasks
    [load_orders_airflow, load_campaign_costs_airflow] >> airflow_tasks_completed

    # Set up task dependencies for customer360 database
    airflow_tasks_completed >> [load_dim_date_customer360, load_product_costs_customer360]
    [load_dim_date_customer360, load_product_costs_customer360] >> load_customers_customer360 >> load_orders_customer360
    load_customers_customer360 >> load_campaigns_customer360 >> load_campaign_costs_customer360

    # Final data quality checks and transformations (on customer360 database)
    [load_orders_customer360, load_campaign_costs_customer360] >> data_quality_checks >> run_transformations

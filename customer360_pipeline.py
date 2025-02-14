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
    date_id INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    week INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);
CREATE INDEX idx_dim_date_full_date ON dim_date(full_date);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month);

-- Create product_costs table with product_id as primary key
DROP TABLE IF EXISTS product_costs;
CREATE TABLE product_costs (
    product_id INTEGER PRIMARY KEY,
    product_category VARCHAR(100) NOT NULL,
    average_cost DECIMAL(10,2) NOT NULL,
    CONSTRAINT uq_product_category UNIQUE (product_category)
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
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    total_amount DECIMAL(10,2) NOT NULL CHECK (total_amount >= 0),
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    CONSTRAINT fk_orders_product FOREIGN KEY (product_id) REFERENCES product_costs(product_id)
);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_product_id ON orders(product_id);

-- Create campaigns table with campaign_id as primary key
DROP TABLE IF EXISTS campaigns;
CREATE TABLE campaigns (
    campaign_id INTEGER PRIMARY KEY,
    campaign_name VARCHAR(255) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE,
    channel VARCHAR(100) NOT NULL,
    budget DECIMAL(10,2) NOT NULL CHECK (budget >= 0),
    CONSTRAINT uq_campaign_name UNIQUE (campaign_name),
    CONSTRAINT check_campaign_dates CHECK (end_date IS NULL OR end_date >= start_date)
);
CREATE INDEX idx_campaigns_dates ON campaigns(start_date, end_date);

-- Create campaign_costs table with composite primary key and foreign key constraints
DROP TABLE IF EXISTS campaign_costs;
CREATE TABLE campaign_costs (
    campaign_id INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    daily_cost DECIMAL(10,2) NOT NULL CHECK (daily_cost >= 0),
    impressions INTEGER NOT NULL CHECK (impressions >= 0),
    clicks INTEGER NOT NULL CHECK (clicks >= 0),
    PRIMARY KEY (campaign_id, date_id),
    CONSTRAINT fk_campaign_costs_campaign FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id),
    CONSTRAINT fk_campaign_costs_date FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);
CREATE INDEX idx_campaign_costs_date_id ON campaign_costs(date_id);
""";

# Function to create tables with proper constraints
def create_tables(**context):
    pg_hook = PostgresHook(postgres_conn_id='postgres_customer360')
    logger.info("Creating tables with proper constraints and indexes")
    pg_hook.run(CREATE_TABLES_SQL)
    logger.info("Tables created successfully with constraints and indexes")

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
        logger.info(f"Loading {len(df)} rows into {table_name}")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logger.info(f"Successfully loaded {len(df)} rows")
        logger.info(f"Successfully loaded {len(df)} rows into {table_name}")
        
    except Exception as e:
        logger.error(f"Error loading {csv_path} into {table_name}: {str(e)}")
        raise

def inspect_tables(**context):
    pg_hook = PostgresHook(postgres_conn_id='postgres_customer360')
    
    # Check orders table structure
    table_info = pg_hook.get_records("""
        SELECT column_name, data_type, character_maximum_length 
        FROM information_schema.columns 
        WHERE table_name = 'orders';
    """)
    
    logger.info("Orders Table Structure:")
    for col in table_info:
        logger.info(f"{col[0]}: {col[1]}")
    
    # Sample data from orders
    sample_data = pg_hook.get_records("SELECT * FROM orders LIMIT 5;")
    logger.info("\nSample Data from Orders:")
    for row in sample_data:
        logger.info(str(row))
    
    # Check for any invalid dates
    invalid_dates = pg_hook.get_records("""
        SELECT order_date, COUNT(*) 
        FROM orders 
        WHERE order_date > CURRENT_DATE 
        GROUP BY order_date;
    """)
    
    if invalid_dates:
        logger.warning("Found invalid dates in orders table:")
        for date_info in invalid_dates:
            logger.warning(f"Date: {date_info[0]}, Count: {date_info[1]}")

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
            SELECT 'Invalid product_id in orders'
            WHERE EXISTS (SELECT 1 FROM orders o LEFT JOIN product_costs p ON o.product_id = p.product_id WHERE p.product_id IS NULL)
            UNION ALL
            SELECT 'Invalid campaign_id in campaign_costs'
            WHERE EXISTS (SELECT 1 FROM campaign_costs cc LEFT JOIN campaigns c ON cc.campaign_id = c.campaign_id WHERE c.campaign_id IS NULL)
            UNION ALL
            -- Data Type and Range Validation
            SELECT 'Invalid dates in orders'
            WHERE EXISTS (SELECT 1 FROM orders WHERE CAST(order_date AS date) > CURRENT_DATE::date)
            UNION ALL
            SELECT 'Invalid negative amounts in orders'
            WHERE EXISTS (SELECT 1 FROM orders WHERE total_amount < 0 OR quantity < 0)
            UNION ALL
            SELECT 'Invalid campaign dates'
            WHERE EXISTS (SELECT 1 FROM campaigns WHERE CAST(end_date AS date) < CAST(start_date AS date))
            UNION ALL
            -- Basic Data Quality Rules
            SELECT 'Missing customer details'
            WHERE EXISTS (SELECT 1 FROM customers WHERE first_name IS NULL OR last_name IS NULL OR email IS NULL)
            UNION ALL
            SELECT 'Missing order details'
            WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id IS NULL OR order_date IS NULL OR product_id IS NULL)
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
    
    # Set up task dependencies
    file_sensors['dim_date.csv'] >> load_dim_date
    file_sensors['product_costs.csv'] >> load_product_costs
    file_sensors['customers.csv'] >> load_customers
    file_sensors['orders.csv'] >> load_orders
    file_sensors['campaigns.csv'] >> load_campaigns
    file_sensors['campaign_costs.csv'] >> load_campaign_costs
    
    [load_dim_date, load_product_costs] >> load_customers >> load_orders
    load_customers >> load_campaigns >> load_campaign_costs
        
        # Task to inspect tables
        inspect_tables_task = PythonOperator(
            task_id='inspect_tables',
            python_callable=inspect_tables,
            dag=dag
        )
        
        # Update dependencies
        load_orders >> inspect_tables_task >> data_quality_checks
        
        [inspect_tables_task, load_campaign_costs] >> data_quality_checks >> run_transformations
    "

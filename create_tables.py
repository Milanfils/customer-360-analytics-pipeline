from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_product_costs_table():
    try:
        # Initialize PostgresHook
        pg_hook = PostgresHook(postgres_conn_id='postgres_customer360')
        
        # SQL for creating product_costs table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS product_costs (
            product_category VARCHAR(100) PRIMARY KEY,
            average_cogs DECIMAL(10,2) NOT NULL
        );
        """
        
        # Execute the SQL
        pg_hook.run(create_table_sql)
        logger.info("Successfully created product_costs table")
        
    except Exception as e:
        logger.error(f"Error creating product_costs table: {str(e)}")
        raise

if __name__ == "__main__":
    create_product_costs_table()


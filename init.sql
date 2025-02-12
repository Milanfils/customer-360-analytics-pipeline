SELECT 'CREATE DATABASE customer360'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'customer360')\gexec

\c customer360

CREATE SCHEMA IF NOT EXISTS customer360;

-- Create customers table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    signup_date DATE,
    region VARCHAR(20)
);

-- Create orders table
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    amount DECIMAL(10,2),
    product_category VARCHAR(50),
    FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
);

-- Create campaigns table
CREATE TABLE campaigns (
    campaign_id INT PRIMARY KEY,
    customer_id INT,
    channel VARCHAR(50),
    sent_date DATE,
    response INT,
    FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA customer360 TO airflow;
GRANT ALL PRIVILEGES ON DATABASE customer360 TO airflow;
ALTER DATABASE customer360 OWNER TO airflow;

-- Initialize PostgreSQL connection settings for optimal performance
SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = ON;
SET check_function_bodies = FALSE;
SET client_min_messages = WARNING;

-- Create and connect to customer360 database if it doesn't exist
SELECT 'CREATE DATABASE customer360'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'customer360')\gexec

\c customer360

-- Create schema for better organization
CREATE SCHEMA IF NOT EXISTS customer360;
SET search_path TO customer360;

-- Create dimension tables first
-- Create date dimension table for time-based analysis
CREATE TABLE dim_date (
    date_key DATE PRIMARY KEY,
    year INT NOT NULL,
    quarter INT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month INT NOT NULL CHECK (month BETWEEN 1 AND 12),
    day_of_month INT NOT NULL CHECK (day_of_month BETWEEN 1 AND 31),
    day_of_week INT NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    day_name VARCHAR(10) NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter_name VARCHAR(2) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

COMMENT ON TABLE dim_date IS 'Date dimension table for time-based analysis';

-- Create base tables
-- Customers table - contains core customer information
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    signup_date DATE NOT NULL REFERENCES dim_date(date_key),
    region VARCHAR(20) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE customers IS 'Core customer information and demographics';

-- Product costs table - contains cost information for each product category
CREATE TABLE product_costs (
    product_category VARCHAR(50) PRIMARY KEY,
    average_cogs DECIMAL(10,2) NOT NULL CHECK (average_cogs >= 0),
    last_updated_date DATE NOT NULL REFERENCES dim_date(date_key),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE product_costs IS 'Cost of goods sold by product category';

-- Orders table - contains transaction history
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    order_date DATE NOT NULL REFERENCES dim_date(date_key),
    amount DECIMAL(10,2) NOT NULL CHECK (amount >= 0),
    product_category VARCHAR(50) NOT NULL REFERENCES product_costs(product_category) ON DELETE RESTRICT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE orders IS 'Customer order transactions and details';

-- Marketing campaign tables
-- Campaigns table - contains campaign details
CREATE TABLE campaigns (
    campaign_id INT PRIMARY KEY,
    campaign_name VARCHAR(100) NOT NULL,
    channel VARCHAR(50) NOT NULL,
    start_date DATE NOT NULL REFERENCES dim_date(date_key),
    end_date DATE NOT NULL REFERENCES dim_date(date_key),
    budget DECIMAL(10,2) NOT NULL CHECK (budget >= 0),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_campaign_dates CHECK (end_date >= start_date)
);

COMMENT ON TABLE campaigns IS 'Marketing campaign details and metadata';

-- Campaign responses table - contains customer responses to campaigns
CREATE TABLE campaign_responses (
    response_id INT PRIMARY KEY,
    campaign_id INT NOT NULL REFERENCES campaigns(campaign_id) ON DELETE CASCADE,
    customer_id INT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    sent_date DATE NOT NULL REFERENCES dim_date(date_key),
    response_date DATE REFERENCES dim_date(date_key),
    response_type VARCHAR(20) NOT NULL CHECK (response_type IN ('opened', 'clicked', 'converted', 'unsubscribed')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_response_dates CHECK (response_date >= sent_date)
);

COMMENT ON TABLE campaign_responses IS 'Customer responses to marketing campaigns';

-- Campaign costs table - contains daily cost information for each campaign
CREATE TABLE campaign_costs (
    cost_id INT PRIMARY KEY,
    campaign_id INT NOT NULL REFERENCES campaigns(campaign_id) ON DELETE CASCADE,
    cost_date DATE NOT NULL REFERENCES dim_date(date_key),
    daily_spend DECIMAL(10,2) NOT NULL CHECK (daily_spend >= 0),
    impressions INT NOT NULL CHECK (impressions >= 0),
    clicks INT NOT NULL CHECK (clicks >= 0),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (campaign_id, cost_date)
);

COMMENT ON TABLE campaign_costs IS 'Daily campaign spend and performance metrics';

-- Create indexes for better query performance
CREATE INDEX idx_customers_region ON customers(region);
CREATE INDEX idx_customers_signup_date ON customers(signup_date);
CREATE INDEX idx_customers_email ON customers(email);

CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_product ON orders(product_category);

CREATE INDEX idx_campaigns_dates ON campaigns(start_date, end_date);
CREATE INDEX idx_campaigns_channel ON campaigns(channel);

CREATE INDEX idx_campaign_responses_dates ON campaign_responses(sent_date, response_date);
CREATE INDEX idx_campaign_responses_campaign ON campaign_responses(campaign_id);
CREATE INDEX idx_campaign_responses_customer ON campaign_responses(customer_id);

CREATE INDEX idx_campaign_costs_date ON campaign_costs(cost_date);
CREATE INDEX idx_campaign_costs_campaign ON campaign_costs(campaign_id);

CREATE INDEX idx_dim_date_components ON dim_date(year, month, quarter);
CREATE INDEX idx_dim_date_month_name ON dim_date(month_name);

-- Create composite indexes for common joins
CREATE INDEX idx_orders_customer_date ON orders(customer_id, order_date);
CREATE INDEX idx_campaign_responses_campaign_date ON campaign_responses(campaign_id, sent_date);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA customer360 TO airflow;
GRANT ALL PRIVILEGES ON DATABASE customer360 TO airflow;
ALTER DATABASE customer360 OWNER TO airflow;

-- Additional schema settings
ALTER SCHEMA customer360 OWNER TO airflow;
SET search_path TO customer360;

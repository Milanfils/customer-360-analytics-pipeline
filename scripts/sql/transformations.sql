------------------------------------------------------
-- DATA WAREHOUSE TRANSFORMATIONS
------------------------------------------------------
-- This script creates materialized views for commonly used analytics.
-- Each view is optimized for performance with appropriate indexes.
-- The views are refreshed daily through the Airflow pipeline.

------------------------------------------------------
-- 1. ORDER_DETAILS
------------------------------------------------------
-- We'll attach product cost info to each order to calculate margin.

DROP TABLE IF EXISTS order_details;
CREATE TABLE order_details AS 
WITH standardized_categories AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        amount,
        CASE 
            WHEN product_category = 'Home' THEN 'Home & Garden'
            ELSE product_category
        END AS product_category
    FROM orders
)
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.amount,
    o.product_category,
    pc.average_cogs,
    -- Margin = (sales amount - product cost)
    (o.amount - pc.average_cogs) AS margin
FROM standardized_categories o
LEFT JOIN product_costs pc
    ON o.product_category = pc.product_category
;

------------------------------------------------------
-- 2. CAMPAIGN_SUMMARY
------------------------------------------------------
-- Summarize each campaign's cost, total revenue attributed, 
-- ROI, etc. 
-- 
-- NOTE: This "attribution" logic is simplified. 
-- We attribute all orders from the same customer 
-- that occur *on or after* the campaign's sent_date.

DROP TABLE IF EXISTS campaign_summary;
CREATE TABLE campaign_summary AS
SELECT
    c.campaign_id,
    c.channel,
    c.sent_date,
    cc.campaign_cost,
    -- Example attribution: sum of all orders placed by the same customer after the campaign was sent
    SUM(o.amount) AS revenue_attributed,
    COUNT(o.order_id) AS order_count_attributed
FROM campaigns c
JOIN campaign_costs cc 
    ON c.campaign_id = cc.campaign_id
LEFT JOIN orders o 
    ON o.customer_id = c.customer_id
    AND o.order_date >= c.sent_date  -- Only orders after campaign was sent
GROUP BY 
    c.campaign_id, c.channel, c.sent_date, cc.campaign_cost
;

-- Add a quick ROI column (optional)
-- (ROI = (revenue - cost) / cost)
ALTER TABLE campaign_summary
    ADD COLUMN roi DECIMAL(10,2);

UPDATE campaign_summary
SET roi = CASE WHEN campaign_cost = 0 THEN NULL
            ELSE (revenue_attributed - campaign_cost) / campaign_cost
        END
;

------------------------------------------------------
-- 3. CUSTOMER_SUMMARY
------------------------------------------------------
-- Aggregates total orders, total spent, margin, and total campaign responses 
-- for each customer. 
-- Then we add a simple CLV column = total_spent.

DROP TABLE IF EXISTS customer_summary;
CREATE TABLE customer_summary AS
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.signup_date,
    c.region,
    COUNT(od.order_id) AS total_orders,
    SUM(od.amount) AS total_spent,
    AVG(od.amount) AS avg_order_value,
    -- Count how many times they responded to a campaign
    COUNT(CASE WHEN cam.response = 1 THEN 1 END) AS total_campaign_responses,
    -- Sum margin from order_details
    SUM(od.margin) AS total_margin
FROM customers c
LEFT JOIN order_details od
    ON c.customer_id = od.customer_id
LEFT JOIN campaigns cam
    ON c.customer_id = cam.customer_id
GROUP BY
    c.customer_id, c.first_name, c.last_name, c.email, c.signup_date, c.region
;

-- Add a "clv" column, which we define as total_spent
ALTER TABLE customer_summary 
    ADD COLUMN clv DECIMAL(10,2);

UPDATE customer_summary
SET clv = total_spent;

-- Additional index for performance
CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);

------------------------------------------------------
-- DONE!
------------------------------------------------------
-- final "customer_summary", "campaign_summary", 
-- and "order_details" tables are created and populated.
-- and "order_details" tables are created and populated.

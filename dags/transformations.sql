-- 1. Drop & recreate a summary table
DROP TABLE IF EXISTS customer_summary;
CREATE TABLE customer_summary AS
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.region,
    COUNT(o.order_id) AS total_orders,
    SUM(o.amount) AS total_spent,
    AVG(o.amount) AS avg_order_value,
    SUM(
    CASE WHEN cam.response = 1 THEN 1 ELSE 0 END
    ) AS total_campaign_responses
FROM customers c
LEFT JOIN orders o
ON c.customer_id = o.customer_id
LEFT JOIN campaigns cam
ON c.customer_id = cam.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.region;

-- 2. Optional: create a star schema or additional tables
-- Example: Fact table for Orders
-- This snippet is just an example of how you might do it:

/*
DROP TABLE IF EXISTS fact_orders;
CREATE TABLE fact_orders AS
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.amount,
    o.product_category
FROM orders o
;
*/


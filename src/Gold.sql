-- =============================================================
-- GOLD LAYER - Materialized Views
-- Medallion Architecture | Databricks Asset Bundles
-- =============================================================
-- Silver Tables used:
--   silver.silver_orders       : order_id, customer_id, order_status, order_purchase_timestamp
--   silver.silver_order_items  : order_id, product_id, price, freight_value
--   silver.silver_products     : product_id, product_category_name
--   silver.silver_customers    : customer_id, customer_unique_id, customer_city, customer_state


-- =============================================================
-- 1. gold_sales_summary
--    Metrics: Total Revenue, Daily Sales Trend, Monthly Sales Trend
-- =============================================================
CREATE OR REPLACE VIEW az_adb_simbus_training.retail.gold_sales_summary
COMMENT 'Aggregated sales metrics: total revenue, daily and monthly trends'
AS
SELECT
    -- Date dimensions
    CAST(o.order_purchase_timestamp AS DATE)                        AS order_date,
    DATE_FORMAT(o.order_purchase_timestamp, 'yyyy-MM')              AS order_month,
    YEAR(o.order_purchase_timestamp)                                AS order_year,

    -- Revenue components
    ROUND(SUM(oi.price), 2)                                         AS total_revenue,
    ROUND(SUM(oi.freight_value), 2)                                 AS total_freight,
    ROUND(SUM(oi.price + oi.freight_value), 2)                      AS gross_revenue,

    -- Order volume
    COUNT(DISTINCT o.order_id)                                      AS total_orders,
    COUNT(oi.order_item_id)                                         AS total_items_sold,

    -- Average order value (revenue only, excluding freight)
    ROUND(SUM(oi.price) / NULLIF(COUNT(DISTINCT o.order_id), 0), 2) AS avg_order_value

FROM az_adb_simbus_training.retail.silver_orders o
INNER JOIN az_adb_simbus_training.retail.silver_order_items oi
    ON o.order_id = oi.order_id
WHERE
    o.order_status NOT IN ('canceled', 'unavailable')
    AND o.order_purchase_timestamp IS NOT NULL
GROUP BY
    CAST(o.order_purchase_timestamp AS DATE),
    DATE_FORMAT(o.order_purchase_timestamp, 'yyyy-MM'),
    YEAR(o.order_purchase_timestamp)
ORDER BY
    order_date;


-- =============================================================
-- 2. gold_product_performance
--    Metrics: Top Selling Products, Revenue by Product & Category
-- =============================================================
CREATE OR REPLACE  VIEW az_adb_simbus_training.retail.gold_product_performance
COMMENT 'Product-level performance: units sold, revenue, and category breakdown'
AS
SELECT
    p.product_id,
    p.product_category_name,

    -- Sales volume
    COUNT(oi.order_item_id)                                              AS total_units_sold,
    COUNT(DISTINCT oi.order_id)                                          AS total_orders,

    -- Revenue metrics
    ROUND(SUM(oi.price), 2)                                              AS total_revenue,
    ROUND(SUM(oi.freight_value), 2)                                      AS total_freight_revenue,
    ROUND(SUM(oi.price + oi.freight_value), 2)                           AS gross_revenue,

    -- Average price per unit
    ROUND(AVG(oi.price), 2)                                              AS avg_unit_price,

    -- Revenue rank within category
    RANK() OVER (
        PARTITION BY p.product_category_name
        ORDER BY SUM(oi.price) DESC
    )                                                                    AS revenue_rank_in_category,

    -- Overall revenue rank
    RANK() OVER (
        ORDER BY SUM(oi.price) DESC
    )                                                                    AS overall_revenue_rank

FROM az_adb_simbus_training.retail.silver_order_items oi
INNER JOIN az_adb_simbus_training.retail.silver_products p
    ON oi.product_id = p.product_id
INNER JOIN az_adb_simbus_training.retail.silver_orders o
    ON oi.order_id = o.order_id
WHERE
    o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY
    p.product_id,
    p.product_category_name
ORDER BY
    total_revenue DESC;


-- =============================================================
-- 3. gold_repeat_customers
--    Metrics: Repeat Customers, Average Order Value per Customer
-- =============================================================
CREATE OR REPLACE VIEW az_adb_simbus_training.retail.gold_repeat_customers
COMMENT 'Customer retention metrics: repeat buyers and average order values'
AS
WITH customer_orders AS (
    SELECT
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        o.order_id,
        o.order_purchase_timestamp,
        ROUND(SUM(oi.price), 2)                  AS order_revenue,
        ROUND(SUM(oi.freight_value), 2)           AS order_freight,
        ROUND(SUM(oi.price + oi.freight_value), 2) AS order_gross_value
    FROM az_adb_simbus_training.retail.silver_customers c
    INNER JOIN az_adb_simbus_training.retail.silver_orders o
        ON c.customer_id = o.customer_id
    INNER JOIN az_adb_simbus_training.retail.silver_order_items oi
        ON o.order_id = oi.order_id
    WHERE
        o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        o.order_id,
        o.order_purchase_timestamp
)
SELECT
    customer_unique_id,
    customer_city,
    customer_state,

    -- Order counts
    COUNT(DISTINCT order_id)                                                AS total_orders,

    -- Repeat customer flag (more than 1 distinct order)
    CASE
        WHEN COUNT(DISTINCT order_id) > 1 THEN TRUE
        ELSE FALSE
    END                                                                     AS is_repeat_customer,

    -- Customer segment
    CASE
        WHEN COUNT(DISTINCT order_id) = 1  THEN 'One-Time'
        WHEN COUNT(DISTINCT order_id) <= 3 THEN 'Occasional'
        ELSE 'Loyal'
    END                                                                     AS customer_segment,

    -- Revenue metrics
    ROUND(SUM(order_revenue), 2)                                            AS total_revenue,
    ROUND(SUM(order_gross_value), 2)                                        AS total_gross_value,
    ROUND(AVG(order_revenue), 2)                                            AS avg_order_value,
    ROUND(AVG(order_gross_value), 2)                                        AS avg_gross_order_value,

    -- Date of first and last purchase
    CAST(MIN(order_purchase_timestamp) AS DATE)                             AS first_order_date,
    CAST(MAX(order_purchase_timestamp) AS DATE)                             AS last_order_date,

    -- Days between first and last order (customer lifespan)
    DATEDIFF(
        CAST(MAX(order_purchase_timestamp) AS DATE),
        CAST(MIN(order_purchase_timestamp) AS DATE)
    )                                                                       AS customer_lifespan_days

FROM customer_orders
GROUP BY
    customer_unique_id,
    customer_city,
    customer_state
ORDER BY
    total_orders DESC,
    total_revenue DESC;
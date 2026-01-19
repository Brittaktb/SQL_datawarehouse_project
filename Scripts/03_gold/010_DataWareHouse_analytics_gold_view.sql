-- Explore All Objects in the database

SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'silver' OR table_schema = 'gold' OR table_schema = 'bronze';


-- Explore ALL Columns in the Database
SELECT table_schema, table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'silver' OR table_schema = 'gold' OR table_schema = 'bronze'
ORDER BY table_name, ordinal_position;

-- Explore all countries our customers come from
SELECT DISTINCT country FROM gold.dim_customers;

-- Explore all categories "The major Divisions"
SELECT DISTINCT category FROM gold.dim_products

-- Find the order date of the first and last order
SELECT MIN(order_date), MAX(order_date) FROM gold.fact_sales;


-- Find the youngest and the oldest customer
SELECT MIN(birthdate), MAX(birthdate) FROM gold.dim_customers;


-- SALES
-- find the total sales
SELECT SUM(sales_amount) FROM gold.fact_sales;
SELECT TO_CHAR(SUM(sales_amount), 'FM€ 999G999G999D00') FROM gold.fact_sales;

-- find the total number of products
SELECT COUNT(DISTINCT product_key) AS number_of_products FROM gold.fact_sales;

-- find the total number of customers
SELECT COUNT(customer_id) FROM gold.dim_customers;

-- find the total number of customers, that has placed an order
SELECT COUNT(DISTINCT customer_key) AS customers_who_bought FROM gold.fact_sales;

-- Generate a Report that shows all key metrics of the business
SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', ROUND(AVG(sls_price)) FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Products', COUNT(product_name) FROM gold.dim_products
UNION ALL
SELECT 'Total Nr. Customers', COUNT(customer_key) FROM gold.dim_customers;


-- find total customer by countries
SELECT country, COUNT(customer_key) AS total_customers FROM gold.dim_customers GROUP BY country ORDER BY total_customers DESC;
-- find total customer by gender
SELECT gender, COUNT(customer_key) FROM gold.dim_customers GROUP BY gender;
SELECT gender, COUNT(customer_key) AS total_customers FROM gold.dim_customers GROUP BY gender ORDER BY total_customers DESC;
-- find total products by category
SELECT category, COUNT(DISTINCT product_key) FROM gold.dim_products GROUP BY category;

-- What is the average costs in each category?
SELECT
category,
ROUND(AVG(cost)) AS avg_costs
FROM gold.dim_products
GROUP BY category
ORDER BY avg_costs;


-- What is the total revenue generated for each category?
SELECT 
p.category, 
SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s 
LEFT JOIN gold.dim_products p
ON s.product_key = p.product_key
GROUP BY p.category
ORDER BY total_revenue DESC
;
-- Find total revenue is by each customer

SELECT
customer_key, SUM(sales_amount) AS total_revenue_by_customer
FROM
gold.fact_sales
GROUP BY
customer_key 
ORDER BY total_revenue_by_customer DESC;


SELECT
c.customer_key,
c.first_name,
c.last_name,
SUM(s.sales_amount) AS total_revenue_by_customer
FROM
gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_key = c.customer_key
GROUP BY
c.customer_key,
c.first_name,
c.last_name
ORDER BY total_revenue_by_customer DESC;

-- What is the distribution of sold items across countries?

SELECT
c.country,
SUM(s.quantity) AS total_sold_items
FROM gold.dim_customers c
LEFT JOIN gold.fact_sales s
ON c.customer_key = s.customer_key
GROUP BY
c.country
ORDER BY
total_sold_items DESC;

-- Which  5 products generate the highest revenue?

SELECT
    p.product_key,
    p.product_name,
    SUM(s.sales_amount) AS total_sales
FROM gold.dim_products p
LEFT JOIN gold.fact_sales s
    ON p.product_key = s.product_key
GROUP BY
    p.product_key,
    p.product_name
ORDER BY
    CASE WHEN SUM(s.sales_amount) IS NULL THEN 1 ELSE 0 END,
    total_sales DESC;

-- alternativ with ROW_NUMBER() ...
WITH product_sales AS (
    SELECT
        p.product_name,
        SUM(s.sales_amount) AS total_sales
    FROM gold.dim_products p
    LEFT JOIN gold.fact_sales s
        ON p.product_key = s.product_key
    GROUP BY
        p.product_name
)
SELECT
    product_name,
    total_sales,
    ROW_NUMBER() OVER (ORDER BY total_sales DESC NULLS LAST) AS rank_products
FROM product_sales
LIMIT 5;


-- Schritt 1: Aggregation auf Fact-Tabelle zuerst
WITH sales_agg AS (
    SELECT
        product_key,
        SUM(sales_amount) AS total_sales
    FROM gold.fact_sales
    GROUP BY product_key
),
ranked_products AS (
    SELECT
        p.product_name,
        COALESCE(sa.total_sales, 0) AS total_sales,
        ROW_NUMBER() OVER (ORDER BY COALESCE(sa.total_sales, 0) DESC) AS rank_products
    FROM gold.dim_products p
    LEFT JOIN sales_agg sa
        ON p.product_key = sa.product_key
)
SELECT *
FROM ranked_products
WHERE rank_products <= 5
ORDER BY total_sales DESC;




-- What are the 5 worst-performing products in terms of sales

SELECT
    p.product_key,
    p.product_name,
    SUM(s.sales_amount) AS total_sales
FROM gold.dim_products p
LEFT JOIN gold.fact_sales s
    ON p.product_key = s.product_key
GROUP BY
    p.product_key,
    p.product_name
ORDER BY total_sales NULLS LAST
LIMIT 5;



-- Find the Top 10 customers who have generated the highest revenue

SELECT
    c.customer_key,
    c.first_name,
    SUM(sa.sales_amount) AS total_revenue
FROM gold.dim_customers c
JOIN gold.fact_sales sa
    ON c.customer_key = sa.customer_key
GROUP BY
    c.customer_key,
    c.first_name
ORDER BY total_revenue DESC
LIMIT 10;





-- Find 3 customer with the fewest orders placed

SELECT
    c.customer_key,
    c.first_name,
    SUM(sa.quantity) AS total_quantity
FROM gold.dim_customers c
JOIN gold.fact_sales sa
    ON c.customer_key = sa.customer_key
GROUP BY
    c.customer_key,
    c.first_name
ORDER BY total_quantity 
LIMIT 3;


SELECT
    c.customer_key,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT sa.order_number) AS total_order_quantity
FROM gold.dim_customers c
JOIN gold.fact_sales sa
    ON c.customer_key = sa.customer_key
GROUP BY
    c.customer_key,
    c.first_name, 
    c.last_name
ORDER BY total_order_quantity 
LIMIT 3;
-- Veränderungen im Zeitverlauf (Gesamtumsatz pro Jahr, durchschnittliche Kosten pro Monat)

--total sales by date

SELECT
order_date,
SUM(sales_amount) as total_Sales
FROM gold.fact_sales
WHERE order_date is not NULL
GROUP BY
order_date
ORDER BY order_date;

--total sales by year

SELECT
EXTRACT(YEAR FROM order_date) as sales_by_year,  --SQL: YEAR(order_date)
SUM(sales_amount) as total_Sales
FROM gold.fact_sales
WHERE order_date is not NULL
GROUP BY
EXTRACT(YEAR FROM order_date)
ORDER BY EXTRACT(YEAR FROM order_date);


-- add to the total sales of the year, the total customers to yearly revenue & the quantity

SELECT
YEAR(order_date) as sales_year,
SUM(sales_amount) as total_Sales,
COUNT(DISTINCT customer_key) as total_customers,
SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date is not NULL
GROUP BY YEAR(order_date) 
ORDER BY sales_year;


-- change the total sales to month view, the total customers to yearly revenue & the quantity

SELECT
    CAST(DATEFROMPARTS(YEAR(order_date), MONTH(order_date), 1) AS DATE) AS sales_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY CAST(DATEFROMPARTS(YEAR(order_date), MONTH(order_date), 1) AS DATE)
ORDER BY sales_month;


--postgreSQL
-- SELECT
-- EXTRACT(MONTH FROM order_date) as sales_by_month, 
-- SUM(sales_amount) as total_Sales,
-- COUNT(DISTINCT customer_key) as total_customers,
-- SUM(quantity) as total_quantity
-- FROM gold.fact_sales
-- WHERE order_date is not NULL
-- GROUP BY
-- EXTRACT(MONTH FROM order_date)
-- ORDER BY EXTRACT(MONTH FROM order_date);

-- year and month view

SELECT
    YEAR(order_date) AS sales_year,
    MONTH(order_date) AS sales_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY
    YEAR(order_date),
    MONTH(order_date)
ORDER BY
    sales_year,
    sales_month;


--postgreSQL
-- SELECT
-- EXTRACT(YEAR FROM order_date) as sales_year,
-- EXTRACT(MONTH FROM order_date) as sales_month, 
-- SUM(sales_amount) as total_Sales,
-- COUNT(DISTINCT customer_key) as total_customers,
-- SUM(quantity) as total_quantity
-- FROM gold.fact_sales
-- WHERE order_date is not NULL
-- GROUP BY
-- EXTRACT(MONTH FROM order_date), EXTRACT(YEAR FROM order_date)
-- ORDER BY EXTRACT(MONTH FROM order_date), EXTRACT(YEAR FROM order_date);


-- Sales year and month view with CAST function

SELECT
    CAST(DATEFROMPARTS(YEAR(order_date), MONTH(order_date), 1) AS DATE) AS sales_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY CAST(DATEFROMPARTS(YEAR(order_date), MONTH(order_date), 1) AS DATE)
ORDER BY sales_month;



-- Cumulative analysis by date with window function = Unterabfrage
-- calculate the total sales per month and the running total of sales over time

--*******************

SELECT
MONTH(order_date) AS order_date,
SUM(sales_amount) AS total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_date
ORDER BY order_date;


-- mit Unterabfrage

SELECT
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS cumulated_total_sales
--window FUNCTION
FROM(
    SELECT
    MONTH(order_date) AS order_date,
    SUM(sales_amount) AS total_sales
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY order_date
) t
ORDER BY order_date;


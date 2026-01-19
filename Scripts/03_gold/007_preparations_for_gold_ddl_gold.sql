/*
===============================================================================
Qualitätsprüfungen
===============================================================================
Zweck des Skripts:
    Dieses Skript führt Qualitätsprüfungen durch, um die Integrität,
    Konsistenz und Genauigkeit der Gold-Schicht zu validieren.
    Diese Prüfungen stellen sicher:

    - Eindeutigkeit der Surrogatschlüssel in Dimensionstabellen.
    - Referenzielle Integrität zwischen Fakten- und Dimensionstabellen.
    - Validierung der Beziehungen im Datenmodell für analytische Zwecke.

Hinweise zur Verwendung:
    - Untersuchen und beheben Sie alle während der Prüfungen festgestellten Abweichungen.
===============================================================================
*/



-- =============================================================================
-- Testungen und Vorbereitungen der Dimension: gold.dim_customers
-- =============================================================================

/* Verbinde alle Kundeninformationen mittels LEFT JOIN aus verschiedenen Tabellen.
   Überprüfe nach dem Zusammenführen der Tabellen, ob durch die Join-Logik
   Duplikate entstanden sind:
   Das bedeutet, prüfen, ob customer_id über COUNT(*) keine doppelten Einträge
   durch den Join mittels einer Subquery aufweist.
*/


SELECT cst_id, COUNT(*) 
FROM (
    SELECT
        ci.cst_id,               
        ci.cst_key,              
        ci.cst_firstname,       
        ci.cst_lastname,        
        ci.cst_marital_status,
        ci.cst_gndr,
        ci.cst_create_date,
        ca.bdate,
        ca.gen,
        la.cntry
    FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca
        ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 la
        ON ci.cst_key = la.cid
) AS t
GROUP BY cst_id
HAVING COUNT (*) > 1



-- integrate gndr & cst_gndr in one column, check via DISTINCT, if differences exist & clean data
-- cause of joins, we can get NULL values in SELECTs, although we have changed them before > USE COALESCE to change them
SELECT DISTINCT
    ci.cst_gndr,
    ca.gen,
    CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the Master for gender info
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender          
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid
ORDER BY 1, 2;  -- order by first column, then second



-- =============================================================================
-- Testungen und Vorbereitungen der Dimension: gold.dim_products
-- =============================================================================


-- if end_date is NULL then it is the current information of the product! Keep only the product info lines with NULL

SELECT
    pn.prd_id,
    pn.prd_key,
    pn.prd_nm,      
    pn.cat_id,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt
FROM silver.crm_prd_info pn
WHERE prd_end_dt IS NULL;  --Filter out all historical data & show product actual data with NULL



-- join the product info table with data from cat table

SELECT
    pn.prd_id,
    pn.prd_key,
    pn.prd_nm,      
    pn.cat_id,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt,
    pc.cat,
    pc.subcat,
    pc.maintenance
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL;  --Filter out all historical data & show product actual data with NULL


-- check if the product key is unique, no duplicates exists 

SELECT prd_key, COUNT(*) 
FROM (
    SELECT
        pn.prd_id,
        pn.prd_key,
        pn.prd_nm,      
        pn.cat_id,
        pc.cat,
        pc.subcat,
        pn.prd_cost,
        pn.prd_line,
        pn.prd_start_dt,
        pc.maintenance
    FROM silver.crm_prd_info pn
    LEFT JOIN silver.erp_px_cat_g1v2 pc
        ON pn.cat_id = pc.id
    WHERE prd_end_dt IS NULL  --Filter out all historical data & show product actual data with NULL
) t 
GROUP BY prd_key
HAVING COUNT(*) > 1; -- no result = no duplicates


-- =============================================================================
-- Testungen und Vorbereitungen der Sales Fakten: gold.fact_sales
-- =============================================================================

SELECT
    sd.sls_ord_num,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt,
    sd.sls_ship_dt,
    sd.sls_due_dt,
    sd.sls_sales,
    sd.sls_quantity,
    sd.sls_price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;

----

-- =============================================================================
-- Prüfe im Nachgang die erstellte Gold view: gold.fact_sales
-- =============================================================================


-- SELECT * FROM gold.fact_sales;

-- check if all dimension tables can successfully join to the fact table
--Foreign Key Integrity (Dimensions)
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_keyp
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL;
-- WHERE c.customer_key IS NULL; -- No result means everything is matching
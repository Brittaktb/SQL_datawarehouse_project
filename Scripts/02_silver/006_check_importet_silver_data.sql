/*
===============================================================================
Qualitätsprüfungen
===============================================================================
Zweck des Skripts:
    Dieses Skript führt verschiedene Qualitätsprüfungen durch, um
    Datenkonsistenz, Genauigkeit und Standardisierung innerhalb der
    **Silver-Schicht** sicherzustellen. Es umfasst unter anderem Prüfungen auf:

    - NULL- oder doppelte Primärschlüssel.
    - Unerwünschte Leerzeichen in Zeichenkettenfeldern.
    - Datenstandardisierung und -konsistenz.
    - Ungültige Datumsbereiche und falsche zeitliche Reihenfolgen.
    - Datenkonsistenz zwischen zusammenhängenden Feldern.

Hinweise zur Verwendung:
    - Führen Sie diese Prüfungen nach dem Laden der Daten in die Silver-Schicht aus.
    - Untersuchen und beheben Sie alle während der Prüfungen festgestellten Abweichungen.
===============================================================================
*/

-- ============================================================================
-- silver.crm_cust_info
-- ============================================================================

-- Check for Nulls or Duplicates in Primary Key
-- Expecation: No Result

SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 or cst_id IS NULL; 


-- check for unwanted spaces each column
-- Expectation: No Results
SELECT 
cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);


-- check for Data Standardization & Consistency
-- 1. check values in column via DISTINCT, decide than if you want to adapt some values
SELECT 
DISTINCT(cst_gndr)
FROM silver.crm_cust_info;


-- check finally the whole table
SELECT * FROM silver.crm_cust_info;


-- ============================================================================
-- silver.crm_prd_info
-- ============================================================================

-- Check for Nulls or Duplicates in Primary Key
-- Expecation: No Result

SELECT 
prd_cost,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_cost
HAVING COUNT(*) > 1 or prd_cost IS NULL; 
--HAVING prd_cost IS NULL; 


-- check for unwanted spaces each column
-- Expectation: No Results
SELECT 
cat_id
FROM silver.crm_prd_info
WHERE cat_id != TRIM(cat_id);


-- check for Data Standardization & Consistency
-- 1. check values in column via DISTINCT, decide than if you want to adapt some values
SELECT 
DISTINCT(cat_id)
FROM silver.crm_prd_info;


--Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ============================================================================
-- silver.crm_sales_details
-- ============================================================================

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT TOP (1000) *
FROM silver.crm_sales_details;


-- ============================================================================
-- silver.erp_cust_az12
-- ============================================================================

--Format cid as it doesn't match with the cst_key from crm_cust_info table
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid
END cid,
bdate,
gen
FROM silver.erp_cust_az12
-- WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
--     ELSE cid
-- END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

--WHERE cid LIKE '%AW00011000%';

--SELECT * FROM [silver].[crm_cust_info];


-- check strange birthday dates, too old or in the future

SELECT
bdate
FROM silver.erp_cust_az12
WHERE bdate <= '1924-01-01' OR bdate > GETDATE()
--issues dates in future has to set to NULL

--Data  Standardization & Consistency
SELECT DISTINCT gen
FROM silver.erp_cust_az12;


-- ============================================================================
-- silver.erp_loc_a101
-- ============================================================================

-- cid doesn't match with cst_ky from silver.crm_cust_info
SELECT TOP(100)
cid,
cntry
FROM silver.erp_loc_a101;


--check for Data Standarization & Consistency

SELECT DISTINCT cntry
FROM silver.erp_loc_a101;


--Corrections if necessary

UPDATE silver.crm_prd_info
SET cat_id = TRIM(cat_id)
WHERE cat_id <> TRIM(cat_id);


-- ============================================================================
-- silver.erp_px_cat_g1v2
-- ============================================================================


SELECT
id,
cat,
subcat,
maintenance
FROM silver.erp_px_cat_g1v2;

--check for unwanted spaces
SELECT * FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat)  OR maintenance != TRIM(maintenance);

--Data Standarization & Consistency
SELECT DISTINCT
subcat
FROM silver.erp_px_cat_g1v2;
-- no issues
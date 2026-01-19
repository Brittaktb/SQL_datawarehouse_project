-- TABLE bronze.crm_cust_info

-- Check for Nulls or Duplicates in Primary Key
-- Ensure only one record per entity by identifying and retaining the most relevant row.
-- Expecation: No Result

SELECT 
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 or cst_id IS NULL; 



-- check for unwanted spaces each column
-- Expectation: No Results
SELECT 
cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);


-- check for Data Standardization & Consistency (maps coded values to meaningful, userfriendly descriptions)
-- 1. check values in column via DISTINCT, decide than if you want to adapt some values
SELECT 
DISTINCT(cst_gndr)
FROM bronze.crm_cust_info;

---------

-- TABLE bronze.crm_prd_info

-- Check for Nulls or Duplicates in Primary Key
-- Ensure only one record per entity by identifying and retaining the most relevant row.
-- Expecation: No Result

SELECT 
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 or prd_id IS NULL; 


--clean

--reorganize the product key, as it contains two values and needs to be matched with the cat_key from erp_list

SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1,5),'-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
    WHEN 'M' THEN 'Mountain'
    WHEN 'R' THEN 'Road'
    WHEN 'S' THEN 'other Sales'
    WHEN 'T' THEN 'Touring'
    ELSE 'n/a'
END AS prd_line,
prd_start_dt,
prd_end_dt,
    DATEADD(day, -1,
        LEAD(prd_start_dt) OVER (
            PARTITION BY prd_key
            ORDER BY prd_start_dt
        )
    ) AS prd_end_dt_test
FROM bronze.crm_prd_info;


--Zwischenchecks
-- filtert/überprüft ob die prd_key ebenfalls in der sales_details list abrufbar sind
-- WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN
-- (SELECT sls_prd_key FROM bronze.crm_sales_details);


---- filtert Nicht-Matches der cat_id in der erp_px_cat_g1v2 list, zur Überprüfung
--WHERE REPLACE(SUBSTRING(prd_key, 1,5),'-', '_') NOT IN  
--(SELECT DISTINCT id from bronze.erp_px_cat_g1v2);

-- check for unwanted spaces each column
-- Expectation: No Results
SELECT 
prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


-- check for negative costs
-- Expectation: No Results
SELECT 
prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- check for Data Standardization & Consistency (maps coded values to meaningful, userfriendly descriptions)
-- 1. check values in column via DISTINCT, decide than if you want to adapt some values to avoid e.g. abbreveations
SELECT 
DISTINCT(prd_line)
FROM bronze.crm_prd_info;

-- check for invalid date-orders, eg. end_date before start_date
SELECT
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt_test
FROM bronze.crm_prd_info;

------------------------------------------

-- silver.crm_sales_details
-- check and prepare columns for import silver_layer

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details;

-- WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);
-- WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);
-- WHERE sls_ord_num != TRIM(sls_ord_num);


-- check for invalid date-orders, bring them into the right format when inserting
SELECT
NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) != 8   --issue covered
OR sls_due_dt > 20500101  
OR sls_due_dt < 19000101;

-- check for invalid order-dates, which must be older...
SELECT
*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;
--no issues


-- check if sales prices matches the rule quanitity * price = sales
/* If Sales is negative, zero, or null, derive it using Quantity and Price
   If Price is zero or null, calculate it using Sales and Quantity
   If Price is negative, convert it to a positive value */

SELECT DISTINCT
sls_sales AS old_sales,
sls_quantity,
sls_price AS old_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
    THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price is NULL OR sls_price <= 0
    THEN sls_sales / NULLIF(sls_quantity, 0) -- gibt Null zurück falls sls_quantity 0 ist.
    ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;
--issues found



-- check bronze.erp_cust_az12

SELECT
cid,
bdate,
gen
FROM bronze.erp_cust_az12;

--Format cid as it doesn't match with the cst_key from crm_cust_info table
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid
END cid,
bdate,
gen
FROM bronze.erp_cust_az12
-- WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
--     ELSE cid
-- END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

--WHERE cid LIKE '%AW00011000%';

--SELECT * FROM [silver].[crm_cust_info];


-- check strange birthday dates, too old or in the future

SELECT
bdate
FROM bronze.erp_cust_az12
WHERE bdate <= '1924-01-01' OR bdate > GETDATE()
--issues dates in future has to set to NULL

--Data  Standardization & Consistency
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;



-- check bronze.erp_loc_a101

-- cid doesn't match with cst_ky from silver.crm_cust_info
SELECT
REPLACE(cid, '-', '') cid,
cntry AS old_cntry,
CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
    WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
    WHEN (TRIM(cntry)) = '' OR cntry IS NULL THEN 'n/a'
    ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;


-- WHERE REPLACE(cid, '-', '') NOT IN (SELECT cst_key FROM silver.crm_cust_info)  ;

--check for Data Standarization & Consistency

SELECT DISTINCT cntry
FROM bronze.erp_loc_a101;



-- erp_px_cat_g1v2

SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;

--check for unwanted spaces

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat)  OR maintenance != TRIM(maintenance);

--Data Standarization & Consistency

SELECT DISTINCT
subcat
FROM bronze.erp_px_cat_g1v2;
-- no issues
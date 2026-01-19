/*
===============================================================================
Gespeicherte Prozedur: Laden der Silver-Schicht (Bronze -> Silver)
===============================================================================
Zweck des Skripts:
    Diese gespeicherte Prozedur führt den ETL-Prozess (Extract, Transform, Load) aus,
    um die Tabellen des Schemas „silver“ mit Daten aus dem Schema „bronze“ zu befüllen.

Ausgeführte Aktionen:
        - Leert (TRUNCATE) die Silver-Tabellen.
        - Fügt transformierte und bereinigte Daten aus Bronze in die Silver-Tabellen ein.

Parameter:
    Keine.
    Diese gespeicherte Prozedur akzeptiert keine Parameter und gibt keine Werte zurück.

Beispiel zur Verwendung:
    EXEC Silver.load_silver;
===============================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    -- silver.crm_cust_info

    -- INSERT clean data INTO silver.crm_cust_info
    -- Cleaning column 'cst_id' multiple and NULL values
    -- 1.Trim cst_firstname & cst_lastname as they have spaces
    -- 2.Change values like marital_status & gender into more clear values

    -- Truncate before you renew the tables with an insert to avoid double data
    TRUNCATE TABLE silver.crm_cust_info;
    --insert into prepared table
    INSERT INTO silver.crm_cust_info(
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    -- cleaning insert data
    SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END cst_marital_status,
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END cst_gndr,
    cst_create_date
    FROM(
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY cst_id
                ORDER BY cst_create_date DESC
            ) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    )t WHERE flag_last = 1; 



    ------------------------------------------

    -- silver.crm_prd_info
    -- INSERT clean data INTO silver.crm_prd_info
    -- Cleaning column 'cst_id' multiple and NULL values etc...

    -- Truncate before you renew the tables with an insert to avoid double data
    TRUNCATE TABLE silver.crm_prd_info;
    --insert into prepared table
    INSERT INTO silver.crm_prd_info(
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
    prd_id,
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
        DATEADD(day, -1,
            LEAD(prd_start_dt) OVER (
                PARTITION BY prd_key
                ORDER BY prd_start_dt
            )
        ) AS prd_end_dt_test
    FROM bronze.crm_prd_info;


    -- SELECT * FROM silver.crm_prd_info;


    ------------------------------------------

    -- silver.crm_sales_details
    -- INSERT clean data INTO silver.crm_sales_details
    -- Cleaning column 'cst_id' multiple and NULL values etc...

    -- Truncate before you renew the tables with an insert to avoid double data
    TRUNCATE TABLE silver.crm_sales_details;
    --insert into prepared table
    INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
    )
    SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)     -- convert wrong date format from int, to string, to date
    END AS sls_order_dt,
    CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE WHEN sls_price is NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0) -- gibt Null zurück falls sls_quantity 0 ist.
        ELSE sls_price
    END AS sls_price
    FROM bronze.crm_sales_details;


    --------------------------

    -- silver.erp_cust_az12

    -- Truncate before you renew the tables with an insert to avoid double data
    TRUNCATE TABLE silver.erp_cust_az12;
    --insert into prepared table
    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END cid,
    CASE WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,
    CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen
    FROM bronze.erp_cust_az12;


    -- silver.erp_loc_a101

    -- Truncate before you renew the tables with an insert to avoid double data
    TRUNCATE TABLE silver.erp_loc_a101;
    --insert into prepared table
    INSERT INTO silver.erp_loc_a101(
        cid,
        cntry
    )
    SELECT
    REPLACE(cid, '-', '') cid,
    CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
        WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
        WHEN (TRIM(cntry)) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
    FROM bronze.erp_loc_a101
    ORDER BY cntry;

    -- silver.erp_px_cat_g1v2

    -- Truncate before you renew the tables with an insert to avoid double data
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    --insert into prepared table
    INSERT INTO silver.erp_px_cat_g1v2(
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
    id,
    cat,
    subcat,
    maintenance
    FROM bronze.erp_px_cat_g1v2;
END
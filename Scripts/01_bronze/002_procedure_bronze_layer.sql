/*
===============================================================================
Gespeicherte Prozedur: Laden der Silver-Schicht (Bronze -> Silver)
===============================================================================
Zweck des Skripts:
Diese gespeicherte Prozedur lädt Daten aus externen CSV-Dateien in das Schema „bronze“.
Dabei werden folgende Aktionen ausgeführt:

Leert (TRUNCATE) die Bronze-Tabellen vor dem Laden der Daten.

Verwendet den Befehl BULK INSERT, um Daten aus CSV-Dateien in die Bronze-Tabellen zu laden.

Parameter:
Keine.
Diese gespeicherte Prozedur akzeptiert keine Parameter und gibt keine Werte zurück.
===============================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @start_time DATETIME,
        @end_time   DATETIME,
        @duration   INT;

    BEGIN TRY    
        /* ================================================= */
        PRINT '========================================';
        PRINT 'Loading Bronze Layer';
        PRINT '========================================';

        PRINT '----------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '----------------------------------------';

        /* ---------- crm_cust_info ---------- */
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data Into: crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\britt\Documents\Data_Engineer\DWH_Project\sql-data-analytics-project\datasets\csv-files\bronze.crm_cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        SET @duration = DATEDIFF(SECOND, @start_time, @end_time);

        PRINT 'Start Time: ' + CONVERT(varchar(30), @start_time, 121);
        PRINT 'End Time:   ' + CONVERT(varchar(30), @end_time, 121);
        PRINT 'Load Duration: ' + CAST(@duration AS varchar(10)) + ' seconds';
        PRINT '----------------------------------------';

        /* Optional: Zeilen zählen */
        SELECT COUNT(*) AS crm_cust_info_count 
        FROM bronze.crm_cust_info;

    END TRY
    BEGIN CATCH
        PRINT '=================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS varchar(10));
        PRINT 'Error State: '  + CAST(ERROR_STATE()  AS varchar(10));
        PRINT '=================================';

        THROW; -- Originalfehler korrekt weiterwerfen
    END CATCH
END;



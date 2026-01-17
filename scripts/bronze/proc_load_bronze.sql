/*
Project    : SQL Data Warehouse
File       : bronze.load_bronze.sql
Purpose    : Orchestrates full reload of the Bronze layer from raw CSV sources into SQL Server staging tables.
Author     : Ramdiany
Platform   : Microsoft SQL Server
Layer      : Bronze (Raw Ingestion)
Repository : sql-data-warehouse-project

Description:
  Loads CRM and ERP source files into the Bronze schema using BULK INSERT. The procedure performs a full refresh
  with truncate-and-load semantics, logs per-table load duration, and captures total runtime. Errors are surfaced
  with detailed diagnostics for operational triage.

Design Principles:
  - Idempotent full reload for reproducibility
  - Explicit logging for observability
  - Batch-safe execution with predictable behavior
  - Minimal transformation (raw landing only)

Change Log:
  2026-01-17 Initial version
*/


SET NOCOUNT ON;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
DECLARE
@start_time        DATETIME,
@end_time          DATETIME,
@start_whole_time  DATETIME,
@end_whole_time    DATETIME;

```
BEGIN TRY
    SET @start_whole_time = GETDATE();

    PRINT '==================================================';
    PRINT '--------------- LOADING BRONZE LAYER --------------';
    PRINT '==================================================';

    PRINT '--------------------------------------------------';
    PRINT '               Loading CRM tables                  ';
    PRINT '--------------------------------------------------';
    PRINT '';

    /* CRM: Customer */
    SET @start_time = GETDATE();
    PRINT '>> Truncating table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    PRINT '>> Inserting data into: bronze.crm_cust_info';
    BULK INSERT bronze.crm_cust_info
    FROM 'D:\SQL Data Warehouse Project\datasets\source_crm\cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
    PRINT '------------------------';
    PRINT '';

    /* CRM: Product */
    SET @start_time = GETDATE();
    PRINT '>> Truncating table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    PRINT '>> Inserting data into: bronze.crm_prd_info';
    BULK INSERT bronze.crm_prd_info
    FROM 'D:\SQL Data Warehouse Project\datasets\source_crm\prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
    PRINT '------------------------';
    PRINT '';

    /* CRM: Sales Details */
    SET @start_time = GETDATE();
    PRINT '>> Truncating table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    PRINT '>> Inserting data into: bronze.crm_sales_details';
    BULK INSERT bronze.crm_sales_details
    FROM 'D:\SQL Data Warehouse Project\datasets\source_crm\sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
    PRINT '------------------------';
    PRINT '';

    PRINT '--------------------------------------------------';
    PRINT '                Loading ERP tables                 ';
    PRINT '--------------------------------------------------';
    PRINT '';

    /* ERP: Customer */
    SET @start_time = GETDATE();
    PRINT '>> Truncating table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;

    PRINT '>> Inserting data into: bronze.erp_cust_az12';
    BULK INSERT bronze.erp_cust_az12
    FROM 'D:\SQL Data Warehouse Project\datasets\source_erp\cust_az12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
    PRINT '------------------------';
    PRINT '';

    /* ERP: Location */
    SET @start_time = GETDATE();
    PRINT '>> Truncating table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    PRINT '>> Inserting data into: bronze.erp_loc_a101';
    BULK INSERT bronze.erp_loc_a101
    FROM 'D:\SQL Data Warehouse Project\datasets\source_erp\loc_a101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
    PRINT '------------------------';
    PRINT '';

    /* ERP: Price Category */
    SET @start_time = GETDATE();
    PRINT '>> Truncating table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    PRINT '>> Inserting data into: bronze.erp_px_cat_g1v2';
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'D:\SQL Data Warehouse Project\datasets\source_erp\px_cat_g1v2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    SET @end_time = GETDATE();
    SET @end_whole_time = GETDATE();

    PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
    PRINT '';
    PRINT '==================================================';
    PRINT 'Total load duration: ' + CAST(DATEDIFF(SECOND, @start_whole_time, @end_whole_time) AS NVARCHAR(20)) + ' seconds';
    PRINT '==================================================';

END TRY
BEGIN CATCH
    PRINT '==================================================';
    PRINT 'Error occurred during loading Bronze layer';
    PRINT 'Error message: ' + ERROR_MESSAGE();
    PRINT 'Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(20));
    PRINT 'Error state: ' + CAST(ERROR_STATE() AS NVARCHAR(20));
    PRINT '==================================================';
    THROW; -- rethrow for upstream orchestration
END CATCH
```

END;
GO

-- Operational Notes:
-- 1) Truncate-and-load is intentional for Bronze to guarantee reproducibility from raw sources.
-- 2) TABLOCK improves load throughput for large files.
-- 3) FIRSTROW=2 skips headers; ensure source files always include a header row.
-- 4) Paths are absolute; parameterize them later for CI/CD and environment portability.

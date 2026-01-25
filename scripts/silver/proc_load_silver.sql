/*
Project    : SQL Data Warehouse
File       : proc_load_silver.sql
Purpose    : Load and transform data from Bronze to Silver layer
Author     : Ramdiany
Platform   : Microsoft SQL Server
Layer      : Silver (Cleansed / Standardized)
Repository : sql-data-warehouse-project

Description:
This stored procedure orchestrates the loading of Silver layer tables from
Bronze CRM and ERP sources. It applies basic cleansing, standardization, and
deduplication logic while preserving source-system boundaries.

The procedure:
- Truncates Silver tables before load
- Applies light business standardization
- Captures execution duration per table
- Logs total runtime for orchestration visibility

Execution Strategy:
- Full refresh (truncate & load)
- Sequential table processing
- TRY/CATCH with rethrow for upstream control

Change Log:
2026-01-23  Initial Silver load procedure
*/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    DECLARE
        @start_time         DATETIME,
        @end_time           DATETIME,
        @start_whole_time   DATETIME,
        @end_whole_time     DATETIME;

    BEGIN TRY
        SET @start_whole_time = GETDATE();

        PRINT '==================================================';
        PRINT '-------------- LOADING SILVER LAYER --------------';
        PRINT '==================================================';

        PRINT '--------------------------------------------------';
        PRINT '               Loading CRM tables                 ';
        PRINT '--------------------------------------------------';
        PRINT '';

        /* =========================================================
           CRM: Customer Information
           - Deduplicated by latest create date per customer
           - Code values standardized for gender and marital status
           ========================================================= */
        SET @start_time = GETDATE();
        PRINT '>> truncating table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> inserting data into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info
        (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM
        (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
        ) t
        WHERE flag_last = 1
          AND cst_id IS NOT NULL;

        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '------------------------';
        PRINT '';

        /* =========================================================
           CRM: Product Information
           - Product key parsed into category and product identifiers
           - End date derived using window function
           ========================================================= */
        SET @start_time = GETDATE();
        PRINT '>> truncating table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> inserting data into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info
        (
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
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
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
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(LEAD(prd_start_dt)
                 OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '------------------------';
        PRINT '';

        /* =========================================================
           CRM: Sales Transactions
           - Invalid dates normalized to NULL
           - Sales amount recalculated when inconsistent
           ========================================================= */
        SET @start_time = GETDATE();
        PRINT '>> truncating table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> inserting data into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details
        (
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
            CASE
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END,
            CASE
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END,
            CASE
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END,
            CASE
                WHEN sls_sales IS NULL
                     OR sls_sales <= 0
                     OR sls_sales != sls_quantity * sls_price
                THEN ABS(sls_price) * sls_quantity
                ELSE sls_sales
            END,
            sls_quantity,
            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '------------------------';
        PRINT '';

        PRINT '--------------------------------------------------';
        PRINT '                Loading ERP tables                ';
        PRINT '--------------------------------------------------';
        PRINT '';

        /* =========================================================
           ERP: Customer Master
           ========================================================= */
        SET @start_time = GETDATE();
        PRINT '>> truncating table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> inserting data into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END,
            CASE
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END,
            CASE
                WHEN TRIM(UPPER(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN TRIM(UPPER(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '------------------------';
        PRINT '';

        /* =========================================================
           ERP: Customer Location
           ========================================================= */
        SET @start_time = GETDATE();
        PRINT '>> truncating table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> inserting data into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (cid, ctry)
        SELECT
            REPLACE(cid, '-', ''),
            CASE
                WHEN TRIM(ctry) = 'DE' THEN 'Germany'
                WHEN TRIM(ctry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(ctry) = '' OR ctry IS NULL THEN 'n/a'
                ELSE TRIM(ctry)
            END
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '------------------------';
        PRINT '';

        /* =========================================================
           ERP: Price & Category Mapping
           ========================================================= */
        SET @start_time = GETDATE();
        PRINT '>> truncating table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> inserting data into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2;

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
        PRINT 'Error occurred during loading Silver layer';
        PRINT 'Error message: ' + ERROR_MESSAGE();
        PRINT 'Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(20));
        PRINT 'Error state: ' + CAST(ERROR_STATE() AS NVARCHAR(20));
        PRINT '==================================================';
        THROW; -- Rethrow for upstream orchestration
    END CATCH
END;

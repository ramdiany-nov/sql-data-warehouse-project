/*
Project    : SQL Data Warehouse
File       : qualiy_check_silver.sql
Purpose    : Data quality validation for Silver layer tables
Author     : Ramdiany
Platform   : Microsoft SQL Server
Layer      : Silver (Cleansed / Standardized)
Repository : sql-data-warehouse-project

Description:
This script contains ad-hoc data quality checks for Silver layer tables.
Each query is designed to validate key assumptions after Bronze-to-Silver
transformations.

General Expectations:
- Queries should return ZERO rows unless otherwise stated
- Any returned rows indicate data quality issues
- Script is read-only and safe to re-run

Validation Categories:
- Primary key uniqueness
- Null and invalid value detection
- Trimming and whitespace validation
- Standardization consistency
- Temporal logic checks

Change Log:
2026-01-23  Initial Silver layer test suite
*/

------------------------------------------------------------
-- Table 1: silver.crm_cust_info
------------------------------------------------------------

-- Check for NULLs or duplicates in primary key
-- Expectation: no results
SELECT
    cst_id,
    COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1
   OR cst_id IS NULL;

-- Check for unwanted leading/trailing spaces
-- Expectation: no results
SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Data standardization & consistency checks
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

-- Full table inspection (manual review)
SELECT *
FROM silver.crm_cust_info;


------------------------------------------------------------
-- Table 2: silver.crm_prd_info
------------------------------------------------------------

-- Check for NULLs or duplicates in primary key
-- Expectation: no results
SELECT
    prd_id,
    COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;

-- Check for unwanted spaces in product name
-- Expectation: no results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULL or negative cost values
-- Expectation: no results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0
   OR prd_cost IS NULL;

-- Data standardization & consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check for invalid date ranges
-- Expectation: no results
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Full table inspection
SELECT *
FROM silver.crm_prd_info;


------------------------------------------------------------
-- Table 3: silver.crm_sales_details
------------------------------------------------------------

-- Check for invalid date relationships
-- Expectation: no results
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

-- Data consistency between sales, quantity, and price
-- Rules:
--   sales = quantity * price
--   values must be non-null and positive
-- Expectation: no results
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- Full table inspection
SELECT *
FROM silver.crm_sales_details;


------------------------------------------------------------
-- Table 4: silver.erp_cust_az12
------------------------------------------------------------

-- Identify out-of-range birth dates
-- Expectation: no results
SELECT DISTINCT
    bdate
FROM silver.erp_cust_az12
WHERE bdate > GETDATE();

-- Data standardization & consistency
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

-- Full table inspection
SELECT *
FROM silver.erp_cust_az12;


------------------------------------------------------------
-- Table 5: silver.erp_loc_a101
------------------------------------------------------------

-- Data standardization & consistency
SELECT DISTINCT ctry
FROM silver.erp_loc_a101
ORDER BY ctry;

-- Full table inspection
SELECT *
FROM silver.erp_loc_a101;


------------------------------------------------------------
-- Table 6: silver.erp_px_cat_g1v2
------------------------------------------------------------

-- Check for unwanted spaces in key attributes
-- Expectation: no results
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE id != TRIM(id);

SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat);

SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat);

SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance);

-- Data standardization & consistency
SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2;

-- Full table inspection
SELECT *
FROM bronze.erp_px_cat_g1v2;


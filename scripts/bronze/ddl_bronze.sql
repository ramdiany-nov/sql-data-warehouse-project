/*
================================================================================
 File       : ddl_bronze.sql
 Purpose    : Create Bronze layer tables for the SQL Server Data Warehouse
 Author     : Ramdiany
 Platform   : Microsoft SQL Server
 Layer      : Bronze (Raw / Ingestion)

 Description:
   This script creates the Bronze layer tables used to land raw data from CRM and
   ERP source systems. The Bronze layer is intentionally lightly typed and minimal
   in constraints to support fast, fault‑tolerant ingestion and schema evolution.

 Design Principles:
   - Idempotent: Safe to re‑run (drops and recreates tables)
   - Minimal constraints: Validation happens in Silver layer
   - Naming convention: source_system.object_name
   - Data types chosen for ingestion reliability, not analytics optimization

 Change Log:
   2026‑01‑17  Initial version
================================================================================
*/

SET NOCOUNT ON;
GO

/* =========================
   CRM: Customer Information
   ========================= */
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info
(
    cst_id            INT,
    cst_key           NVARCHAR(50),
    cst_firstname     NVARCHAR(50),
    cst_lastname      NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr          NVARCHAR(50),
    cst_create_date   DATE
);
GO

/* ======================
   CRM: Product Information
   ====================== */
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info
(
    prd_id        INT,
    prd_key       NVARCHAR(50),
    prd_nm        NVARCHAR(50),
    prd_cost      INT,
    prd_line      NVARCHAR(10),
    prd_start_dt  DATETIME,
    prd_end_dt    DATETIME
);
GO

/* =====================
   CRM: Sales Transactions
   ===================== */
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details
(
    sls_ord_num   NVARCHAR(50),
    sls_prd_key   NVARCHAR(50),
    sls_cust_id   INT,
    sls_order_dt  INT,
    sls_ship_dt   INT,
    sls_due_dt    INT,
    sls_sales     INT,
    sls_quantity  INT,
    sls_price     INT
);
GO

/* ======================
   ERP: Customer Master
   ====================== */
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12
(
    cid   NVARCHAR(50),
    bdate DATE,
    gen   NVARCHAR(10)
);
GO

/* ======================
   ERP: Customer Location
   ====================== */
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101
(
    cid   NVARCHAR(50),
    ctry  NVARCHAR(50)
);
GO

/* =========================
   ERP: Product Category Map
   ========================= */
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2
(
    id          NVARCHAR(50),
    cat         NVARCHAR(50),
    subcat      NVARCHAR(50),
    maintenance NVARCHAR(10)
);
GO

/* =========================
   Notes (Professional Rationale)
   =========================
   1. No primary keys or foreign keys are defined here by design.
      Reason: Bronze layer prioritizes ingestion speed and flexibility. All data
      quality enforcement happens in the Silver layer.

   2. NVARCHAR is used instead of VARCHAR.
      Reason: Safe for international data and future‑proof for Unicode sources.

   3. INT date fields in crm_sales_details are kept as INT.
      Reason: Source‑aligned raw ingestion. Casting to DATE happens in Silver.

   4. GO batch separators are used.
      Reason: Required by SQL Server for clean batch execution when dropping and
      recreating objects.
*/

/*
Project    : SQL Data Warehouse
File       : ddl_silver.sql
Purpose    : Create Silver layer tables for the SQL Server Data Warehouse
Author     : Ramdiany
Platform   : Microsoft SQL Server
Layer      : Silver (Cleansed / Standardized)
Repository : sql-data-warehouse-project

Description:
This script creates the Silver layer tables derived from Bronze CRM and ERP data.
The Silver layer represents structured, standardized datasets prepared for
business modeling and analytical consumption in the Gold layer.

Data in this layer is expected to be:
- Cleaned and type-aligned
- Structurally consistent across sources
- Ready for joins and dimensional modeling

Design Principles:
- Idempotent: Safe to re-run (drops and recreates tables)
- No enforced business constraints
- Consistent audit column (dwh_create_date)
- Source-system separation preserved

Change Log:
2026-01-23  Initial Silver layer version
*/

SET NOCOUNT ON;
GO

/* =========================================================
   CRM: Customer Information
   ========================================================= */
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info
(
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE,

    -- Audit column capturing load timestamp into Silver layer
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

/* =========================================================
   CRM: Product Information
   ========================================================= */
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info
(
    prd_id          INT,
    cat_id          NVARCHAR(50),
    prd_key         NVARCHAR(50),
    prd_nm          NVARCHAR(50),
    prd_cost        INT,
    prd_line        NVARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,

    -- Record creation timestamp in the warehouse
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

/* =========================================================
   CRM: Sales Transactions
   ========================================================= */
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details
(
    sls_ord_num     NVARCHAR(50),
    sls_prd_key     NVARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt   DATE,
    sls_ship_dt    DATE,
    sls_due_dt     DATE,
    sls_sales      INT,
    sls_quantity   INT,
    sls_price      INT,

    -- Load timestamp for traceability
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

/* =========================================================
   ERP: Customer Master
   ========================================================= */
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12
(
    cid             NVARCHAR(50),
    bdate           DATE,
    gen             NVARCHAR(10),

    -- Timestamp when record is written to Silver layer
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

/* =========================================================
   ERP: Customer Location
   ========================================================= */
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101
(
    cid             NVARCHAR(50),
    ctry            NVARCHAR(50),

    -- Warehouse load timestamp
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

/* =========================================================
   ERP: Product Price & Category
   ========================================================= */
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2
(
    id              NVARCHAR(50),
    cat             NVARCHAR(50),
    subcat          NVARCHAR(50),
    maintenance     NVARCHAR(10),

    -- Silver layer ingestion timestamp
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

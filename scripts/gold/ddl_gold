/*
Project    : SQL Data Warehouse
File       : ddl_gold.sql
Purpose    : Create Gold layer views for analytics and reporting
Author     : Ramdiany
Platform   : Microsoft SQL Server
Layer      : Gold (Business / Presentation)
Repository : sql-data-warehouse-project

Description:
This script defines the Gold layer objects representing the business-facing
semantic model of the data warehouse. The Gold layer exposes curated dimensions
and facts optimized for analytical queries and reporting.

The objects in this layer:
- Apply business-friendly naming
- Define clear dimensional grains
- Hide source-system complexity from consumers

Design Principles:
- Read-only views
- One business concept per object
- Explicit surrogate keys for dimensional modeling
- Joins only against Silver or Gold objects

Change Log:
2026-01-23  Initial Gold layer definitions
*/

------------------------------------------------------------
-- Dimension: Customers
------------------------------------------------------------
-- Grain: One row per customer
-- Source: CRM customer master enriched with ERP demographics
------------------------------------------------------------

CREATE VIEW gold.dim_customers
AS
SELECT
    -- Surrogate key for dimensional joins
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,

    -- Business identifiers
    ci.cst_id        AS customer_id,
    ci.cst_key       AS customer_number,

    -- Descriptive attributes
    ci.cst_firstname AS first_name,
    ci.cst_lastname  AS last_name,
    la.ctry          AS country,
    ci.cst_marital_status AS marital_status,

    -- Gender resolution: CRM preferred, ERP as fallback
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,

    -- Demographics
    ca.bdate         AS birthdate,

    -- Reference creation date from CRM
    ci.cst_create_date AS create_date

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;



------------------------------------------------------------
-- Dimension: Products
------------------------------------------------------------
-- Grain: One row per active product
-- Note : Only current products (prd_end_dt IS NULL)
------------------------------------------------------------

CREATE VIEW gold.dim_products
AS
SELECT
    -- Surrogate key for dimensional joins
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,

    -- Business identifiers
    pn.prd_id    AS product_id,
    pn.prd_key   AS product_number,

    -- Descriptive attributes
    pn.prd_nm    AS product_name,
    pn.cat_id    AS category_id,
    pc.cat       AS category,
    pc.subcat    AS subcategory,
    pc.maintenance,
    pn.prd_cost  AS cost,
    pn.prd_line  AS product_line,

    -- Lifecycle
    pn.prd_start_dt AS start_date

FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;



------------------------------------------------------------
-- Fact: Sales
------------------------------------------------------------
-- Grain: One row per sales order line
-- Joins: Product and Customer dimensions
------------------------------------------------------------

CREATE VIEW gold.fact_sales
AS
SELECT
    -- Transaction identifiers
    sd.sls_ord_num AS order_number,

    -- Dimension keys
    pr.product_key,
    cr.customer_key,

    -- Degenerate & reference keys
    sd.sls_cust_id AS customer_id,

    -- Dates
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,

    -- Measures
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price,
    sd.sls_sales    AS sales_amount

FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cr
    ON sd.sls_cust_id = cr.customer_id;

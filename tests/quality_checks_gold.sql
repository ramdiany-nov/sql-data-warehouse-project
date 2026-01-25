/* ============================================================
Project      : SQL Data Warehouse
File         : quality_checks_gold.sql
Purpose      : Validate Gold layer dimensional model integrity
Author       : Ramdiany
Platform     : Microsoft SQL Server
Layer        : Gold (Analytics / Presentation)
Repository   : sql-data-warehouse-project

Description:
This script performs data quality and integrity validations
for the Gold layer. The checks ensure surrogate key uniqueness
and referential integrity between fact and dimension tables,
supporting reliable analytical consumption.

Design Principles:
- Read-only: No mutation of Gold layer objects
- Deterministic: Expected result is zero rows returned
- Analytics-safe: Prevents silent dimensional model corruption
- Execution-order aware: Dimensions validated before facts

Change Log:
2026-01-25  Initial version
============================================================ */


/* ============================================================
1. DIM_CUSTOMERS — Surrogate Key Uniqueness Check
============================================================ */

select
	customer_key,
	count(*) as duplicate_count
from gold.dim_customers
group by customer_key
having count(*) > 1;


/* ============================================================
2. DIM_PRODUCTS — Surrogate Key Uniqueness Check
============================================================ */

select
	product_key,
	count(*) as duplicate_count
from gold.dim_products
group by product_key
having count(*) > 1;


/* ============================================================
3. FACT_SALES — Referential Integrity Validation
============================================================
Purpose:
- Ensures all fact rows reference valid dimension members
- Detects orphaned records caused by upstream data issues
============================================================ */

select
	f.order_number,
	f.product_key,
	f.customer_key,
	f.order_date,
	f.sales_amount
from gold.fact_sales f
left join gold.dim_customers c
	on c.customer_key = f.customer_key
left join gold.dim_products p
	on p.product_key = f.product_key
where c.customer_key is null
   or p.product_key is null;

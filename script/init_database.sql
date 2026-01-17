/*==============================================================================
  Project        : SQL Data Warehouse
  File           : init_database.sql
  Purpose        : Initialise core database and schema structure for the DW stack
  Author         : Ramdiany
  Platform       : Microsoft SQL Server
  Layering Model : Bronze / Silver / Gold (Medallion Architecture)
  Repository     : sql-data-warehouse-project


  Description:
  - Creates the DataWarehouse database if it does not already exist.
  - Establishes the Medallion Architecture schemas:
      bronze : Raw, immutable ingestion layer
      silver : Cleaned, conformed, business-ready data
      gold   : Analytics-ready, aggregated, semantic layer

  Notes:
  - This script is idempotent and safe to re-run.
  - Run with sysadmin or dbcreator privileges.

  Change Log:
    2026‑01‑17  Initial version
==============================================================================*/


USE master;
GO

-- Create database only if it does not exist
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    CREATE DATABASE DataWarehouse;
END
GO

USE DataWarehouse;
GO

-- Create Medallion Architecture schemas
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC ('CREATE SCHEMA bronze');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC ('CREATE SCHEMA silver');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC ('CREATE SCHEMA gold');
GO

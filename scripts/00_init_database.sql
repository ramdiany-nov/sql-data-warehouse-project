/*==============================================================================
  Project        : Enterprise Data Warehouse
  Environment    : Microsoft SQL Server
  Layering Model : Bronze / Silver / Gold (Medallion Architecture)
  Author         : Ramdiany
  Repository     : sql-data-warehouse-project
  Purpose        : Initialise core database and schema structure for the DW stack

  Description:
  - Creates the DataWarehouse database if it does not already exist.
  - Establishes the Medallion Architecture schemas:
      bronze : Raw, immutable ingestion layer
      silver : Cleaned, conformed, business-ready data
      gold   : Analytics-ready, aggregated, semantic layer

  Notes:
  - This script is idempotent and safe to re-run.
  - Run with sysadmin or dbcreator privileges.
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

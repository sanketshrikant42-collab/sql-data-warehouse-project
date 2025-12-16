/*
===================================================
Create Database and Schemas
=====================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. If the database exists,
    it is dropped and recreated.
    Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.

Warning
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. 
    Proceed with caution and ensure you have proper backups before running this script.
*/

USE master;
GO

--Drop & Recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.database WHERE name ='DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehousr;
END;
GO


--create database 'DataWarehouse'
Create Database DataWarehouse;
GO
Use DataWarehouse;
GO

--Create Schemas
Create SCHEMA bronze;
GO
  
Create SCHEMA silver;
GO
  
Create SCHEMA gold;
GO

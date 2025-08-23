/*
======================================================================================================
Create Database and Schemas
=====================================================================================================
Script Purpose:
  This script creates a new database anems 'DataWarehouse ' after checking if it already exists.
  If the database exists , it is dropped and recreated. Additionally, the script sets up three schemas
  within the database: 'bronze', 'silver',and 'gold'.

Warning:
  Running this script will drop the entire 'DataWarehouse' database if ti exists.
  All data in th edatabase will be parmanently deleted . Proceed with caution
  and ensure you have proper backups before running this script.
*/

USE master;
GO

--Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET Single_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database

CREATE DATABASE DataWarehouse;

USE DataWarehouse;

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

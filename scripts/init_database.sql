/*
=============================================================================
CREATE DATABASES AND SCHEMAS
=============================================================================

Script Purpose:
	This Script creates a new database named 'DataWarehouse' after checking if it already exists.
	If the db exists, it is dropped and recreated. 
	Subsequently, 3 schemas are created: bronze, silver, gold

Warning:
	Running this script will delete the 'DataWarehouse' Database and all of the data within it.
	The deletion is permanent. Proceed with caution and ensure you have proper backups before running this script.

*/


-- switch to master database
USE master;

IF EXISTS (SELECT * FROM sys.databases where name = 'DataWarehouse')
	BEGIN
		ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
		DROP DATABASE DataWarehouse;
	END
GO

-- create Database
CREATE DATABASE DataWarehouse;
USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO


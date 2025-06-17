/*
========================================================================================================
LOAD BRONE STORED PROCEDURE
========================================================================================================
Script Purpose:
  This script loads given csv files to the datawarehouse
  The loading format used is "Truncate and Insert"
  Load times of datasets are calculated
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '======================================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '======================================================================================';
		PRINT '';

		PRINT '--------------------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT 'Loading table bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\karti\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FORMAT = 'CSV',
			FIELDTERMINATOR = ',',
			FIRSTROW = 2,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT 'Loading table bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\karti\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FORMAT = 'CSV',
			FIELDTERMINATOR = ',',
			FIRSTROW = 2,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT 'Loading table bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\karti\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FORMAT = 'CSV',
			FIELDTERMINATOR = ',',
			FIRSTROW = 2,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------------------------';

		PRINT '';
		PRINT '--------------------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------------------------------------------------------------------';
		PRINT '';

		SET @start_time = GETDATE();
		PRINT 'Truncating table bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT 'Loading table bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\karti\Downloads\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FORMAT = 'CSV',
			FIELDTERMINATOR = ',',
			FIRSTROW = 2,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT 'Loading table bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\karti\Downloads\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FORMAT = 'CSV',
			FIELDTERMINATOR = ',',
			FIRSTROW = 2,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT 'Loading table bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\karti\Downloads\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FORMAT = 'CSV',
			FIELDTERMINATOR = ',',
			FIRSTROW = 2,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------------------------';
		SET @batch_end_time = GETDATE();
		PRINT '';
		PRINT '>> Batch Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';

	END TRY
	BEGIN CATCH
		PRINT '======================================================================================';
		PRINT 'Error Loading Bronze Layer';
		PRINT '======================================================================================';
		PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
		PRINT 'ERROR STATE: ' + CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH
END

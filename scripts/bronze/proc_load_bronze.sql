/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=================================================================================
Script Purpose:
    This stored procedure load data into bronze schema from external CSV file
    It performs the following actions:
    - Truncates the bronze tables before loading data 
    - Uses the 'BULK INSERT' command to load the external CSV files to the bronze tables

Parameters: None
This stored procedure does not return any value .

Usage Example:
  EXECUTE bronze.load_bronze;
===============================================================================
/*
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
DECLARE @start DATETIME, @end DATETIME;
SET @start = GETDATE();
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY
		PRINT'=============================================';
		PRINT'LOADING BRONZE LAYER';
		PRINT'==============================================';

		PRINT '---------------------------------------------';
		PRINT 'LOADING CRM Tables';
		PRINT '---------------------------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING THE TABLE: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> INSRETING DATA INTO : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Admin\Desktop\sql project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT'--------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING THE TABLE: bronze.crm_prod_info';
		TRUNCATE TABLE bronze.crm_prod_info;

		PRINT '>> INSRETING DATA INTO : bronze.crm_prod_info';
		BULK INSERT bronze.crm_prod_info
		FROM 'C:\Users\Admin\Desktop\sql project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT'---------------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING THE TABLE: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> INSRETING DATA INTO : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Admin\Desktop\sql project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT'---------------------------------------------------';



		PRINT '***************************************************';
		PRINT 'LOADING ERP Tables';
		PRINT '***************************************************';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING THE TABLE: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> INSRETING DATA INTO : bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Admin\Desktop\sql project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT'---------------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING THE TABLE: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> INSERTING DATA INTO : bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Admin\Desktop\sql project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT'---------------------------------------------------';

		
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING THE TABLE: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> INSERTING DATA INTO : bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Admin\Desktop\sql project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT'---------------------------------------------------';
			
			
			END TRY
			BEGIN CATCH
				PRINT '================================='
				PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
				PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
				PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
				PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
				PRINT '================================='
			END CATCH
END;
SET @end = GETDATE();
PRINT'######################################################';
PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start, @end) AS NVARCHAR) + 'seconds'; 
PRINT'######################################################';

*/

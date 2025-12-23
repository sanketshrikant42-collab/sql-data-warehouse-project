/*
===============================================================================
Stored Procedure: Load silver Layer (Bronze -> Silver)
=================================================================================
Script Purpose:
    This stored procedure perform ETL(Extract, Transfor, Load) process to populate
    'silver' schema tables from 'bronze' schemma 
    It performs the following actions:
    - Truncates the silver tables before loading data 
    - Insert transformed, clean data from bronze into silver tables

Parameters: None
This stored procedure does not return any value .

Usage Example:
  EXECUTE silver.load_silver;
===============================================================================
/*

CREATE OR ALTER PROCEDURE silver.load_silver AS 
DECLARE @start DATETIME, @end DATETIME;
SET @start = GETDATE();
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY
		PRINT'=============================================';
		PRINT'LOADING SILVER LAYER';
		PRINT'==============================================';

		PRINT '---------------------------------------------';
		PRINT 'LOADING CRM Tables';
		PRINT '---------------------------------------------';
	
	SET @start_time = GETDATE();
	PRINT '>>TRUNCATING TABLE: silver.crm_cust_info'	
	TRUNCATE TABLE silver.crm_cust_info

	PRINT '>>INSERTING DATA INTO: silver.crm_cust_info '
	INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

	SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) as cst_firstmame, 								--remove unwanted spaces
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'		-- data normalization & standardization
		 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'		
		 ELSE 'n/a'
	END cst_marital_status,
	CASE WHEN UPPER(cst_gndr) = 'F' THEN 'Female' 						-- data normalization & standardization
		 WHEN UPPER(cst_gndr) = 'M' THEN 'Male'							--adding full name as male/female
		 ELSE 'n/a'
	END cst_gndr,
	cst_create_date
	FROM
		(SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL                       --- removing duplicates
	)AS t WHERE flag_last = 1;					-- select most recent records (data filtering)
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT'--------------------------------------------------';



	SET @start_time = GETDATE();
	PRINT '>>TRUNCATING TABLE: silver.crm_prod_info'
	TRUNCATE TABLE silver.crm_prod_info

	PRINT '>>INSERTING DATA INTO: silver.crm_prod_info '
	INSERT INTO silver.crm_prod_info(
		prod_id ,
		cat_id ,
		prod_key ,
		prod_name,
		prod_cost ,
		prod_line ,
		prod_start_dt,
		prod_end_dt 
	)
	SELECT 
		prod_id,
		REPLACE(SUBSTRING(prod_key,1,5),'-','_' ) AS cat_id,    --extract Category id
		SUBSTRING(prod_key, 7,LEN(prod_key)) AS prod_key,		--extract product key
		prod_name,
		ISNULL(prod_cost,0) AS prod_cost,
		CASE UPPER(TRIM(prod_line))
			 WHEN 'M' THEN 'Mountain' 
			 WHEN 'R' THEN 'Road' 
			 WHEN 'S' THEN 'Other Sales' 
			 WHEN 'T' THEN 'Touring' 
			 ELSE 'n/a'
		END AS prod_line,										--map product line codes to descriptive values
		CAST(prod_start_dt AS DATE) AS prod_start_dt,
		CAST(
			LEAD(prod_start_dt) OVER (PARTITION BY prod_key ORDER BY prod_start_dt)-
			1 AS DATE)
			AS prod_end_dt										--calculate the end date as one day before the start date
	FROM bronze.crm_prod_info;
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT'---------------------------------------------------';


	SET @start_time = GETDATE()
	PRINT '>>TRUNCATING TABLE: silver.crm_sales_details'

	TRUNCATE TABLE silver.crm_sales_details
	PRINT '>>INSERTING DATA INTO: silver.crm_sales_details '
	INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prod_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_date,
		sls_sales,
		sls_quantity,
		sls_price
	)
	SELECT 
	sls_ord_num,
	sls_prod_key,
	sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS varchar) AS DATE)
	END AS sls_order_dt,

	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS varchar) AS DATE)
	END AS sls_ship_dt,

	CASE WHEN sls_due_date = 0 OR LEN(sls_due_date) != 8 THEN NULL
		 ELSE CAST(CAST(sls_due_date AS varchar) AS DATE)
	END AS sls_due_date,

	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity* ABS(sls_price)
		 THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales 								--RECALCULATE SALES IF original value is missing or incorrect
	END AS sls_sales,
	sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
		 THEN sls_sales / NULLIF(sls_quantity, 0)
		 ELSE sls_price									--Derived price if original value is invalid
	END AS sls_price
	FROM bronze.crm_sales_details;
	SET @end_time = GETDATE();
	PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
	PRINT'---------------------------------------------------';

	PRINT '***************************************************';
	PRINT 'LOADING ERP Tables';
	PRINT '***************************************************';

	SET @start_time = GETDATE()
	PRINT '>>TRUNCATING TABLE: silver.erp_cust_az12'
	TRUNCATE TABLE silver.erp_cust_az12;

	PRINT '>>INSERTING DATA INTO: silver.erp_cust_az12 '
	INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
	)
	SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4,LEN(cid))	   --Remove NAS prefix if Present
		 ELSE cid
	END AS cid,                               
	CASE WHEN bdate > GETDATE() THEN NULL 
		   ELSE bdate
	END AS bdate,												   -- Set future dates as nulls
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'n/a'
	END AS gen													   -- Normalise gender values and handle unknown case 
	FROM bronze.erp_cust_az12;
	SET @end_time = GETDATE();
	PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
	PRINT'---------------------------------------------------';

	SET @start_time = GETDATE();
	PRINT '>>TRUNCATING TABLE: silver.erp_loc_a101'

	TRUNCATE TABLE silver.erp_loc_a101
	PRINT '>>INSERTING DATA INTO: silver.erp_loc_a101 '
	INSERT INTO silver.erp_loc_a101 (cid,cntry)
	SELECT
	REPLACE(cid, '-', '') cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END AS cntry										--Normalize and handle missing or blank country codes
	FROM bronze.erp_loc_a101 ;
	SET @end_time = GETDATE();
	PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
	PRINT'---------------------------------------------------';


	SET @start_time = GETDATE();
	PRINT '>>TRUNCATING TABLE: silver.erp_px_cat_g1v2'
	TRUNCATE TABLE silver.erp_px_cat_g1v2

	PRINT '>>INSERTING DATA INTO: silver.erp_px_cat_g1v2 '
	INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
	SELECT 
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2;
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
PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @starT, @end) AS NVARCHAR) + 'seconds'; 
PRINT'######################################################';

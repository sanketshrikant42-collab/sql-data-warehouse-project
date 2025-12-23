/*
======================================================================================================================
Quality Checks
Script Purpose:
This script performs various quality checks for data consistency, accuracy, and standardization across the 'silver schema.
It includes checks for:
                    -Null or duplicate primary keys.
                    -Unwanted spaces in string fields.
                    -Data standardization and consistency.
                    -Invalid date ranges and orders.
                    -Data consistency between related fields.


Usage Notes:
      - Run these checks after data loading Silver Layer.
      - Investigate and resolve any discrepancies found during the checks.
==================================================================================================================
*/

-- check null or duplicates 
SELECT 
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

--check unwanted spaces (firstname, lastname,gndr)
SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)

--data standardization and consistency rplacing m to male
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info
----------------------------------------------------------------------------------------------------------------------------------
-- data quality check

--CHECK NULL
SELECT
prod_id,
COUNT(*)
FROM bronze.crm_prod_info
GROUP BY prod_id
HAVING COUNT(*) > 1 OR prod_id IS NULL;

-- CHECK UNWANTED SPACES
SELECT prod_name 
FROM bronze.crm_prod_info
WHERE prod_name != TRIM(prod_name)

--CHECK FOR NULL OR NEGATIVE NUMBER
SELECT prod_cost
FROM bronze.crm_prod_info
WHERE prod_cost < 0 OR prod_cost IS NULL

-- DATA STANDARDIZATION
SELECT DISTINCT prod_line
FROM bronze.crm_prod_info

-- CHECK FOR INVALID DATES
SELECT * 
FROM bronze.crm_prod_info
WHERE prod_end_dt < prod_start_dt
-------------------------------------------------------------------------------------------------------------------------------------------
-- CHECK INVALID DATES
SELECT 
NULLIF(sls_due_date,0) AS sls_due_date
FROM bronze.crm_sales_details
WHERE sls_due_date <= 0
OR LEN(sls_due_date) != 8
OR sls_due_date > 20250101
OR sls_due_date < 19000101

-- CHECK FOR INVALID DATE ORDER
SELECT *
FROM bronze.crm_sales_details 
WHERE sls_order_dt > sls_ship_dt
OR sls_order_dt > sls_due_date

-- CHECK DATA CONSISTENCY BETWEEN SALES, PRICE, QUANTITY
-- > SALES = QUANTITY * PRICE
-- > Values must no be NULL, zero, Negative

SELECT DISTINCT
sls_sales AS OLD_SALES,
sls_quantity,
sls_price AS OLD_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity* ABS(sls_price)
	 THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0
	 THEN sls_sales / NULLIF(sls_quantity, 0)
	 ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales < 0 OR sls_quantity < 0 OR sls_price < 0
ORDER BY sls_sales, sls_quantity, sls_price


-- quality check data consistency 
SELECT DISTINCT
sls_sales ,
sls_quantity,
sls_price 
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales < 0 OR sls_quantity < 0 OR sls_price < 0
ORDER BY sls_sales, sls_quantity, sls_price

SELECT * FROM silver.crm_sales_details
--------------------------------------------------------------------------------------------------------------------------------------
-- data checks
-- Identifying out-of-range dates
SELECT DISTINCT 
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- DATA STANDARDIZATION AND CONSISTENCY
SELECT DISTINCT gen
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

SELECT * FROM silver.erp_cust_az12
---------------------------------------------------------------------------------------------------------------------------------------
-- data standardization
 SELECT DISTINCT cntry 
from bronze.erp_loc_a101
order by cntry

SELECT * FROM silver.erp_loc_a101
-----------------------------------------------------------------------------------------------------------------------------------------
--CHECKING UNWANTED SPACES
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

--DATA STANDARDIZATION
SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2

SELECT * FROM silver.erp_px_cat_g1v2
-----------------------------------------------------------------------------------------------------------------------------------------

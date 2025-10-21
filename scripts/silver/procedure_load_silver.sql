/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

-- EXEC Silver.load_Silver
CREATE OR ALTER PROCEDURE Silver.load_Silver AS
BEGIN

	DECLARE @start_time AS DATETIME, @end_time AS DATETIME, @start_batch_time AS DATETIME, @end_batch_time AS DATETIME
	BEGIN TRY

		SET @start_batch_time = GETDATE();
		PRINT '===============================================';
		PRINT 'Loading Silver Layer';
		PRINT '===============================================';

		PRINT '-----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------';
		--Step 1: Transforming the data, cleaning NULL Values and duplicate from cst_id
		-- Step 2: Clean up unwanted spaces from string columns by TRIM(column_name)
		-- Step 3: Make all abbrv full eg cst_gndr M should be Male and F should be Female and also apply upper just in
		--case mixed_case values appear later in your column
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: Silver.crm_cust_info';
		TRUNCATE TABLE Silver.crm_cust_info;
		PRINT '>>> Inseting Data into: Silver.crm_cust_info';
		INSERT INTO Silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname),
		TRIM(cst_lastname),
		--cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 ELSE 'N/A'
		END cst_marital_status,
		--cst_gndr,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'N/A'
		END cst_gndr,
		cst_create_date
		FROM
		(
		SELECT 
		*,
		--selecting only one from duplicates
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
		FROM Bronze.crm_cust_info 
		WHERE cst_id IS NOT NULL
		)t WHERE flag_last = 1 

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>------------';

		--==============================================
		-- crm_prd_info Table Cleaning And Transformation
		--==============================================
	
--We have to recreate the table again since there have beem changes 
/*IF OBJECT_ID ('Silver.crm_prd_info', 'U') IS NOT NULL 
			DROP TABLE Silver.crm_prd_info;
		CREATE TABLE Silver.crm_prd_info (
			prd_id INT,
			cat_id NVARCHAR(50),
			prd_key NVARCHAR(50),
			prd_nm NVARCHAR(50),
			prd_cost INT,
			prd_line NVARCHAR(50),
			prd_start_dt DATE,
			prd_end_dt DATE,
			dwh_end_dt DATETIME2 DEFAULT GETDATE()
		); */
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: Silver.crm_prd_info';
		TRUNCATE TABLE Silver.crm_prd_info;
		PRINT '>>> Inseting Data into: Silver.crm_prd_info';
		INSERT INTO Silver.crm_prd_info (
			prd_id ,
			cat_id ,
			prd_key ,
			prd_nm ,
			prd_cost ,
			prd_line ,
			prd_start_dt ,
			prd_end_dt 
		)
		SELECT 
		prd_id,
		--we need to extract the catefgory id from the prd_key cos the first five characters of the prd key is the category id
		-- if we check id from erp._px_cat_g1v2 we will see that and also we will replace '-' with '_' for it to match 
		-- erp._px_cat_g1v2 id
		REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_') AS cat_id,
		--Secondly now we need to extract the second part of the prd_key as the prd_key in order to join crm_sales_details and since
		-- the second part have difference length we cant specify len of what to extract with substring soo we will make it dynamic
		SUBSTRING(prd_key, 7, len(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		--prd_line,
		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			 ELSE 'N/A'
		END prd_line,
		-- lets cast as date instead of datetime since time info is missing from the data or does not exist
		CAST(prd_start_dt AS DATE),
		-- cos we have issues with some date where start date are higher than end date we will calculate the end date ourselves
		-- base on the next start date of the next row of the same product video sql with baraa 25:57:30
		CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS prd_end_dt
		FROM Bronze.crm_prd_info

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>------------';

		--===============================================
		-- crm_sales_details Table Cleaning And Transformation
		--===============================================
	/*IF OBJECT_ID ('Silver.crm_sales_details', 'U') IS NOT NULL 
			DROP TABLE Silver.crm_sales_details;
		CREATE TABLE Silver.crm_sales_details (
			sls_ord_num NVARCHAR(50),
			sls_prd_key NVARCHAR(50),
			sls_cust_id INT,
			sls_order_dt DATE,
			sls_ship_dt DATE,
			sls_due_date DATE,
			sls_sales INT,
			sls_quantity INT,
			sls_price INT, 
			dwh_end_dt DATETIME2 DEFAULT GETDATE()
		); */
	SET @start_time = GETDATE();
	PRINT '>>> Truncating Table: Silver.crm_sales_details';
	TRUNCATE TABLE Silver.crm_sales_details;
	PRINT '>>> Inserting Data into: Silver.crm_sales_details';

	INSERT INTO Silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
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
		sls_prd_key,
		sls_cust_id,
		TRY_CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE) AS sls_order_dt,
		TRY_CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE) AS sls_ship_dt,
		TRY_CAST(CAST(sls_due_date AS VARCHAR(8)) AS DATE) AS sls_due_date,
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 
             OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
	FROM Bronze.crm_sales_details;

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>>------------';



		--===============================================
		-- erp_cust_az12 Table Cleaning And Transformation
		--===============================================
		PRINT '-----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: Silver.erp_cust_az12';
		TRUNCATE TABLE Silver.erp_cust_az12;
		PRINT '>>> Inseting Data into: Silver.erp_cust_az12';
		INSERT INTO Silver.erp_cust_az12 (
			CID,
			BDATE,
			GEN
		)
		SELECT 
		--CID,
		CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
			 ELSE CID
		END CID,
		CASE WHEN BDATE > GETDATE() THEN NULL 
			 ELSE BDATE 
		END BDATE,
		CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'MALE'
			 ELSE 'N/A'
		END AS GEN
		FROM Bronze.erp_cust_az12
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>------------';

		--===============================================
		-- erp_loc_a101 Table Cleaning And Transformation
		-- ======================================
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: Silver.erp_loc_a101';
		TRUNCATE TABLE Silver.erp_loc_a101;
		PRINT '>>> Inseting Data into: Silver.erp_loc_a101';
		INSERT INTO Silver.erp_loc_a101
		( 
			cid,
			cntry
		)
		SELECT 
		REPLACE(cid, '-','') cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
			 ELSE TRIM(cntry)
		END cntry 
		FROM Bronze.erp_loc_a101 
		--WHERE REPLACE(cid, '-','') NOT IN (SELECT cst_key FROM Silver.crm_cust_info)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>------------';
		--===============================================
		-- erp_px_cat_g1v2 Table Cleaning And Transformation
		-- ======================================
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: Silver.erp_px_cat_g1v2';
		TRUNCATE TABLE Silver.erp_px_cat_g1v2;

		PRINT '>>> Inseting Data into: Silver.erp_px_cat_g1v2';
		INSERT INTO Silver.erp_px_cat_g1v2 (
			ID,
			CAT,
			SUBCAT,
			MAINTAINANCE
		)
		SELECT 
		ID,
		CAT,
		SUBCAT,
		MAINTAINANCE
		FROM Bronze.erp_px_cat_g1v2

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>------------';

		SET @end_batch_time = GETDATE();
		PRINT '======================================'
		PRINT 'Loading Silver Layer Completed'
		PRINT ' Total Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '======================================'
	END TRY 
	BEGIN CATCH
		PRINT '============================================';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '============================================'
	END CATCH
END






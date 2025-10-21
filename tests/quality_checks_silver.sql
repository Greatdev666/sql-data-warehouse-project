/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- Check for Nulls or Duplicates in Primary key
-- Expecatation: No result
SELECT 
cst_id,
COUNT(*)
FROM Silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

--Check 2: Check for unwanted spaces in String values
-- Expecatation: No result
-- Check for all string values, the lastname,gndr firstname
SELECT 
cst_lastname
FROM Silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

--Check the consistency of value in low cardinality columns cst_gndr and cst_marital status
SELECT DISTINCT cst_gndr
FROM Silver.crm_cust_info

-- ======================================
-- Table 2 Quality Check crm_prd_info
-- ======================================
SELECT 
prd_id,
COUNT(*)
FROM Silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL


--Check for unwanted spaces in strings
SELECT 
prd_nm
FROM Silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLs or Negative Numbes in Price/Cost
SELECT prd_cost
FROM Silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

--Check the consistency of value in low cardinality columns prd_line
SELECT DISTINCT prd_line
FROM Silver.crm_prd_info

--Check for invalid Date Orders
-- End date must not be earlier than start date
SELECT DISTINCT *
FROM Silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT * FROM Silver.crm_prd_info

-- ======================================
-- Table 3 Quality Check crm_sales_details
-- ======================================
SELECT 
*
FROM Silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

--Check if we can connect the table using the prd key to the intended table later on
SELECT 
*
FROM Silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM Silver.crm_prd_info)

SELECT 
*
FROM Silver.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM Silver.crm_cust_info)

-- Check for Invalid Dates
-- Dates was inputed as an integer 
SELECT 
NULLIF(sls_order_dt, 0) sls_order_dt
FROM Silver.crm_sales_details
WHERE sls_order_dt <= 0

-- Check for lenght of date it must be 8 not less not more 'yyyy-mm-dd'
SELECT 
NULLIF(sls_order_dt, 0) sls_order_dt
FROM Silver.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 OR sls_order_dt > 20500101

-- Date not higher than 2050 and not < when the business started
SELECT 
NULLIF(sls_order_dt, 0) sls_order_dt
FROM Silver.crm_sales_details
WHERE sls_order_dt > 20500101 OR sls_order_dt < 19000101

-- All checks in one query (check same thing for other date too like the shipping_dt, due_dt etc
SELECT 
NULLIF(sls_due_date, 0) sls_order_dt
FROM Silver.crm_sales_details
WHERE sls_due_date <= 0 
OR LEN(sls_due_date) != 8
OR sls_due_date > 20500101
OR sls_due_date < 19000101

-- Check for invalid date orders as order date must be earlier than the shipping date or due date
SELECT 
* 
FROM Silver.crm_sales_details 
WHERE sls_order_dt > sls_ship_dt
OR sls_order_dt > sls_due_date

-- Checking Sales , Quantity and Price
-- We have business rules
--1 : Sales = Quantity * Price
--2: Negative,zeros, Nulls are Not allowed
--RULES
--1: If sales is negative, zero, or null, derive it using Quantity and Price
--2: If price os zero or null, calculate it using Sales and Quantity
--3: If price is negative, convert it to a positive value
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price 
FROM Silver.crm_sales_details
WHERE sls_sales <> sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales,sls_quantity,sls_price

SELECT * FROM Silver.crm_sales_details

-- ======================================
-- Table 4 Quality Check crm_sales_details
-- ======================================
-- Because we will connet the table to the cust_info table with the cid key, we need to check if the key corresponds
SELECT 
CID
FROM Silver.erp_cust_az12
WHERE CID like '%AW00011000'

-- Identify Out of Range Dates
SELECT DISTINCT 
BDATE
FROM Silver.erp_cust_az12
WHERE BDATE < '1924-01-01' OR BDATE > GETDATE()

-- DATA STANDARDIZATION AND NORMALIZATION
SELECT DISTINCT GEN
FROM Silver.erp_cust_az12

SELECT * FROM Silver.erp_cust_az12 

 -- ======================================
-- Table 5 Quality Check erp_loc_a101
-- ======================================
SELECT 
cid,
cntry
FROM Silver.erp_loc_a101

SELECT
cst_key
FROM Silver.crm_cust_info

---- Data Standardization AND Consistency
SELECT DISTINCT cntry 
FROM Silver.erp_loc_a101
ORDER BY CNTRY

 -- ======================================
-- Table 6 Quality Check erp_px_cat_g1v2
-- ======================================
SELECT 
ID,
CAT,
SUBCAT,
MAINTAINANCE
FROM Silver.erp_px_cat_g1v2
WHERE MAINTAINANCE <> TRIM(MAINTAINANCE)

-- Data Standardization
SELECT DISTINCT 
MAINTAINANCE 
FROM Silver.erp_px_cat_g1v2

SELECT * FROM Silver.erp_px_cat_g1v2

-- Check for Null and duplicate values in Primary key
-- Expected result: No records should be returned
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING cst_id IS NULL OR COUNT(*) > 1;

-- Check for unwanted spaces 
-- Expected result: No records should be returned
SELECT cst_id, cst_firstname, cst_lastname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) OR cst_lastname != TRIM(cst_lastname);

-- Data standardization and consistency check for marital status and gender
-- Expected result: No records should be returned
SELECT cst_id, cst_marital_status, cst_gndr
FROM silver.crm_cust_info
WHERE cst_marital_status NOT IN ('Married', 'Single', 'n/a') 
   OR cst_gndr NOT IN ('Female', 'Male', 'n/a');




-- Check for Null and duplicate values in Primary key
-- Expected result: No records should be returned
SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING prd_id IS NULL OR COUNT(*) > 1;

-- Check for unwanted spaces
-- Expected result: No records should be returned
SELECT prd_id, prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Data standardization and consistency check for product line
-- Expected result: No records should be returned
SELECT prd_id, prd_line
FROM silver.crm_prd_info
WHERE prd_line NOT IN ('Mountain', 'Road', 'Touring', 'other Sales', 'n/a');

-- Check for invalid date orders
-- Expected result: No records should be returned
SELECT prd_id, prd_start_dt, prd_end_dt
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;
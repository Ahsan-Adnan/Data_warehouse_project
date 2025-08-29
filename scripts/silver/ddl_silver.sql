IF OBJECT_ID ('silver.crm_cust_info','U') IS NOT NULL
DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()

);
IF OBJECT_ID ('silver.crm_prd_info','U') IS NOT NULL
DROP TABLE silver.crm_prd_info;
GO
CREATE TABLE silver.crm_prd_info(
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
IF OBJECT_ID ('silver.crm_sales_details','U') IS NOT NULL
DROP TABLE silver.crm_sales_details;
GO
CREATE TABLE silver.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id NVARCHAR(50),
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.erp_CUST_AZ12','U') IS NOT NULL
DROP TABLE silver.erp_CUST_AZ12

CREATE TABLE silver.erp_CUST_AZ12(
	CID NVARCHAR(50),
	BDATE DATETIME,
	GEN NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
IF OBJECT_ID ('silver.erp_LOC_A101','U') IS NOT NULL
DROP TABLE silver.erp_LOC_A101;
GO
CREATE TABLE silver.erp_LOC_A101(
	CID NVARCHAR(50),
	CNTRY NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
IF OBJECT_ID ('silver.erp_PX_CAT_G1V2','U') IS NOT NULL
DROP TABLE silver.erp_PX_CAT_G1V2;
GO
CREATE TABLE silver.erp_PX_CAT_G1V2(
	ID NVARCHAR(50),
	CAT NVARCHAR(50),
	SUBCAT NVARCHAR(50),
	MAINTENANCE NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);


-- To change any data type , we need to first drop that table

--IF OBJECT_ID ('silver.crm_cust_info','U') IS NOT NULL
	--DROP TABLE silver.crm_cust_info


	

CREATE OR ALTER PROCEDURE silver.load_bronze AS
BEGIN
DECLARE @start_time DATETIME,@end_time DATETIME
BEGIN TRY
 PRINT  '================================================'
 PRINT 'Loading Bronze Layer'
 PRINT '================================================='


 SET @start_time = GETDATE();
 PRINT 'Truncating Table:silver.crm_cust_info'
 PRINT 'Inserting Data Into:silver.crm_cust_info'

TRUNCATE TABLE silver.crm_cust_info;
BULK INSERT silver.crm_cust_info
FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	
	
);
SET @end_time = GETDATE();
PRINT '>> Load Duration :' + CAST(DATEDIFF (SECOND,@start_time,@end_time) AS NVARCHAR) + 'Seconds';
PRINT 'Truncating Table:silver.crm_prd_info'
 PRINT 'Inserting Data Into:silver.crm_prd_info'


 SET @start_time = GETDATE();
TRUNCATE TABLE silver.crm_prd_info;
BULK INSERT silver.crm_prd_info
FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	
	
);
SET @end_time = GETDATE();
PRINT '>> Load Duration :' + CAST(DATEDIFF (SECOND,@start_time,@end_time) AS NVARCHAR) + 'Seconds';


SET @start_time = GETDATE();
PRINT 'Truncating Table:silver.crm_sales_details'
 PRINT 'Inserting Data Into:silver.crm_sales_details'

TRUNCATE TABLE silver.crm_sales_details;
BULK INSERT silver.crm_sales_details
FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	
	
);
SET @end_time = GETDATE();
PRINT '>> Load Duration :' + CAST(DATEDIFF (SECOND,@start_time,@end_time) AS NVARCHAR) + 'Seconds';

PRINT 'Truncating Table:silver.erp_CUST_AZ12'
 PRINT 'Inserting Data Into:silver.erp_CUST_AZ12'

SET @start_time = GETDATE();
TRUNCATE TABLE silver.erp_CUST_AZ12;
BULK INSERT silver.erp_CUST_AZ12
FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'

WITH (
	FIRSTROW = 2,
	
	FIELDTERMINATOR = ',',
	TABLOCK
	
);
SET @end_time = GETDATE();
PRINT '>> Load Duration :' + CAST(DATEDIFF (SECOND,@start_time,@end_time) AS NVARCHAR) + 'Seconds';


SET @start_time = GETDATE();
PRINT 'Truncating Table:silver.erp_LOC_A101'
 PRINT 'Inserting Data Into:silver.erp_LOC_A101'

TRUNCATE TABLE silver.erp_LOC_A101;
BULK INSERT silver.erp_LOC_A101
FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'

WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	
);
SET @end_time = GETDATE();
PRINT '>> Load Duration :' + CAST(DATEDIFF (SECOND,@start_time,@end_time) AS NVARCHAR) + 'Seconds';

SET @start_time = GETDATE();
PRINT 'Truncating Table:silver.erp_PX_CAT_G1V2'
 

TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
PRINT 'Inserting Data Into:silver.erp_PX_CAT_G1V2'
BULK INSERT silver.erp_PX_CAT_G1V2
FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'

WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	
);
SET @end_time= GETDATE()
PRINT '>> Load Duration :' + CAST(DATEDIFF (SECOND,@start_time,@end_time) AS NVARCHAR) + 'Seconds';
END TRY
BEGIN CATCH
	PRINT '========================================================='
	PRINT 'ERROR ORCCURED DURING LOADING BRONZE LAYER'
	PRINT 'Error Message ' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '========================================================='
END CATCH

END

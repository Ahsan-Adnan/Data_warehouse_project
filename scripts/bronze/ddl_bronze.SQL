IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE

);

CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start DATE,
	prd_end_dt DATE
);
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id NVARCHAR(50),
	sls_order_dt NVARCHAR(50),
	sls_ship_dt NVARCHAR(50),
	sls_due_dt NVARCHAR(50),
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

IF OBJECT_ID ('bronze.erp_CUST_AZ12','U') IS NOT NULL
DROP TABLE bronze.erp_CUST_AZ12

CREATE TABLE bronze.erp_CUST_AZ12(
	CID NVARCHAR(50),
	BDATE DATETIME,
	GEN NVARCHAR(50)
);
CREATE TABLE bronze.erp_LOC_A101(
	CID NVARCHAR(50),
	CNTRY NVARCHAR(50)
);
CREATE TABLE bronze.erp_PX_CAT_G1V2(
	ID NVARCHAR(50),
	CAT NVARCHAR(50),
	SUBCAT NVARCHAR(50),
	MAINTENANCE NVARCHAR(50)
);


-- To change any data type , we need to first drop that table

--IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
	--DROP TABLE bronze.crm_cust_info


	

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @start_time DATETIME,@end_time DATETIME
BEGIN TRY
 PRINT  '================================================'
 PRINT 'Loading Bronze Layer'
 PRINT '================================================='


 SET @start_time = GETDATE();
 PRINT 'Truncating Table:bronze.crm_cust_info'
 PRINT 'Inserting Data Into:bronze.crm_cust_info'

TRUNCATE TABLE bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	
	
);
SET @end_time = GETDATE();
PRINT '>> Load Duration :' + CAST(DATEDIFF (SECOND,@start_time,@end_time) AS NVARCHAR) + 'Seconds';
PRINT 'Truncating Table:bronze.crm_prd_info'
 PRINT 'Inserting Data Into:bronze.crm_prd_info'


 SET @start_time = GETDATE();
TRUNCATE TABLE bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	
	
);
SET @end_time = GETDATE();
PRINT '>> Load Duration :' + CAST(DATEDIFF (SECOND,@start_time,@end_time) AS NVARCHAR) + 'Seconds';


SET @start_time = GETDATE();
PRINT 'Truncating Table:bronze.crm_sales_details'
 PRINT 'Inserting Data Into:bronze.crm_sales_details'

TRUNCATE TABLE bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	
	
);
SET @end_time = GETDATE();
PRINT '>> Load Duration :' + CAST(DATEDIFF (SECOND,@start_time,@end_time) AS NVARCHAR) + 'Seconds';

PRINT 'Truncating Table:bronze.erp_CUST_AZ12'
 PRINT 'Inserting Data Into:bronze.erp_CUST_AZ12'

SET @start_time = GETDATE();
TRUNCATE TABLE bronze.erp_CUST_AZ12;
BULK INSERT bronze.erp_CUST_AZ12
FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'

WITH (
	FIRSTROW = 2,
	
	FIELDTERMINATOR = ',',
	TABLOCK
	
);
SET @end_time = GETDATE();
PRINT '>> Load Duration :' + CAST(DATEDIFF (SECOND,@start_time,@end_time) AS NVARCHAR) + 'Seconds';


SET @start_time = GETDATE();
PRINT 'Truncating Table:bronze.erp_LOC_A101'
 PRINT 'Inserting Data Into:bronze.erp_LOC_A101'

TRUNCATE TABLE bronze.erp_LOC_A101;
BULK INSERT bronze.erp_LOC_A101
FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'

WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	
);
SET @end_time = GETDATE();
PRINT '>> Load Duration :' + CAST(DATEDIFF (SECOND,@start_time,@end_time) AS NVARCHAR) + 'Seconds';

SET @start_time = GETDATE();
PRINT 'Truncating Table:bronze.erp_PX_CAT_G1V2'
 

TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
PRINT 'Inserting Data Into:bronze.erp_PX_CAT_G1V2'
BULK INSERT bronze.erp_PX_CAT_G1V2
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

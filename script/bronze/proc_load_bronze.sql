*/
  ==============================================================================
  Stored Procedure: load bronze layer(Source -> Bronze)
  ==============================================================================
  Script Purpose:
      This stored procedure loads data into the"bronze" schema from external CSV files.
      It performs the following actions: 
      -- Truncates the bronze tables befor loading data.
      -- Uses the 'BULK INSERT' command to load data from CSV files to bronze tables.
  
  Parameters:
      None.
      This stored procedure does not accept any parameter or return any values.
  
  Usage Example:
      EXEC bronze.load_bronze;
==================================================================================
  */
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME ,@end_time DATETIME,@batch_start DATETIME,@batch_end_time DATETIME;
	BEGIN TRY

		SET @batch_start = GETDATE();
		PRINT('=====================================================');
		PRINT('LOADING BRONZE LAYER');
		PRINT('=====================================================');
		PRINT '-----------------------------------------------------';
		PRINT'LOADING CRM TABLES';
		PRINT'------------------------------------------------------';
		
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.crm_cust_info' ;

	TRUNCATE TABLE bronze.crm_cust_info; 
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\vijay\Downloads\sqlprojectmaterial\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH(
		FIRSTROW =2,
		FIELDTERMINATOR =',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT'>>LOAD DURATION :' +CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	print'============================================================================================';
		
		SET @start_time = GETDATE();
		PRINT'>>  TRUNCATING TABLE:bronze.crm_prd_info';

	TRUNCATE TABLE bronze.crm_prd_info; 
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Users\vijay\Downloads\sqlprojectmaterial\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH(
		FIRSTROW =2,
		FIELDTERMINATOR =',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT'>>LOAD DURATION :' +CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	print'============================================================================================';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE :  bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details; 
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\vijay\Downloads\sqlprojectmaterial\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH(
		FIRSTROW =2,
		FIELDTERMINATOR =',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT'>>LOAD DURATION :' +CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	print'============================================================================================';
		PRINT'----------------------------------------------------------------------------------------------------';
		PRINT'LOADING ERP TABLES';
		SET @start_time = GETDATE();
		PRINT'----------------------------------------------------------------------------------------------------';
		PRINT'TRUNCATE TABLE:: bronze.erp_CUST_AZ12';
		PRINT'====================================================================================================';


	TRUNCATE TABLE bronze.erp_CUST_AZ12; 
	BULK INSERT bronze.erp_CUST_AZ12
	FROM 'C:\Users\vijay\Downloads\sqlprojectmaterial\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH(
		FIRSTROW =2,
		FIELDTERMINATOR =',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT'>>LOAD DURATION :' +CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';

	print'============================================================================================';
		SET @start_time = GETDATE();
		PRINT'TRUNCATE TABLE bronze.erp_LOC_A101';
	TRUNCATE TABLE bronze.erp_LOC_A101; 
	BULK INSERT bronze.erp_LOC_A101
	FROM 'C:\Users\vijay\Downloads\sqlprojectmaterial\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	WITH(
		FIRSTROW =2,
		FIELDTERMINATOR =',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT'>>LOAD DURATION :' +CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	print'============================================================================================';
		SET @start_time = GETDATE();
		PRINT'TRUNCATE TABLE: bronze.erp_PX_CAT_G1V2';
	TRUNCATE TABLE bronze.erp_PX_CAT_G1V2; 

	BULK INSERT bronze.erp_PX_CAT_G1V2
	FROM 'C:\Users\vijay\Downloads\sqlprojectmaterial\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH(
		FIRSTROW =2,
		FIELDTERMINATOR =',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT'>>LOAD DURATION :' +CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	SET @batch_end_time = GETDATE();
	PRINT'BRONZE LAYER IS COMPLETED';
	PRINT'TOTAL LOAD DURATION:' +CAST(DATEDIFF(SECOND,@batch_start,@batch_end_time) AS NVARCHAR) +'seconds';

	print'============================================================================================';
	END TRY
	BEGIN CATCH
		PRINT'========================================';
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT'ERROR MESSAGE'+ ERROR_MESSAGE();
		PRINT'ERROR MESSAGE'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'ERROR MESSAGE'+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'========================================';
	END CATCH
END








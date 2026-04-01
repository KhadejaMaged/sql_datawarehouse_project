

create or alter procedure bronze.load_bronze as
begin
	Declare @start_time DATETIME , @end_time DATETIME;
	begin try
		print('=======================================================================');
		print('Loading Bronze Layer');
		print('=======================================================================');
		print('=======================================================================');
		print('Loading CRM Tables');
		print('=======================================================================');
		set @start_time=GETDATE();
		print 'Truncating Table >> bronze.crm_cust_info ';
		Truncate Table bronze.crm_cust_info
		print 'Inserting Data into Table >> bronze.crm_cust_info ';
		Bulk insert  bronze.crm_cust_info
		from 'D:\Data_enginnering\Datawarehouseproject\source_crm\cust_info.csv'
		WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
		);
		set @end_time=GETDATE();
		print('Load duration' + cast (datediff(second, @start_time,@end_time)  as nvarchar ) + 'seconds') 
		print('---------------------------------------------------------------------------------------------')
		set @start_time=GETDATE();
		print 'Truncating Table >> bronze.crm_prd_info';
		Truncate Table bronze.crm_prd_info
		print 'Inserting Data into Table >> bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'D:\Data_enginnering\Datawarehouseproject\source_crm\prd_info.csv'
		WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
		);
		set @end_time=GETDATE();
		print('Load duration' + cast (datediff(second, @start_time,@end_time)  as nvarchar ) + 'seconds') 
		print('---------------------------------------------------------------------------------------------')
		set @start_time=GETDATE();
		print 'Truncating Table >> bronze.crm_sales_details';
		Truncate Table bronze.crm_sales_details
		print 'Inserting Data into Table >> bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'D:\Data_enginnering\Datawarehouseproject\source_crm\sales_details.csv'
		with 
		(
			Firstrow=2,
			FIELDTERMINATOR =',',
			tablock
		);
		set @end_time=GETDATE();
		print('Load duration' + cast (datediff(second, @start_time,@end_time)  as nvarchar ) + 'seconds') 
		print('---------------------------------------------------------------------------------------------')
		print('=======================================================================')
		print('Loading ERP Table')
		print('=======================================================================')
		set @start_time=GETDATE();

		print 'Truncating Table >> bronze.erp_cust_az12';
		Truncate Table bronze.erp_cust_az12
		print 'Inserting Data into Table >> bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'D:\Data_enginnering\Datawarehouseproject\source_erp\CUST_AZ12.csv'
		with 
		(
			Firstrow=2,
			FIELDTERMINATOR =',',
			tablock
		);
		set @end_time=GETDATE();
		print('Load duration' + cast (datediff(second, @start_time,@end_time)  as nvarchar ) + 'seconds') 
		print('---------------------------------------------------------------------------------------------')
		set @start_time=GETDATE();
		print 'Truncating Table >>bronze.erp_loc_a101';
		Truncate Table bronze.erp_loc_a101
		print 'Inserting Data into Table >>bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'D:\Data_enginnering\Datawarehouseproject\source_erp\LOC_A101.csv'
		with 
		(
			Firstrow=2,
			FIELDTERMINATOR =',',
			tablock
		);
		set @end_time=GETDATE();
		print('Load duration' + cast (datediff(second, @start_time,@end_time)  as nvarchar ) + 'seconds') 
		print('---------------------------------------------------------------------------------------------')
		set @start_time=GETDATE();
		print 'Truncating Table >> bronze.erp_px_cat_g1v2';
		Truncate Table bronze.erp_px_cat_g1v2
		print 'Inserting Data into Table >> bronze.erp_px_cat_g1v2';

		bulk insert bronze.erp_px_cat_g1v2
		from 'D:\Data_enginnering\Datawarehouseproject\source_erp\PX_CAT_G1V2.csv'
		with 
		(
			Firstrow=2,
			FIELDTERMINATOR =',',
			tablock
		);
		set @end_time=GETDATE();
		print('Load duration' + cast (datediff(second, @start_time,@end_time)  as nvarchar ) + 'seconds') 
		print('---------------------------------------------------------------------------------------------')
	End try
	Begin catch 
		print('========================================================');
		print('ERROR OCCURED DURING BRONZE LAYER');
		print('ERROR MESSEGE ' + ERROR_MESSAGE());
		print('ERROR MESSEGE ' + ERROR_MESSAGE());
		print('ERROR MESSEGE ' + cast(ERROR_NUMBER() as nvarchar));
		print('ERROR MESSEGE ' + cast(ERROR_STATE() as nvarchar));
		print('========================================================');

	End catch 
End 

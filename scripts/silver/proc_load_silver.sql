CREATE OR ALTER PROCEDURE silver.load_silver AS
Begin
	declare @start_time datetime , @end_time datetime , @batch_start_time datetime , @batch_end_time datetime;
	begin try 
		set @batch_start_time = getdate()
		PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';
		set @start_time = getdate()
		print 'Truncating crm_cust_info';
		truncate table silver.crm_cust_info
		print 'Inserting crm_cust_info'
		Insert into silver.crm_cust_info
		(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname ,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)
		select
		cst_id ,
		cst_key , 
		trim(cst_firstname),
		trim(cst_lastname) ,
		case when upper(trim(cst_marital_status)) = 'S' then 'Single'
			when  upper(trim(cst_marital_status)) = 'M' then 'Married'
			else 'n/a'
		end as cst_marital_status,


		Case When upper(cst_gndr) ='F' then 'female'
			 When upper(cst_gndr) ='M' then 'Male'
			 Else 'n/a'
		End as cst_gndr ,
		cst_create_date
		from
		(
		select * ,
		ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as rnk
		from bronze.crm_cust_info
		WHERE cst_id IS NOT NULL

		) t
		where rnk =1
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
	    -- Loading silver.crm_prd_info
        SET @start_time = GETDATE();
		print 'Truncating crm_prd_info'
		truncate table silver.crm_prd_info
		print 'Inserting crm_prd_info'
		Insert into silver.crm_prd_info
		(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		SELECT
					prd_id,
					REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
					SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Extract product key
					prd_nm,
					ISNULL(prd_cost, 0) AS prd_cost,
					CASE 
						WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
						WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
						WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
						WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
						ELSE 'n/a'
					END AS prd_line, -- Map product line codes to descriptive values
					CAST(prd_start_dt AS DATE) AS prd_start_dt,
					DATEADD(
			DAY,
			-1,
			LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
		) AS prd_end_dt -- Calculate end date as one day before the next start date
				FROM bronze.crm_prd_info;

		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading crm_sales_details
        SET @start_time = GETDATE();
		print 'Truncating crm_sales_details'
		truncate table silver.crm_sales_details
		print 'Inserting crm_sales_details'
		Insert into silver.crm_sales_details
		(
			sls_ord_num ,
			sls_prd_key  ,
			sls_cust_id ,
			sls_order_dt ,
			sls_ship_dt  ,
			sls_due_dt   ,
			sls_sales    ,
			sls_quantity ,
			sls_price    
		)

		select 
			sls_ord_num  ,
			sls_prd_key  ,
			sls_cust_id  ,
			case when sls_order_dt<0 or len(sls_order_dt) !=8 then Null 
			else  cast(cast(sls_order_dt as varchar) as date)
			end as sls_order_dt,
			case when sls_ship_dt<0 or len(sls_ship_dt) !=8 then Null 
			else  cast(cast(sls_ship_dt as varchar) as date)
			end as sls_ship_dt,
				case when sls_due_dt<0 or len(sls_due_dt) !=8 then Null 
			else  cast(cast(sls_due_dt as varchar) as date)
			end as sls_due_dt,
			case when sls_sales < 0 or sls_sales is null or sls_sales != sls_quantity* abs(sls_price)
			then sls_quantity* abs(sls_price)
			else sls_sales
			end as sls_sales,
			sls_quantity ,
			case when sls_price is null or sls_price < 0 
			then sls_sales/ nullif(sls_quantity,0)
			ELSE sls_price 
			END AS sls_price 
			from 
			bronze.crm_sales_details


		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading erp_cust_az12
        SET @start_time = GETDATE();
		print 'Truncating erp_cust_az12'
		truncate table silver.erp_cust_az12
		print 'Inserting erp_cust_az12'
		insert into silver.erp_cust_az12(
		CID,
		BDATE,
		GEN

		)
		select
		case when cid like'NAS%' then SUBSTRING(cid,4,len(cid))  
		else cid 
		end as cid
		,
		case when bdate > getdate() then null 
		else BDATE
		end as BDATE
		,
		case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
			when upper(trim(gen)) in ('M','MALE')    then 'Male'
			else 'n/a'
		end as gen
		from 
		bronze.erp_cust_az12
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

        -- Loading erp_loc_a101
        SET @start_time = GETDATE();
		print 'Truncating erp_loc_a101'
		truncate  table silver.erp_loc_a101
		print 'Inserting erp_cust_az12'
		insert into silver.erp_loc_a101(
		cid,
		cntry
		)
		select 
		replace(cid,'-','') as cid,
		case when trim(cntry) = 'DE' then 'Germany'
			 when trim(cntry) in ('US','USA') then 'United state'
			 when trim(cntry) = ' '  or cntry is null then 'n/a'
			 else trim(cntry)
		end as cntry
		from bronze.erp_loc_a101
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		
		-- Loading erp_px_cat_g1v2
		SET @start_time = GETDATE();
		print 'Truncating erp_px_cat_g1v2'
		truncate  table silver.erp_px_cat_g1v2
		print 'Inserting erp_px_cat_g1v2'
		insert into silver.erp_px_cat_g1v2
		(
		id,
		cat ,
		subcat,
		maintenance

		)
		select 
		id ,
		cat , 
		subcat , 
		maintenance
		from
		bronze.erp_px_cat_g1v2
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	end try 
	begin catch
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	end catch

End 

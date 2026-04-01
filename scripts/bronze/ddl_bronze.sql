
If OBJECT_ID('bronze.crm_cust_info','U') is  Not Null
	Drop table bronze.crm_cust_info
create table bronze.crm_cust_info
(
	cst_id             Int,
	cst_key            Int,
	cst_firstname      nvarchar(50),
	cst_lastname       nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr           nvarchar(50),
	cst_create_date    Date
)

If OBJECT_ID('bronze.crm_prd_info','U') is  Not Null
	Drop table bronze.crm_prd_info
create table bronze.crm_prd_info
(
	prd_id        Int,
	prd_key       nvarchar(50),
	prd_nm        nvarchar(50),
	prd_cost      Int,
	prd_line      nvarchar(50),
	prd_start_dt  Date,
	prd_end_dt    Date
)
If OBJECT_ID('bronze.crm_sales_details','U') is  Not Null
	Drop table bronze.crm_sales_details
create table bronze.crm_sales_details
(
	sls_ord_num  nvarchar(50),
	sls_prd_key  nvarchar(50),
	sls_cust_id  INT,
	sls_order_dt INT,
	sls_ship_dt  INT,
	sls_due_dt   INT,
	sls_sales    INT,
	sls_quantity INT,
	sls_price    INT
)
If OBJECT_ID('bronze.erp_cust_az12','U') is  Not Null
	Drop table bronze.erp_cust_az12
create table bronze.erp_cust_az12
(
	CID  nvarchar(50),
	BDATE Date,
	GEN   nvarchar(550)
)
If OBJECT_ID('bronze.erp_loc_a101','U') is  Not Null
	Drop table bronze.erp_loc_a101
create table bronze.erp_loc_a101
(
	CID	nvarchar(50),
	CNTRY nvarchar(50)
)
If OBJECT_ID('bronze.erp_px_cat_g1v2','U') is  Not Null
	Drop table bronze.erp_px_cat_g1v2
create table bronze.erp_px_cat_g1v2
(
	ID	        nvarchar(50),
	CAT         nvarchar(50),
	SUBCAT      nvarchar(50),
	MAINTENANCE nvarchar(50),
)

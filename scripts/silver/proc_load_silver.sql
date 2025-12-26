CREATE OR ALTER PROCEDURE silver.load_silver AS
-- WE created a procedure
-- named "load_silver" for loading the transformations inside it
BEGIN 
    declare @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
    BEGIN TRY 
        SET @batch_start_time=GETDATE();
        PRINT '================================================'
        PRINT 'Loading Silver Layer';
        PRINT '================================================'


        PRINT '-------------------------------------------------'
        PRINT 'Loading CRM Tables';
        PRINT '-------------------------------------------------'

        SET @start_time =GETDATE();
        PRINT '>>Truncating Table:silver.crm_cust_info '
        truncate table silver.crm_cust_info
        print '>>Inserting data into silver.crm_cust_info'
        INSERT INTO silver.crm_cust_info( -- we inserted clean data inside the silver table
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_material_status,
            cst_gndr,
            cst_create_date
        )


        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
    
            CASE WHEN UPPER(TRIM(cst_material_status))='S' THEN 'Single' -- TRIM juste in case spaces will appear later in your column , that's called data normalization or stadarization
                 WHEN UPPER(TRIM(cst_material_status))='M' THEN 'Married' -- UPPER same thing (small letters)
                 ELSE 'n/a' -- handlinig missing data
            END cst_material_status,
   
            CASE WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female' -- TRIM juste in case spaces will appear later in your column
                 WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male' -- UPPER same thing (small letters)
                 ELSE 'n/a'
            END cst_gndr,
            cst_create_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY cst_id
                    ORDER BY cst_create_date DESC
                ) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1
        SET @end_time=GETDATE();
        print '>> Load Duration :'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar)+'seconds'

        ---------------------------------------------------------------------------------------------------
        PRINT '>>Truncating Table:silver.crm_prd_info '
        SET @start_time =GETDATE();
        truncate table silver.crm_prd_info
        print '>>Inserting data into silver.crm_prd_info'
        INSERT INTO silver.crm_prd_info(
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
        REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,-- we created new column 
        SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
        prd_nm,
        ISNULL(prd_cost,0) AS prd_cost,
        CASE UPPER(TRIM(prd_line))
             WHEN 'M' THEN 'Mountain'
             WHEN 'R' THEN 'Road'
             WHEN 'S' THEN 'Other Sales'
             WHEN 'T' THEN 'Touring'
             ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt    
        FROM bronze.crm_prd_info
        SET @end_time=GETDATE();
        print '>> Load Duration :'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar)+'seconds'

        ----------------------------------------------------------------
        PRINT '>>Truncating Table:silver.crm_sales_details '
        SET @start_time =GETDATE();
        truncate table silver.crm_sales_details
        print '>>Inserting data into silver.crm_sales_details'
        insert into silver.crm_sales_details(
	        sls_ord_num,
	        sls_prd_key,
	        sls_cust_id,
	        sls_order_dt,
	        sls_ship_dt,
	        sls_due_dt,
	        sls_sales,
	        sls_quantity,
	        sls_price
        )
        select 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt=0 OR len(sls_order_dt)!=8 THEN NULL
             else cast(cast(sls_order_dt as varchar) as date) -- we can not convert directly from interger to date 
        end as sls_order_dt,
        CASE WHEN sls_ship_dt=0 OR len(sls_ship_dt)!=8 THEN NULL
             else cast(cast(sls_ship_dt as varchar) as date) -- sls_ship_dt is ok but juste for the futur 
        end as sls_ship_dt,
        CASE WHEN sls_due_dt=0 OR len(sls_due_dt)!=8 THEN NULL
             else cast(cast(sls_due_dt as varchar) as date) -- sls_ship_dt is ok but juste for the futur 
        end as sls_due_dt,
        CASE WHEN sls_sales is null or sls_sales<=0 or sls_sales !=sls_quantity * abs(sls_price)
		        then sls_quantity*abs(sls_price)
	        else sls_sales
        end as sls_sales,
        sls_quantity,
        case when sls_price is null or sls_price<=0
		        then sls_sales/NULLIF(sls_quantity,0)
	        else sls_price
        end as sls_price
        FROM bronze.crm_sales_details

        SET @end_time=GETDATE();
        print '>> Load Duration :'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar)+'seconds'
        --------------------------------------------
        PRINT '>>Truncating Table:silver.erp_cust_az12 '
        SET @start_time =GETDATE();
        truncate table silver.erp_cust_az12
        print '>>Inserting data into silver.erp_cust_az12'
        insert into silver.erp_cust_az12(cid,bdate,gen)
        select
        case when cid LIKE 'NAS%' THEN substring(cid,4,len(cid))
                else cid
        end as cid,
        case when bdate>GETDATE() THEN null
            else bdate
        end as bdate, 
        case when upper(trim(gen)) in ('F','FEMALE') THEN 'Female'
             when upper(trim(gen)) in ('M','MALE') THEN 'Male'
             ELSE 'n/a'
        end as gen
        from bronze.erp_cust_az12

        SET @end_time=GETDATE();
        print '>> Load Duration :'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar)+'seconds'
        -----------------------
        PRINT '>>Truncating Table:silver.erp_col_a101 '
        SET @start_time =GETDATE();
        truncate table silver.erp_loc_a101
        print '>>Inserting data into silver.erp_loc_a101'
        insert into silver.erp_loc_a101
        (cid,cntry)
        select 
        replace(cid,'-','') cid,
        case when trim(cntry) ='DE' then 'GERMANY'
             when trim(cntry) in ('US','USA') THEN 'United States'
             when trim(cntry)='' or cntry is NULL then 'n/a'
             else trim(cntry)
        END AS cntry
        from bronze.erp_loc_a101

        SET @end_time=GETDATE();
        print '>> Load Duration :'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar)+'seconds'
        --------------------------------------------------
        PRINT '>>Truncating Table:silver.erp_px_cat_g1v2 '
        SET @start_time =GETDATE();
        truncate table silver.erp_px_cat_g1v2
        print '>>Inserting data into silver.erp_px_cat_g1v2'
        insert into silver.erp_px_cat_g1v2
        (id,cat,subcat,maintenance) 
        select id,cat,subcat,maintenance from bronze.erp_px_cat_g1v2
        SET @end_time=GETDATE();
        SET @batch_end_time=GETDATE();
        print '>> Load Duration :'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar)+'seconds'
        PRINT '+++++++++++++++++++++++++++++++++++++'
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '   -Total Load Durarion: '+CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time)AS NVARCHAR)+'seconds'
		PRINT '++++++++++++++++++++++++++++++++++++++'
    end try
    begin catch
        print '================================================'
        print 'error occured during loading bronze layer'
        print 'Error message'+ERROR_MESSAGE();
        PRINT 'Error Message'+CAST (ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message'+CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================='
    end catch
  END






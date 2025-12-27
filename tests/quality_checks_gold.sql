use DataWareHouse
select distinct -- checking integration
	ci.cst_gndr,
	ca.gen,
	case when ci.cst_gndr!='n/a' then ci.cst_gndr -- crm is the master for gender info 
		else coalesce(ca.gen,'n/a') --COALESCE retourne la première valeur non nulle de la liste.
	end as new_gen
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key=la.cid
order by 1,2
-- null in gen column comes from join that means there are customers in the crm tables
-- that are not available in the erp table 
-------------------------------
create view gold.dim_products as 
 select 
	 row_number() over (order by pn.prd_start_date,pn.prd_key) as product_key,--we will be using it to connect our data model
	 pn.prd_id as product_id,
	 pn.prd_key as product_number,
	 pn.prd_nm as product_name,
	 pn.cat_id as category_id,
	 pc.cat as category,
	 pc.subcat as subcategory,
	 pc.maintenance,
	 pn.prd_cost as cost,
	 pn.prd_line as product_line,
	 pn.prd_start_dt as start_date
 --pn.prd_end_dt : not necessery now with the filter criteria
 from silver.crm_prd_info pn
 left join silver.erp_px_cat_g1v2 pc
 on pn.cat_id=pc.id
 where prd_end_dt is null--for selecting only the current infos 
 --------------------------------

 select * from gold.dim_products
 -----------------------
 select * from silver.crm_sales_details

 select * from gold.dim_customers
 select * from gold.fact_sales



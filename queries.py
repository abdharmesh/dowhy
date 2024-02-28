def generate_total_data_query(cond_str):
    total_data_query = f'''

            with  inv_base as (
                select date(updated_at) as date , sku , store , min(available) as avail
                from inventory
                group by 1,2,3
            ) ,

            base as (
                    select a.date , a.store , (case when a.source_name = 'pos' then 'pos' else 'online' end) as source_name , a.id , a.sku , a.item_selling_price as usp , a.quantity as qty,
                    (case when i.avail is null then 1 when i.avail <= 0 then 0 else 1 end) as avail
                    from order_item a
                    left join inv_base i on a.date = i.date and a.store = i.store and a.sku = i.sku
                    where a.date >= '2023-01-01' and a.date <= '2024-01-31'
                    and a.product_type IN ('Personal & Home','Grains & Flour','Spices & Condiments','Instant Food & Beverages')
                    group by 1,2,3,4,5,6,7,8
                    
                    union all
                    
                    select a.date ,'ALL' as store , (case when a.source_name = 'pos' then 'pos' else 'online' end) as source_name , a.id , a.sku , a.item_selling_price as usp , a.quantity as qty,
                    (case when i.avail is null then 1 when i.avail <= 0 then 0 else 1 end) as avail
                    from order_item a
                    left join inv_base i on a.date = i.date and a.store = i.store and a.sku = i.sku
                    where a.date >= '2023-01-01' 
                    and a.product_type IN ('Personal & Home','Grains & Flour','Spices & Condiments','Instant Food & Beverages')
                    group by 1,2,3,4,5,6,7,8
                    
                    union all
                    
                    select a.date ,a.store , 'ALL' as source_name , a.id , a.sku , a.item_selling_price as usp , a.quantity as qty,
                    (case when i.avail is null then 1 when i.avail <= 0 then 0 else 1 end) as avail
                    from order_item a
                    left join inv_base i on a.date = i.date and a.store = i.store and a.sku = i.sku
                    where a.date >= '2023-01-01' 
                    and a.product_type IN ('Personal & Home','Grains & Flour','Spices & Condiments','Instant Food & Beverages')
                    group by 1,2,3,4,5,6,7,8
                    
                    union all
                    
                    select a.date ,'ALL' as store , 'ALL' as source_name , a.id , a.sku , a.item_selling_price as usp , a.quantity as qty,
                    (case when i.avail is null then 1 when i.avail <= 0 then 0 else 1 end) as avail
                    from order_item a
                    left join inv_base i on a.date = i.date and a.store = i.store and a.sku = i.sku
                    where a.date >= '2023-01-01' 
                    and a.product_type IN ('Personal & Home','Grains & Flour','Spices & Condiments','Instant Food & Beverages')
                    group by 1,2,3,4,5,6,7,8      
            ) ,

            weight_avail as (
                select date , store , sum(qty*avail)/sum(qty) as wth_avail
                from base
                group by 1,2
            ),

            aov_base as
            (
                select date, store, source_name, id, sum(qty*usp::float) as gmv
                from base
                group by 1,2,3,4
            ),



            aov as
            (
                select date, store, source_name, avg(gmv::float*1.00) as aov
                from aov_base
                group by 1,2,3
            ),


            unique_sku as
            (
                select date, store, (case when source_name = 'pos' then 'pos' else 'online' end) as source_name, id, count(distinct sku) as unique_products
                from order_item a
                where a.date >= '2023-01-01' 
                and product_type IN ('Personal & Home','Grains & Flour','Spices & Condiments','Instant Food & Beverages')
                group by 1,2,3,4
                
                union all
                
                select date,'ALL' as store, (case when source_name = 'pos' then 'pos' else 'online' end) as source_name, id, count(distinct sku) as unique_products
                from order_item a
                where a.date >= '2023-01-01' 
                and product_type IN ('Personal & Home','Grains & Flour','Spices & Condiments','Instant Food & Beverages')
                group by 1,2,3,4
                
                union all
                
                select date, store,'ALL' as source_name, id, count(distinct sku) as unique_products
                from order_item a
                where a.date >= '2023-01-01' 
                and product_type IN ('Personal & Home','Grains & Flour','Spices & Condiments','Instant Food & Beverages')
                group by 1,2,3,4
                
                union all
                
                select date,'ALL' as store, 'ALL' as source_name, id, count(distinct sku) as unique_products
                from order_item a
                where a.date >= '2023-01-01' 
                and product_type IN ('Personal & Home','Grains & Flour','Spices & Condiments','Instant Food & Beverages')
                group by 1,2,3,4   
            ),

            avg_unique_sku as
            (
                select date, store, source_name, avg(unique_products*1.00) as avg_unique_products
                from unique_sku
                group by 1,2,3
            ),

            pre_final as
            (
                select a.date, a.store, (case when a.source_name = 'pos' then 'pos' else 'online' end) as source_name ,
                count(distinct a.email) as customers,
                count(distinct a.id) as orders,
                sum(a.quantity*a.item_selling_price::float) as gmv ,
                sum(a.quantity) as quantity ,
                sum(a.quantity)*1.00/count(distinct a.id) as ipc ,
                sum(case when product_type = 'Dookan Bistro' then quantity end) as bistro_sales ,
                sum(case when product_type = 'Frozen Foods' then quantity end) as frozen_sales ,
                sum(case when product_type = 'Fruits & Veggies' then quantity end) as fnv_sales ,
                sum(case when product_type = 'Grains & Flour' then quantity end) as grain_flour_sales ,
                sum(case when product_type = 'Instant Food & Beverages' then quantity end) as instant_sales ,
                sum(case when product_type = 'Personal & Home' then quantity end) as personal_home_sales ,
                sum(case when product_type = 'Spices & Condiments' then quantity end) as spices_sales
                from order_item a
                where a.date >= '2023-01-01'
                and product_type IN ('Personal & Home','Grains & Flour','Spices & Condiments','Instant Food & Beverages')
                group by 1,2,3
                
                union all
                
                select a.date, 'ALL' as store, (case when a.source_name = 'pos' then 'pos' else 'online' end) as source_name ,
                count(distinct a.email) as customers,
                count(distinct a.id) as orders,
                sum(a.quantity*a.item_selling_price::float) as gmv ,
                sum(a.quantity) as quantity ,
                sum(a.quantity)*1.00/count(distinct a.id) as ipc ,
                sum(case when product_type = 'Dookan Bistro' then quantity end) as bistro_sales ,
                sum(case when product_type = 'Frozen Foods' then quantity end) as frozen_sales ,
                sum(case when product_type = 'Fruits & Veggies' then quantity end) as fnv_sales ,
                sum(case when product_type = 'Grains & Flour' then quantity end) as grain_flour_sales ,
                sum(case when product_type = 'Instant Food & Beverages' then quantity end) as instant_sales ,
                sum(case when product_type = 'Personal & Home' then quantity end) as personal_home_sales ,
                sum(case when product_type = 'Spices & Condiments' then quantity end) as spices_sales
                from order_item a
                where a.date >= '2023-01-01'
                and product_type IN ('Personal & Home','Grains & Flour','Spices & Condiments','Instant Food & Beverages')
                group by 1,2,3
                
                union all
                
                select a.date, a.store, 'ALL' as source_name ,
                count(distinct a.email) as customers,
                count(distinct a.id) as orders,
                sum(a.quantity*a.item_selling_price::float) as gmv ,
                sum(a.quantity) as quantity ,
                sum(a.quantity)*1.00/count(distinct a.id) as ipc ,
                sum(case when product_type = 'Dookan Bistro' then quantity end) as bistro_sales ,
                sum(case when product_type = 'Frozen Foods' then quantity end) as frozen_sales ,
                sum(case when product_type = 'Fruits & Veggies' then quantity end) as fnv_sales ,
                sum(case when product_type = 'Grains & Flour' then quantity end) as grain_flour_sales ,
                sum(case when product_type = 'Instant Food & Beverages' then quantity end) as instant_sales ,
                sum(case when product_type = 'Personal & Home' then quantity end) as personal_home_sales ,
                sum(case when product_type = 'Spices & Condiments' then quantity end) as spices_sales
                from order_item a
                where a.date >= '2023-01-01'
                and product_type IN ('Personal & Home','Grains & Flour','Spices & Condiments','Instant Food & Beverages')
                group by 1,2,3
                
                union all
                
                select a.date, 'ALL' as store, 'ALL' as source_name ,
                count(distinct a.email) as customers,
                count(distinct a.id) as orders,
                sum(a.quantity*a.item_selling_price::float) as gmv ,
                sum(a.quantity) as quantity ,
                sum(a.quantity)*1.00/count(distinct a.id) as ipc ,
                sum(case when product_type = 'Dookan Bistro' then quantity end) as bistro_sales ,
                sum(case when product_type = 'Frozen Foods' then quantity end) as frozen_sales ,
                sum(case when product_type = 'Fruits & Veggies' then quantity end) as fnv_sales ,
                sum(case when product_type = 'Grains & Flour' then quantity end) as grain_flour_sales ,
                sum(case when product_type = 'Instant Food & Beverages' then quantity end) as instant_sales ,
                sum(case when product_type = 'Personal & Home' then quantity end) as personal_home_sales ,
                sum(case when product_type = 'Spices & Condiments' then quantity end) as spices_sales
                from order_item a
                where a.date >= '2023-01-01'
                and product_type IN ('Personal & Home','Grains & Flour','Spices & Condiments','Instant Food & Beverages')
                group by 1,2,3
                
                
            )

            select a.date, a.store, a.source_name, a.customers, a.orders,a.gmv, a.ipc, b.avg_unique_products as unique_skus, c.aov , a.quantity ,
            a.bistro_sales , a.frozen_sales , a.fnv_sales ,a.grain_flour_sales , a.instant_sales , a.personal_home_sales , a.spices_sales , d.wth_avail
            from pre_final a
                left join avg_unique_sku b on a.date = b.date and a.store = b.store and a.source_name = b.source_name
                left join aov c on a.date = c.date and a.store = c.store and a.source_name = c.source_name
                join weight_avail d on a.date = d.date and a.store = d.store
            where {cond_str}

            '''
    return total_data_query
#1
#create a hive meta store database named problem6 and import all tables from mysql retail_db database into hive meta store.

sqoop import-all-tables \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user --password itversity \
--autoreset-to-one-mapper --as-textfile \
--warehouse-dir /user/hive/warehouse/iskibongue_problem6.db \
--hive-import --hive-database iskibongue_problem6 \
--create-hive-table

sqoop import-all-tables \
--connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
--username retail_user \
--password itversity \
--hive-import \
--hive-database iskibongue_problem6 \
--create-hive-table \
--hive-overwrite \
--as-textfile

retail_db - MLD
+---------------------+
| Tables_in_retail_db |
+---------------------+
| categories          |
| customers           |
| departments         |
| order_items         |
| order_items_nopk    |
| orders              |
| products            |
+---------------------+

sqoop import \
--connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
--username retail_user \
--password itversity \
--table products \
--m 1 \
--hive-import \
--hive-database iskibongue_problem6 \
--create-hive-table \
--hive-overwrite \
--as-textfile

# 2 - On spark shell use data available on meta store as source and perform step 3,4,5 and 6. [this proves your ability to use meta store as a source]
# 3 - Rank products within department by price and order by department ascending and rank descending 
#     [this proves you can produce ranked and sorted data on joined data sets]

select 
  dep.department_name,
  pdt.product_name,
  pdt.product_price,
  rank() over (partition by dep.department_id order by pdt.product_price) as pdt_price_rank
from
  categories join departments dep on dep.department_id = categories.category_department_id
  inner join products pdt on pdt.product_category_id = categories.category_id
order by department_name ASC, pdt_price_rank DESC;

# 4 - find top 10 customers with most unique product purchases. if more than one customer has the same number of product purchases
#     then the customer with the lowest customer_id will take precedence [this proves you can produce aggregate statistics on joined datasets]
select 
  c.customer_id,
  concat(c.customer_fname,' ',c.customer_lname) c_name,
  count(distinct(oi.order_item_product_id)) most_u
from
  customers c join orders o on o.order_customer_id = c.customer_id
  join order_items oi on oi.order_item_order_id = o.order_id 
  GROUP BY c.customer_id, c.customer_fname, c.customer_lname
  order by most_u DESC, c.customer_id limit 10;

# 5 - On dataset from step 3, apply filter such that only products less than 100 are extracted 
#     [this proves you can use subqueries and also filter data]

select res.* from (select 
  dep.department_name,
  pdt.product_name,
  pdt.product_price,
  rank() over (partition by dep.department_id order by pdt.product_price) as pdt_price_rank
from
  categories join departments dep on dep.department_id = categories.category_department_id
  inner join products pdt on pdt.product_category_id = categories.category_id
order by department_name ASC, pdt_price_rank DESC) as res where res.product_price < 100;

# 6 - On dataset from step 4, extract details of products purchased 
#     by top 10 customers which are priced at less than 100 USD per unit [this proves you can use subqueries and also filter data]

select 
  c.customer_id,
  concat(c.customer_fname,' ',c.customer_lname) c_name,
  count(distinct(oi.order_item_product_id)) most_u
from
  customers c join orders o on o.order_customer_id = c.customer_id
  join order_items oi on oi.order_item_order_id = o.order_id
  GROUP BY c.customer_id, c.customer_fname, c.customer_lname
  order by most_u DESC, c.customer_id limit 10;

select 
    min(f.order_customer_id), 
    f.orderNbe 
from (
	    select 
	       order_customer_id, 
	       count(order_id) orderNbe 
	    from 
	       iskibongue_jay__mock_orders 
	    group by order_customer_id 
	    order by orderNbe DESC
	 ) 
f group by f.orderNbe order by f.orderNbe desc;
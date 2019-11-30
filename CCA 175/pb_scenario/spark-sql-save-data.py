from pyspark.sql import Row 

pdtsDF = sc.textFile("/user/iskibongue/retail_db/products"). \
map(lambda o : Row(product_id=int(o.split(",")[0]), \
product_category_id=int(o.split(",")[1]),product_name=o.split(",")[2], \
product_description=o.split(",")[3],product_price=o.split(",")[4], \
prodcut_image=o.split(",")[5])).toDF() 

sqlContext.sql("use iskibongue_retail_db_txt")

pdtsDF.registerTempTable("tempProducts")

sqlRes = sqlContext.sql("select p.product_name,o.order_date,round(SUM(oi.order_item_subtotal),2) revenue \
from order_items oi join orders o on oi.order_item_order_id = o.order_id \
join tempProducts p on oi.order_item_product_id = p.product_id \
where o.order_status in ('COMPLETED','CLOSED') \
group by p.product_name, o.order_date \
order by o.order_date ASC, revenue DESC") 

sqlContext.sql("create database IF NOT EXISTS iskibongue_daily_revenue")
sqlContext.sql("create table IF NOT EXISTS iskibongue_daily_revenue.daily_revenue (product_name string, order_date string, daily_revenue_per_product float) STORED AS orc")

sqlRes.insertInto("iskibongue_daily_revenue.daily_revenue")
#If you can solve these problems.. you may be ready for CCA-175 . Give it a shot!

#Problem 1 :
#Import orders table from mysql (db: retail_db , user : retail_user , password : xxxx)
#Import only records that are in “COMPLETE” status
#Import all columns other than customer id
#Save the imported data as text and tab delimitted in this hdfs location /user/yourusername/jay/problem1/

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user --password itversity \
--table orders --columns 'order_id, order_date, order_status' \
--where 'order_status = "COMPLETE"' --m 1 \
--as-textfile --fields-terminated-by '\t' \
--target-dir /user/iskibongue/problem30052018/pb1/problem1

#Problem 2
#Import orders table from mysql (db: retail_db , user : retail_user , password : xxxx)
#Import all records and columns from Orders table
#Save the imported data as text and tab delimitted in this hdfs location /user/yourusername/jay/problem2/

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user --password itversity \
--table orders \
--m 1 \
--as-textfile --fields-terminated-by '\t' \
--target-dir /user/iskibongue/problem30052018/pb2/problem2

#Problem 3 :
#Export orders data into mysql
#Input Source : /user/yourusername/jay/problem2/
#Target Table : Mysql . DB = retail_export . Table Name : iskibongue_jay__mock_orders
#Reason for somealias in table name is … to not overwrite others in mysql db in labs

sqoop export \
--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
--username retail_user --password itversity \
--export-dir '/user/iskibongue/problem30052018/pb2/problem2' \
--input-fields-terminated-by '\t' \
--table 'iskibongue_jay__mock_orders'

#Problem 4 :
# Read data from hive and perform transformation and save it back in HDFS
# Read table populated from Problem 3 (jay__mock_orders )
# Produce output in this format (2 fields) , sort by order count in descending and save it as avro with snappy 
# compression in hdfs location /user/yourusername/jay/problem4/avro-snappy
# ORDER_STATUS : ORDER_COUNT
# COMPLETE 54
# CANCELLED 89
# INPROGRESS 23
# Save above output in avro snappy compression in avro format in hdfs location /user/yourusername/jay/problem4/avro

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
--username retail_user --password itversity \
--table iskibongue_jay__mock_orders \
--m 1 \
--as-textfile --fields-terminated-by '\t' \
--hive-import --hive-database problem30052018_pb4 \
--create-hive-table

pyspark --master yarn \
--conf spark.ui.port=12890 \
--num-executors 2 \
--executor-memory 512M \
--packages com.databricks:spark-avro_2.10:2.0.1

sqlContext.sql('use problem30052018_pb4')
res = sqlContext.sql('select order_status from iskibongue_jay__mock_orders')
resRdd = res.rdd.map(tuple).map(lambda o:(o[0],1)).reduceByKey(lambda a,p : a+p).sortBy(lambda o : o[1], ascending=False)
resDf = resRdd.toDF(schema = ['ORDER_STATUS','ORDER_COUNT'])
sqlContext.setConf('spark.sql.avro.compression.codec','snappy')
resDf.write.format('com.databricks.spark.avro').save('/user/iskibongue/problem30052018/pb4/problem4/avro-snappy')
sqlContext.setConf('spark.sql.avro.compression.codec','uncompressed')

#Problem 5 :
#  Import orders table from mysql (db: retail_db , user : retail_user , password : xxxx)
#  Import all records and columns from Orders table
#  Save the imported data as avro and snappy compression in hdfs location /user/yourusername/jay/problem5-avro-snappy/
#  Read above hdfs data
#  Consider orders only in “COMPLETE” status and order id between 1000 and 50000 (1001 to 49999)
#  Save the output (only 2 columns orderid and orderstatus) in parquet format 
#  with gzip compression in location /user/yourusername/jay/problem5-parquet-gzip/
#  Advance : Try if you can save output only in 2 files (Tip : use coalesce(2))

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user --password itversity \
--table orders \
--m 1 \
--compress --compression-codec 'snappy' \
--as-avrodatafile \
--target-dir /user/iskibongue/problem30052018/pb5/problem5-avro-snappy

# -- /user/iskibongue/problem30052018/pb5/problem5-avro-snappy/part-m-00000.avro

pyspark --master yarn \
--conf spark.ui.port=12890 \
--num-executors 2 \
--executor-memory 512M \
--packages com.databricks:spark-avro_2.10:2.0.1, com.databricks:spark-csv_2.10:1.4.0

oDf = sqlContext.read.format("com.databricks.spark.avro").load("/user/iskibongue/problem30052018/pb5/problem5-avro-snappy/part-m-00000.avro").\
filter("order_status = 'COMPLETE' AND order_id > 1000 AND order_id < 50000")

sqlContext.setConf('spark.sql.parquet.compression.codec','gzip')
oDf.select('order_id', 'order_status').coalesce(2).write.format('parquet').save('/user/iskibongue/problem30052018/pb5/problem5-parquet-gzip')
sqlContext.setConf('spark.sql.parquet.compression.codec','uncompressed')

#Problem 6 :
# Import orders table from mysql (db: retail_db , user : retail_user , password : xxxx)
# Import all records and columns from Orders table
# Save the imported data as text and tab delimitted in this hdfs location /user/yourusername/jay/problem6/orders/
# Import order_items table from mysql (db: retail_db , user : retail_user , password : xxxx)
# Import all records and columns from Order_items table
# Save the imported data as text and tab delimitted in this hdfs location /user/yourusername/jay/problem6/order-items/
# Read orders data from above HDFS location
# Read order items data form above HDFS location
# Produce output in this format (price and total should be treated as decimals)
# ORDER_ID ORDER_ITEM_ID PRODUCT_PRICE ORDER_SUBTOTAL ORDER_TOTAL
# Save above output as ORC in hive table “jay_mock_orderdetails”
# (Tip : Try saving into hive table from DF directly without explicit table creation manually)

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user --password itversity \
--table orders \
--as-textfile --fields-terminated-by '\t' \
--target-dir /user/iskibongue/problem30052018/pb6/problem6/orders

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user --password itversity \
--table order_items \
--as-textfile --fields-terminated-by '\t' \
--target-dir /user/iskibongue/problem30052018/pb6/problem6/order-items/

pyspark --master yarn \
--conf spark.ui.port=12890 \
--num-executors 2 \
--executor-memory 512M \
--packages com.databricks:spark-avro_2.10:2.0.1,com.databricks:spark-csv_2.10:1.4.0

o_Rdd = sc.textFile('/user/iskibongue/problem30052018/pb6/problem6/orders').
map(lambda o : )
oi_Rdd = sc.textFile('/user/iskibongue/problem30052018/pb6/problem6/order-items/')

o_df = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "\t").load("/user/iskibongue/problem30052018/pb6/problem6/orders")
+---+--------------------+-----+---------------+
| C0|                  C1|   C2|             C3|
+---+--------------------+-----+---------------+
|  1|2013-07-25 00:00:...|11599|         CLOSED|
|  2|2013-07-25 00:00:...|  256|PENDING_PAYMENT|
+---+--------------------+-----+---------------+

oi_df = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "\t").load("/user/iskibongue/problem30052018/pb6/problem6/order-items")
+---+---+----+---+------+------+
| C0| C1|  C2| C3|    C4|    C5|
+---+---+----+---+------+------+
|  1|  1| 957|  1|299.98|299.98|
|  2|  2|1073|  1|199.99|199.99|
|  5|  4| 897|  2| 49.98| 24.99|
|  6|  4| 365|  5|299.95| 59.99|
|  7|  4| 502|  3| 150.0|  50.0|
|  8|  4|1014|  4|199.92| 49.98|
+---+---+----+---+------+------+

# ORDER_ID ORDER_ITEM_ID PRODUCT_PRICE ORDER_SUBTOTAL ORDER_TOTAL
oi_df.registerTempTable('iskibongue_oi_df')
finalDf = sqlContext.sql('select C1 ORDER_ID, C0 ORDER_ITEM_ID, C5 PRODUCT_PRICE, C4 ORDER_SUBTOTAL, round(SUM(C4) over(partition by C1),2) ORDER_TOTAL from iskibongue_oi_df')
+--------+-------------+-------------+--------------+-----------+
|ORDER_ID|ORDER_ITEM_ID|PRODUCT_PRICE|ORDER_SUBTOTAL|ORDER_TOTAL|
+--------+-------------+-------------+--------------+-----------+
|       4|            5|        24.99|         49.98|     699.85|
|       4|            6|        59.99|        299.95|     699.85|
|       4|            7|         50.0|         150.0|     699.85|
|       4|            8|        49.98|        199.92|     699.85|
+--------+-------------+-------------+--------------+-----------+
finalDf.write.saveAsTable('problem30052018_pb4.jay_mock_orderdetails','ORC')

#Problem 7:
#  Import order_items table from mysql (db: retail_db , user : retail_user , password : xxxx)
#  Import all records and columns from Order_items table
#  Save the imported data as parquet in this hdfs location /user/yourusername/jay/problem7/order-items/
#  Import products table from mysql (db: retail_db , user : retail_user , password : xxxx)
#  Import all records and columns from products table
#  Save the imported data as avro in this hdfs location /user/yourusername/jay/problem7/products/
#  Read above orderitems and products from HDFS location
#  Produce this output (price and total should be treated as decimal)
#  ORDER_ITEM_ORDER_ID PRODUCT_ID PRODUCT_PRICE ORDER_SUBTOTAL
#  Save above output as avro snappy in hdfs location /user/yourusername/jay/problem7/output-avro-snappy/

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user --password itversity \
--table order_items \
--as-parquetfile \
--target-dir /user/iskibongue/problem30052018/pb7/problem7/order_items/

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user --password itversity \
--table products \
--as-avrodatafile \
--target-dir /user/iskibongue/problem30052018/pb7/problem7/products/



pyspark --master yarn \
--conf spark.ui.port=12890 \
--num-executors 2 \
--executor-memory 512M \
--packages com.databricks:spark-avro_2.10:2.0.1

oi_df = sqlContext.read.format('parquet').load('/user/iskibongue/problem30052018/pb7/problem7/order_items/')
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|        86100|              34470|                 1073|                  1|             199.99|                  199.99|
|        86101|              34470|                 1014|                  2|              99.96|                   49.98|
|        86102|              34470|                  365|                  2|             119.98|                   59.99|
|        86103|              34471|                  810|                  5|              99.95|                   19.99|
|        86104|              34471|                 1004|                  1|             399.98|                  399.98|
|        86105|              34471|                 1073|                  1|             199.99|                  199.99|
|        86106|              34471|                 1073|                  1|             199.99|                  199.99|
|        86107|              34472|                  957|                  1|             299.98|                  299.98|
|        86108|              34472|                 1014|                  5|              249.9|                   49.98|
|        86109|              34472|                  403|                  1|             129.99|                  129.99|
|        86110|              34473|                  191|                  4|             399.96|                   99.99|
|        86111|              34473|                  502|                  2|              100.0|                    50.0|
|        86112|              34473|                 1014|                  5|              249.9|                   49.98|
|        86113|              34474|                 1004|                  1|             399.98|                  399.98|
|        86114|              34474|                 1014|                  2|              99.96|                   49.98|
|        86115|              34474|                  365|                  4|             239.96|                   59.99|
|        86116|              34474|                  565|                  3|              210.0|                    70.0|
|        86117|              34475|                   44|                  1|              59.99|                   59.99|
|        86118|              34475|                  502|                  4|              200.0|                    50.0|
|        86119|              34475|                  957|                  1|             299.98|                  299.98|
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+

pdt_df = sqlContext.read.format('com.databricks.spark.avro').load('/user/iskibongue/problem30052018/pb7/problem7/products/') 
+----------+-------------------+--------------------+-------------------+-------------+--------------------+
|product_id|product_category_id|        product_name|product_description|product_price|       product_image|
+----------+-------------------+--------------------+-------------------+-------------+--------------------+
|         1|                  2|Quest Q64 10 FT. ...|                   |        59.98|http://images.acm|
|         2|                  2|Under Armour Men'...|                   |       129.99|http://images.acm...|
|         3|                  2|Under Armour Men'...|                   |        89.99|http://images.acm...|
|         4|                  2|Under Armour Men'...|                   |        89.99|http://images.acm...|
+----------+-------------------+--------------------+-------------------+-------------+--------------------+

oi_df.registerTempTable('oi_df')
finalDf = sqlContext.sql('select order_item_order_id ORDER_ITEM_ORDER_ID, order_item_product_id PRODUCT_ID, order_item_product_price PRODUCT_PRICE, order_item_subtotal ORDER_SUBTOTAL from oi_df')


sqlContext.setConf('spark.sql.avro.compression.codec','snappy')
finalDf.write.format('com.databricks.spark.avro').save('/user/iskibongue/problem30052018/pb7/problem7/output-avro-snappy/')
sqlContext.setConf('spark.sql.avro.compression.codec','uncompressed')

#Problem 8
#  Read order item from /user/yourusername/jay/problem7/order-items/
#  Read products from /user/yourusername/jay/problem7/products/
#  Produce output that shows product id and total no. of orders for each product id.
#  Output should be in this format… sorted by order count descending
#  If any product id has no order then order count for that product id should be “0”
#  PRODUCT_ID PRODUCT_PRICE ORDER_COUNT
#  Output should be saved as sequence file with Key=ProductID , Value = PRODUCT_ID|PRODUCT_PRICE|ORDER_COUNT (pipe separated)

oi_df = sqlContext.read.format('parquet').load('/user/iskibongue/problem30052018/pb7/problem7/order_items/')
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|        86100|              34470|                 1073|                  1|             199.99|                  199.99|
|        86101|              34470|                 1014|                  2|              99.96|                   49.98|
|        86102|              34470|                  365|                  2|             119.98|                   59.99|
|        86103|              34471|                  810|                  5|              99.95|                   19.99|
|        86104|              34471|                 1004|                  1|             399.98|                  399.98|
|        86105|              34471|                 1073|                  1|             199.99|                  199.99|
|        86106|              34471|                 1073|                  1|             199.99|                  199.99|
|        86107|              34472|                  957|                  1|             299.98|                  299.98|
|        86108|              34472|                 1014|                  5|              249.9|                   49.98|
|        86109|              34472|                  403|                  1|             129.99|                  129.99|
|        86110|              34473|                  191|                  4|             399.96|                   99.99|
|        86111|              34473|                  502|                  2|              100.0|                    50.0|
|        86112|              34473|                 1014|                  5|              249.9|                   49.98|
|        86113|              34474|                 1004|                  1|             399.98|                  399.98|
|        86114|              34474|                 1014|                  2|              99.96|                   49.98|
|        86115|              34474|                  365|                  4|             239.96|                   59.99|
|        86116|              34474|                  565|                  3|              210.0|                    70.0|
|        86117|              34475|                   44|                  1|              59.99|                   59.99|
|        86118|              34475|                  502|                  4|              200.0|                    50.0|
|        86119|              34475|                  957|                  1|             299.98|                  299.98|
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+

oi_df.registerTempTable('oi_df')

finalDf = sqlContext.sql('select order_item_product_id PRODUCT_ID, order_item_product_price PRODUCT_PRICE, count(order_item_product_id) ORDER_COUNT from oi_df group by order_item_product_id,order_item_product_price order by ORDER_COUNT desc')
finalDf.registerTempTable('finalDf')
finalRdd = sqlContext.sql('select PRODUCT_ID, CONCAT(PRODUCT_ID,"|",PRODUCT_PRICE,"|",ORDER_COUNT) AS details from finalDf').rdd.map(tuple)
finalRdd.saveAsSequenceFile('/user/iskibongue/problem30052018/pb8/problem8/output-sequence/')

#Problem 9
#  Import orders table from mysql (db: retail_db , user : retail_user , password : xxxx)
#  Import all records and columns from Orders table
#  Save the imported data as avro in this hdfs location /user/yourusername/jay/problem9/orders-avro/
#  Read above Avro orders data
#  Convert to JSON
#  Save JSON text file in hdfs location /user/yourusername/jay/problem9/orders-json/
#  Read json data from /user/yourusername/jay/problem9/orders-json/
#  Consider only “COMPLETE” orders.
#  Save orderid and order status (just 2 columns) as JSON text file in location /user/yourusername/jay/problem9/orders-mini-json/

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user --password itversity \
--table orders \
--as-avrodatafile --m 1 \
--target-dir /user/iskibongue/problem30052018/pb9/problem9/orders-avro/

pyspark --master yarn \
--conf spark.ui.port=12890 \
--num-executors 2 \
--executor-memory 512M \
--packages com.databricks:spark-avro_2.10:2.0.1

orders = sqlContext.read.format('com.databricks.spark.avro').load('/user/iskibongue/problem30052018/pb9/problem9/orders-avro/')
ordersJson = orders.toJSON()
ordersJson.saveAsTextFile('/user/iskibongue/problem30052018/pb9/problem9/orders-json/')
ro_js = sqlContext.read.format('json').load('/user/iskibongue/problem30052018/pb9/problem9/orders-json/').where('order_status = "COMPLETE"').select('order_id','order_status')

ro_js.toJSON().saveAsTextFile('/user/iskibongue/problem30052018/pb9/problem9/orders-mini-json/')
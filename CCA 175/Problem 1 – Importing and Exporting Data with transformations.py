#   *******
#   ******* 1 - Using sqoop, import orders table into hdfs to folders /user/cloudera/problem1/orders. File should be loaded as Avro File and use snappy compression 
#   *******
#   sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
#   -- username _user --password _pwd --table orders --target-dir /user/cloudera/problem1/orders \
#   --as-avrodatafile --compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec

#   *******
#   ******* 2 - Using sqoop, import order_items  table into hdfs to folders /user/cloudera/problem1/order-items. Files should be loaded as avro file and use snappy 
#   *******     compression
#   sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
#   -- username _user --password _pwd --table order-items --target-dir /user/cloudera/problem1/order-items \
#   -- as-avrodatafile --compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec

#   *******
#   ******* 3 - Using Spark Scala load data at /user/cloudera/problem1/orders and /user/cloudera/problem1/orders-items items as dataframes.
#   *******
#   pyspark --master yarn \
#     --conf spark.ui.port=12890 \
#     --num-executors 2 \
#     --executor-memory 512M \
#     --packages com.databricks:spark-avro_2.10:2.0.1

   dfO = sqlContext.read.format("com.databricks.spark.avro").load("/user/iskibongue/training120218/orders/part-m-00000.avro")
#   dfO.show(2)
#   +--------+-------------+-----------------+---------------+
#   |order_id|   order_date|order_customer_id|   order_status|
#   +--------+-------------+-----------------+---------------+
#   |       1|1374724800000|            11599|         CLOSED|
#   |       2|1374724800000|              256|PENDING_PAYMENT|
#   +--------+-------------+-----------------+---------------+

dfOi = sqlContext.read.format("com.databricks.spark.avro").load("/user/iskibongue/training120218/order_items/part-m-00000.avro")
#   dfOi.show(2)
#   +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
#   |order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
#   +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
#   |            1|                  1|                  957|                  1|             299.98|                  299.98|
#   |            2|                  2|                 1073|                  1|             199.99|                  199.99|
#   +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+

#   *******
#   ******* 4 - Expected Intermediate Result: Order_Date , Order_status, total_orders, total_amount
#   *******     In plain english, please find total orders and total amount per status per day
#   *******

ordersRdd = dfO.rdd.map(tuple)
order_itemsRdd = dfOi.rdd.map(tuple)

#   ******* a). Just by using Data Frames API – here order_date should be YYYY-MM-DD format

from pyspark.sql.functions import *

orders_df = sqlContext.read.format("com.databricks.spark.avro").load("/user/iskibongue/training120218/orders/part-m-00000.avro")
order_items_df = sqlContext.read.format("com.databricks.spark.avro").load("/user/iskibongue/training120218/order_items/part-m-00000.avro") 

joined_df = order_items_df.join(orders_df, order_items_df.order_item_order_id == orders_df.order_id, 'inner')
grouped_df = joined_df.groupBy(to_date(from_unixtime(col("order_date")/1000)).alias("order_date"),orders_df.order_status).agg({"order_item_subtotal":"sum","order_status":"count"})

grouped_df = grouped_df.select('order_formatted_date','order_status',col('count(order_status)').alias('total_orders'),round('sum(order_item_subtotal)',2).alias('total_amount'))

dataFrameResult = grouped_df.orderBy(desc("order_date"),"order_status",desc("total_amount"),"total_orders")

#   ********* b). Using Spark SQL  – here order_date should be YYYY-MM-DD format

sqlContext.sql("create database IF NOT EXISTS pb1_iskibongue_orders")
sqlContext.sql("use pb1_iskibongue_orders")

grouped_df.registerTempTable("resultData")

sqlResult = sqlContext.sql("select order_date, order_status, total_orders, total_amount from resultData")

#   ********* c). By using combineByKey function on RDDS — No need of formatting order_date or total_amount

orders_rdd = orders_df.rdd.map(tuple)
order_items_rdd = order_items_df.rdd.map(tuple)

o_kv = orders_rdd.map(lambda o: (int(o[0]),o))
oi_kv = order_items_rdd.map(lambda o:(int(o[1]),o))

joined_kv = o_kv.join(oi_kv)
#(32768, ((32768, 1392181200000, 1900, u'PENDING_PAYMENT'), (81958, 32768, 1073, 1, 199.99000549316406, 199.99000549316406)))
final_kv = joined_kv.map(lambda o:((o[1][0][1],o[1][0][3]),o[1][1][4]))
final_kv.first() 
#((1392181200000, u'PENDING_PAYMENT'), 199.99000549316406)
agg_final_kv = final_kv.aggregateByKey((0,0), lambda a,b:(a[0]+b,a[1]+1), lambda a,b : (a[0]+b[0],a[1]+b[1]))
agg_final_kv.first()
#((1384405200000, u'ON_HOLD'), (11893.650188446045, 60))
f_agg_final_kv = agg_final_kv.map(lambda o: (o[0][0],o[0][1],o[1][1],o[1][0]))
comByKeyResult = f_agg_final_kv.toDF(schema = ["order_date","order_status","total_orders","total_amount"]).orderBy(desc("order_date"),"order_status",desc("total_amount"),"total_orders")
comByKeyResult.show()


#   ******* 5.Store the result as parquet file into hdfs using gzip compression under folder
#   *******    /user/`whoami`/problem1/result4a-gzip
#   *******    /user/`whoami`/problem1/result4b-gzip
#   *******    /user/`whoami`/problem1/result4c-gzip

sqlContext.setConf("spark.sql.parquet.compression.codec","gzip");

dataFrameResult.write.parquet("/user/cloudera/problem1/result4a-gzip");
sqlResult.write.parquet("/user/cloudera/problem1/result4b-gzip");
comByKeyResult.write.parquet("/user/cloudera/problem1/result4c-gzip");

#   ******* 6.Store the result as parquet file into hdfs using snappy compression under folder
#   *******    /user/`whoami`/problem1/result4a-snappy
#   *******    /user/`whoami`/problem1/result4b-snappy
#   *******    /user/`whoami`/problem1/result4c-snappy

sqlContext.setConf("spark.sql.parquet.compression.codec","snappy");

dataFrameResult.write.parquet("/user/cloudera/problem1/result4a-snappy");
sqlResult.write.parquet("/user/cloudera/problem1/result4b-snappy");
comByKeyResult.write.parquet("/user/cloudera/problem1/result4c-snappy");

#   ******* 7.Store the result as CSV file into hdfs using No compression under folder
#   *******    /user/`whoami`/problem1/result4a-csv
#   *******    /user/`whoami`/problem1/result4b-csv
#   *******    /user/`whoami`/problem1/result4c-csv

dataFrameResult.rdd.map(tuple).map(lambda o: str(o[0]) + ',' + str(o[1]) + str(o[2]) + str(o[3])).saveAsTextFile("/user/`whoami`/problem1/result4a-csv")
sqlResult.rdd.map(tuple).map(lambda o: str(o[0]) + ',' + str(o[1]) + str(o[2]) + str(o[3])).saveAsTextFile("/user/`whoami`/problem1/result4b-csv")
comByKeyResult.rdd.map(tuple).map(lambda o: str(o[0]) + ',' + str(o[1]) + str(o[2]) + str(o[3])).saveAsTextFile("/user/`whoami`/problem1/result4c-csv")

#   ******* 
#   ******* 8.create a mysql table named result and load data from /user/`whoami`/problem1/result4a-csv to mysql table named result
#   ******* 
#   $ mysql -h ms.itversity.com:3306 -u _user -p _pwd
#   $ use retail_db;
#   $ create table result (
#   >                         order_date varchar(50) not null,
#   >                         order_status varchar(75) not null,
#   >                         total_orders int not null,
#   >                         total_amount numeric not null,
#   >                         constraint pk_result primary key (order_date,order_status)  
#   >                     );
#   > exit;

#   $ sqoop export --connect jdbc:mysql://msitversity.com:3306/retail_db \
#     --table result --input-fields-terminated-by "," --export-dir "/user/`whoami`/problem1/result4a-csv" \
#     --columns "order_date,order_status,total_orders,total_amount"
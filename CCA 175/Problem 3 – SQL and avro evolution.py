# 1 - Import all tables from mysql database into hdfs as avro data files. 
#    use compression and the compression codec should be snappy. 
#    data warehouse directory should be retail_stage.db

$ sqoop import-all-tables \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username root --password cloudera --compress \
--compression-codec snappy --warehouse-dir '/user/cloudera/problem3/retail_stage.db'
--autoreset-to-one-mapper --as-avrodatafile

# 2 - Create a metastore table that should point to the orders data imported by sqoop job above. Name the table orders_sqoop.
$ pyspark --master yarn \
--conf spark.ui.port=12890 \
--num-executors 2 \
--executor-memory 512M \
--packages com.databricks:spark-avro_2.10:2.0.1

orders_sqoop = sqlContext.read.format('com.databricks.spark.avro').load("/user/cloudera/pblem3/retail_stage.db/orders")
sqlContext.sql('create database in not exists rd_pb3')
orders_sqoop.saveAsTempTable('rd_pb3.orders_sqoop')

# Write query in hive that shows all orders belonging to a certain day. 
# This day is when the most orders were placed. select data from orders_sqoop

#select * from orders as x where x.order_date in (select order_date from (select * from (select order_date, count(order_id) as nbe from orders group by order_date) as sq order by nbe desc limit 1) i);
sqlRes = sqlContext.sql('\
	select * from orders as x where x.order_date in\
	(select order_date from(\
     select * from (select order_date, count(order_id) as nbe from orders group by order_date\
     ) as sq order by nbe desc limit 1) i)')

# Step 5 and 6 – Partitioning and Inserting data

create database retail;

create table orders_avro (
  order_id int,
  order_date date,
  order_customer_id int,
  order_status string)
  partitioned by (order_month string)
STORED AS AVRO;

insert overwrite table orders_avro partition (order_month)
select order_id, 
  to_date(from_unixtime(cast(order_date/1000 as int))), 
  order_customer_id, 
  order_status, 
  substr(from_unixtime(cast(order_date/1000 as int)),1,7) as order_month 
from default.orders_sqoop;


a = sc.parallelize(["dog", "salmon", "salmon", "rat", "elephant"], 3)
b = a.keyBy(lambda o: len(o))
c = sc.parallelize(["dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"], 3) 
d = c.keyBy(lambda o: len(o))
b.leftOuterJoin(d).collect()
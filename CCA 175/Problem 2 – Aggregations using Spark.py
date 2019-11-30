# 1 - Using sqoop copy data available in mysql products table to folder 
#    /user/cloudera/products on hdfs as text file. columns should be delimited by pipe ‘|’

#          sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
#          --username _user --password _pwd --table products --target-dir '/user/cloudera/products' \
#          --as-textfile --fields-terminated-by '|'


# 2 - move all the files from /user/`whoami`/products folder to /user/`whoami`/problem2/products folder
#          $ hdfs dfs -mkdir /user/cloudera/problem2
#          $ hdfs dfs -mkdir /user/cloudera/problem2/products
#          $ hdfs dfs -mv /user/cloudera/products /user/cloudera/problem2/products

# 3 - Change permissions of all the files under /user/`whoami`/problem2/products 
# such that owner has read,write and execute permissions, group has read and 
# write permissions whereas others have just read and execute permissions

#          //Read is 4, Write is 2 and execute is 1. 
#          //ReadWrite,Execute = 4 + 2 + 1 = 7
#          //Read,Write = 4+2 = 6
#          //Read ,Execute=4+1=5
#          hdfs dfs -chmod 765 /user/cloudera/problem2/products/*


# 4 - read data in /user/`whoami`/problem2/products and do the following operations using 
# a) dataframes api 
# b) spark sql 
# c) RDDs aggregateByKey method. 
# Your solution should have three sets of steps. Sort the resultant dataset by category id

#    filter such that your RDD\DF has products whose price is lesser than 100 USD
#    on the filtered data set find out the higest value in the product_price column under each category
#    on the filtered data set also find out total products under each category
#    on the filtered data set also find out the average price of the product under each category
#    on the filtered data set also find out the minimum price of the product under each category

from pyspark.sql import Row, functions as F


# pdt_id int, pdt_cat int, pdt_name string, pdt_des string, pdt_price float, pdt_image string
pdt_rdd = sc.textFile("/user/iskibongue/products").\
map(lambda o : (int((o.split('|')[0]).encode('utf-8').strip()),int((o.split('|')[1]).encode('utf-8').strip()),\
(o.split('|')[2]).encode('utf-8').strip(),(o.split('|')[3]).encode('utf-8').strip(),\
float((o.split('|')[4]).encode('utf-8').strip()),(o.split('|')[5]).encode('utf-8').strip()))


pdt_df = pdt_rdd.map(lambda o : Row(p_id=int(o[0]), p_category=int(o[1]), p_name=o[2], p_description= o[3], p_price=float(o[4]), p_image= o[5])).\
toDF()

pdt_df = pdt_df.filter(pdt_df.p_price < 100.00) 

# filter such that your RDD\DF has products whose price is lesser than 100 USD
filtered_pdt_rdd = pdt_rdd.filter(lambda o : o[4] < 100)

# on the filtered data set find out the higest value in the product_price column under each category
filtered_pdt_rdd2 = filtered_pdt_rdd.map(lambda o: (o[1],o[4])).\
aggregateByKey((0, 999999999, 0, 0) , lambda a,b:(a[0] if a[0] > b else b, a[1] if a[1] < b else b, a[2]+1, a[3]+b), lambda a,b:(a[0] if a[0] > b[0] else b[0], a[1] if a[1] < b[1] else b[1], a[2] + b[2], a[3] + b[3]),1).\
map(lambda o: (o[0],o[1][0],o[1][2],round(o[1][3]/o[1][2],2),o[1][1])).sortByKey()

for i in filtered_pdt_rdd2.take(15): print(i)

#### DataFrame
pdt_df2 = pdt_df.groupBy('p_category').agg(F.max('p_price').alias('maximum_price'),F.count("p_id").alias("total_products"),F.avg("p_price").alias("average_price"),F.min("p_price").alias("minimum_price"))
pdt_df2.show()

# 5 - store the result in avro file using snappy compression under these folders respectively
#    /user/`whoami`/problem2/products/result-df
#    /user/`whoami`/problem2/products/result-rdd

pdt_df2.write.format('com.databricks.spark.avro').save('/user/iskibongue/problem2/products/result-df')
filtered_pdt_rdd2.toDF(schema=['product_category_id','maximum_price','total_products','average_price','minimum_price']).write.format('com.databricks.spark.avro').save('/user/iskibongue/problem2/products/result-rdd')

pyspark --master yarn \
--conf spark.ui.port=12890 \
--num-executors 2 \
--executor-memory 512M \
--packages com.databricks:spark-avro_2.10:2.0.1
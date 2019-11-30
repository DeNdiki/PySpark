#Read orders & order_items
orders = sc.textFile("/public/retail_db/orders")
order_items = sc.textFile("/public/retail_db/order_items")

#filter for completed or closed orders
filteredOrders = orders.filter(lambda o : o.split(",")[3] == "COMPLETED" or o.split(",")[3] == "CLOSED")

#Convert both filtered orders and order_items to key value pairs
kv_orders = filteredOrders.map(lambda o: (int(o.split(",")[0]),o.split(",")[1])) #orderId, orderDate
kv_order_items = order_items.map(lambda oi: (int(oi.split(",")[1]),(int(oi.split(",")[2]),float(oi.split(",")[4])))) #orderId, productId, productRevenue

#join the two data sets
joinedkvDs = kv_orders.join(kv_order_items) 
#(4, (u'2013-07-25 00:00:00.0', (897, 49.98))) ---> (orderId,(date,(productId,revenue)))
joinedkvDsMap = joinedkvDs.map(lambda o : ((o[1][0],o[1][1][0]),o[1][1][1]))
# Aggregate to get daily revenue per product id
dr_per_product = joinedkvDsMap.aggregateByKey(0,lambda a,b:a+b, lambda a,b:a+b)
for i in dr_per_product.take(10) : print i
#((u'2013-12-05 00:00:00.0', 957), 1499.9)
#((u'2014-06-11 00:00:00.0', 24), 319.96)
#((u'2014-01-20 00:00:00.0', 977), 89.97)


#Load products from local file system and convert into RDD
#Product line structure
#'1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy'
products = sc.textFile("/public/retail_db/products")
product_map = products.map(lambda p: (int(p.split(",")[0]),p.split(",")[2]))
product_map.first()
#(1, u'Quest Q64 10 FT. x 10 FT. Slant Leg Instant U')
#Join daily revenue per product id with products to get daily revenue per product in descending order (by name)
dr_per_product_2 = dr_per_product.map(lambda dr: (dr[0][1],dr))
dr_per_product_2.first()

joinedkvDs_2 = product_map.join(dr_per_product_2)
joinedkvDs_2.first()
#(957, (u"Diamondback Women's Serene Classic Comfort Bi", ((u'2014-01-11 00:00:00.0', 957), 1499.9)))
#Daily revenue per product by name
joinedkvDs_3 = joinedkvDs_2.map(lambda o: ((o[1][0],o[1][1][0][0]),o[1][1][1]))
joinedkvDs_3.first()
#((u'Elevation Training Mask 2.0', u'2014-06-11 00:00:00.0'), 319.96)

#Sort the data by date in ascending order and by daily revenue per product in descending order
toSort_RDD = joinedkvDs_3.map(lambda o : ((o[0][1],-o[1]),o))
toSort_RDD.first()
#((u'2014-06-11 00:00:00.0', -319.96), ((u'Elevation Training Mask 2.0', u'2014-06-11 00:00:00.0'), 319.96))
sortedRDD = toSort_RDD.sortByKey()
for i in sortedRDD.take(100) : print i
#((u'2013-08-01 00:00:00.0', -1149.54), ((u"O'Brien Men's Neoprene Life Vest", u'2013-08-01 00:00:00.0'), 1149.54))
final_sortedRDD = sortedRDD.map(lambda o : o[1])
for i in final_sortedRDD.take(100) : print i
#((u'Pelican Sunstream 100 Kayak', u'2013-08-01 00:00:00.0'), 1599.92)
#((u"Diamondback Women's Serene Classic Comfort Bi", u'2013-08-01 00:00:00.0'), 1499.9)
#((u"O'Brien Men's Neoprene Life Vest", u'2013-08-01 00:00:00.0'), 1149.54)
#((u"Nike Men's Free 5.0+ Running Shoe", u'2013-08-01 00:00:00.0'), 899.9100000000001)

#Get data to desired format : order_date, daily_revenue_per_product, product_name
final_DS = final_sortedRDD.map(lambda o: (o[0][1],round(o[1],2),o[0][0]))
final_DS.first()
#(u'2013-07-25 00:00:00.0', 2399.88, u'Field & Stream Sportsman 16 Gun Fire Safe')

# Save to HDFS and validate in text file format
final_DS.saveAsTextFile("/user/iskibongue/daily_revenue_txt_python")

#Saving data in avro file format
final_DS_DF = final_DS.toDF(schema=["order_date", "revenue_per_product", "product_name"])
final_DS_DF.show()
#Avro 
#final_DS_DF.save("/user/iskibongue/daily_revenue_avro_python","com.databricks.spark.avro")
#Json
final_DS_DF.save("/user/iskibongue/daily_revenue_json_python","json")

# Develop as application to get daily revenue per product
#Execute following CMD to do it
#cd pythonDemo
#cd retail
# spark-submit \
# --master yarn \
# --conf spark.ui.port=12789 \
# --num-executors 2 \
# --executor-memory 512M \
# scr/main/python/DailyRevenuePerProduct.py

#Set operations - Prepare data - subsets of products for 2013-12 and 2014-01
orders = sc.textFile("/public/retail_db/orders")
orderItems = sc.textFile("/public/retail_db/order_items")

orders201312 = orders. \
filter(lambda o: o.split(",")[1][:7] == "2013-12"). \
map(lambda o: (int(o.split(",")[0]), o))

orders201401 = orders. \
filter(lambda o: o.split(",")[1][:7] == "2014-01"). \
map(lambda o: (int(o.split(",")[0]), o))

orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), oi))

orderItems201312 = orders201312. \
join(orderItemsMap). \
map(lambda oi: oi[1][1])
orderItems201401 = orders201401. \
join(orderItemsMap). \
map(lambda oi: oi[1][1])
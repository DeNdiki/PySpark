#problem scenario 30

pyspark --master yarn \
  --conf spark.ui.port=12890 \
  --num-executors 2 \
  --executor-memory 512M \
  --packages com.databricks:spark-avro_2.10:2.0.1



rootPath = "/user/iskibongue/pb_scenario/29/"
emp = sc.textFile(rootPath+"employeename.csv").map(lambda es: (int(es.split(',')[0]),es.split(',')[1]))
empMan = sc.textFile(rootPath+"employeemanager.csv").map(lambda es: (int(es.split(',')[0]),es.split(',')[1]))
empSal = sc.textFile(rootPath+"employeesalary.csv").map(lambda es: (int(es.split(',')[0]),es.split(',')[1]))

join_Set = emp.join(empSal).join(empMan).sortByKey()
join_Set.first()

finalSet = join_Set.map(lambda e : (e[0],e[1][0][0],e[1][0][1],e[1][1]))
finalSet.first()
finalSet.saveAsTextFile(rootPath+"solution/result.txt")
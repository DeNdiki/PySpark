$ pyspark --master yarn \
--conf spark.ui.port=12890 \
--num-executors 2 \
--executor-memory 512M \
--packages com.databricks:spark-avro_2.10:2.0.1

from pyspark.sql import functions as F, Row

def isfloat(value):
	  try:
	    c = float(value)
	    return c
	  except:
	    return 0.0

val = sc.textFile("/user/iskibongue/pb_scenario/sncf/validations.csv").filter(lambda o: o.split(';')[0] != 'JOUR')

nbeVal = val.count()

valDf = val.map(lambda o : Row(jour=o.split(';')[0].encode('utf-8'),\
codeStifTrns=o.split(';')[1].encode('utf-8'), codeStifRes=o.split(';')[2].encode('utf-8'),\
codeStifArret=o.split(';')[3].encode('utf-8'),\
libelleArret=o.split(';')[4].encode('utf-8'), idRefaLda=o.split(';')[5].encode('utf-8'),\
titre=o.split(';')[6].encode('utf-8'), validations= isfloat(o.split(';')[7].encode('utf-8')) if len(o.split(';')) == 8 else 0 )).toDF()

val_1_sem2015 = valDf.where("jour like '%2015-01%' or jour like '%2015-02%' or jour like '%2015-03%' or jour like '%2015-04%' or jour like '%2015-05%' or jour like '%2015-06%'")

val_1_sem2015.count()

val_1_01_2015 = valDf.filter(valDf.jour == '2015-01')

val_1_01_2015.count()

#Pourcentage de validation par mois
val_mois = val.map(lambda o: ((o.split(';')[0])[:7],isfloat(o.split(';')[7].encode('utf-8')))).reduceByKey(lambda a,b : a+b)
val_mois.first()

c = val_mois.count()
nbeVal = 0.0

for i in val_mois.take(c): nbeVal += i[1]

val_mois_per = val_mois.map(lambda o : (o[0], o[1], round((o[1]/nbeVal)*100, 2)))
val_mois_per.collect()

#Requête 4 : top 5 des gares qui ont le plus de validations?
val_gare = val.map(lambda o: (o.split(';')[4], isfloat(o.split(';')[7].encode('utf-8')))).\
reduceByKey(lambda a,b : a+b).sortBy(lambda o : o[1], False).\
map(lambda o : (o[0], o[1], round((o[1]/nbeVal)*100, 2)))

for i in val_gare.take(5): print(i)

val_gare.coalesce(1).map(lambda o: Row(gare=o[0].encode('utf-8'), validations=o[1], pourcentage=o[2])).\
toDF().write.format('json').save('/user/iskibongue/pb_scenario/sncf/topGares')

val_gare.coalesce(1).map(lambda o: Row(gare=o[0].encode('utf-8'), validations=o[1], pourcentage=o[2])).\
toDF().write.format('csv').save('/user/iskibongue/pb_scenario/sncf/topGares-csv')

#Requête 5 : top 5 des validations par gare
valg_top_5 = val.map(lambda o: (o.split(';')[4], isfloat(o.split(';')[7].encode('utf-8')))).\
groupByKey().map(lambda o : (o[0], sorted(list(o[1]), reverse=True)[:5])).sortByKey()

valg_top_5.coalesce(1).map(lambda o: Row(gare=o[0].encode('utf-8'), top_5_validations=o[1])).\
toDF().write.format('json').save('/user/iskibongue/pb_scenario/sncf/top-5-validations-Gares-json')
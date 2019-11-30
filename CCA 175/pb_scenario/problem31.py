#Problem Scenario 31 : You have given following two files
#1. Content.txt : Contain a huge text file containing space separated words.
#2. Remove.txt : Ignore/filter all the words given in this file (Comma Separated).
#Write a Spark program which reads the Content.txt tile and load as an ROD, remove all the
#words from a broadcast variables (which is loaded as an RDDof words from remove.txt).
#and count the occurrence of the each word and save ie as a text file HDFS.

_sPath = "/home/iskibongue/kandor/pb_scenario/31/"
sPath = "/user/iskibongue/pb_scenario/31/"
_Content = open(_sPath + "Content.txt").read()
_remove = open(_sPath + "Remove.txt").read()

lstR = []
for i in _remove.split(",") : lstR.append(str(i).lower().strip())

listW = []
for i in _Content.split(" ") : listW.append(str(i).lower().strip())

ContentRdd = sc.parallelize(listW).filter(lambda w: w not in lstR)
ContentRdd2 = ContentRdd.map(lambda w: (w,1)).reduceByKey(lambda a,b:a+b)

ContentRdd2.first()

ContentRdd2.coalesce(1).saveAsTextFile(sPath + "result")

#coalesce(numPartition) part-00000, part-00001, part-00002



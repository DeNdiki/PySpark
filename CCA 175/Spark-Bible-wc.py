import re
import string
import itertools



def clearup(s, chars):
       return re.sub('[%s]' % chars, '', s).lower()



brutBible = open("kandor/bible.txt").read().splitlines()
brutBible = filter(lambda b:len(b)>0,brutBible)
brutBible = map(lambda p : clearup(p, string.punctuation+string.digits).strip(), brutBible)
bibleWords = [x.split(" ") for x in brutBible]
finalBibleWords = list(itertools.chain(*bibleWords))

bw_rdd = sc.parallelize(finalBibleWords). \
filter(lambda p: len(p) > 0). \
map(lambda p:(p,1)). \
aggregateByKey(0, lambda a,b: a+b, lambda _a,_b: _a+_b). \
sortBy(lambda p:p[1], False)

bw_df = bw_rdd.toDF(schema=["word","occurence"])

#before save the dataframe in json, parquet, avro or sequence File format, make sure that Rdd is transformed to a DataFrame
bw_df.save("/user/iskibongue/bilewc_project/json","json")

#Or use this other method 
bw_df.write.json("/user/iskibongue/bilewc_project/json")

#For read the data
sqlContext.read.json("/user/iskibongue/bilewc_project/json").show()

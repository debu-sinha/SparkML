from pyspark import SparkConf, SparkContext
import collections
import re

def denormalizeWords(text):
	return re.compile(r'\W+',re.UNICODE).split(text.lower())
	
conf = SparkConf().setMaster("local").setAppName("word count")
sc =  SparkContext(conf = conf)

lines= sc.textFile("./PracticeData/Book.txt")

#rdd = lines.flatMap(denormalizeWords).countByValue()
rdd = lines.flatMap(denormalizeWords).map(lambda x: (x,1)).reduceByKey(lambda x, y: x+y).map(lambda x: (x[1],x[0])).sortByKey().collect()

for x, y in rdd:
	cleanWord = y.encode('ascii', 'ignore')
	if(cleanWord):	
		print x, cleanWord

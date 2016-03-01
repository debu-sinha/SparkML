from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("weather data")
sc = SparkContext(conf = conf)

lines = sc.textFile("./PracticeData/1800.csv")

def parseLine(line):
	fields = line.split(',')
	stationId = fields[0]
	date = fields[1]
	entryType = fields[2]
	temperature = float(fields[3])*0.1*(9.0/5.0)+32.0
	return(stationId, date, entryType, temperature) 
 
rdd = lines.map(parseLine)
rddMax = rdd.filter(lambda x: "TMAX" in x[2]).map(lambda x: (x[0],x[3])).reduceByKey(lambda x , y :  max(x,y))

results = rddMax.collect()
for result in results:
	print result[0]+"\t{:.2f}F".format(result[1])
 

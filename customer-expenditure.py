from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('Customer Expenditure')
sc = SparkContext(conf = conf)

lines = sc.textFile('./PracticeData/customer-orders.csv')

def process(line):
	values = line.split(',')
	customerId = values[0]
	productId = values[1]
	amountSpent = float(values[2])
	return (customerId,amountSpent)

rdd = lines.map(process).reduceByKey(lambda x, y : x+y).map(lambda x: (x[1],x[0])).sortByKey().collect()

for amountSpent, clientId in rdd:
	print clientId, amountSpent     

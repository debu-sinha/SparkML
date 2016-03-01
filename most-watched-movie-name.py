from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('Most popular movie by name')
sc = SparkContext(conf = conf)

lines1 =  sc.textFile('./PracticeData/ml-100k/u.data')
lines2 = sc.textFile('./PracticeData/ml-100k/u.item')

def prepare(line):
	return (line.split("\t")[1],1)

def extractMovieName(line):
	values = line.split("|")
	return (values[0],values[1])

rdd1 = lines1.map(prepare).reduceByKey(lambda x, y: x+y)
rdd2 = lines2.map(extractMovieName)
rdd3 = rdd1.join(rdd2).map(lambda x: (x[1][0],x[1][1])).sortByKey().collect()

print rdd3[-1][1]

from pyspark import SparkConf, SparkContext

conf =  SparkConf().setMaster('local').setAppName('Most Watched Movie')
sc = SparkContext(conf = conf)

def process(line):
	values = line.split("\t")
	return (values[1],1)

lines = sc.textFile('./SparkCourse/ml-100k/u.data')
 
rdd = lines.map(process).reduceByKey(lambda x, y: x+y).map(lambda x: (x[1],x[0])).sortByKey().collect()

mostWatchedMovie = rdd[-1]
print mostWatchedMovie[1]
 

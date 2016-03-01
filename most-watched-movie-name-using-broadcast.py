from pyspark import SparkConf, SparkContext

def loadMovieNames():
	movieNames = {}
	with open('./PracticeData/ml-100k/u.item') as f:
		for line in f:
			fields = line.split('|')
			movieNames[int(fields[0])] =  fields[1]
	return movieNames
		
conf = SparkConf().setMaster('local').setAppName('Most popular movie by name')
sc = SparkContext(conf = conf)


nameDict =  loadMovieNames()

lines =  sc.textFile('./PracticeData/ml-100k/u.data')

nameDict =  sc.broadcast(loadMovieNames())

def prepare(line):
	return (line.split("\t")[1],1)

rdd = lines.map(lambda x: (int(x.split()[1]),1)).reduceByKey(lambda x, y: x+y).map(lambda x: (x[1],x[0])).sortByKey()

sortedMoviesByNames= rdd.map(lambda (count, movie) : (nameDict.value[movie],count)).collect()

for result in sortedMoviesByNames:
	print result


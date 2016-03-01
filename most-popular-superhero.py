from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('most popular super hero')
sc =  SparkContext(conf = conf)

def getNames():
	nameDict = {}
	with open('./Marvel-Names.txt') as f:
		for line in f:
			if(len(line.strip())>0):
				values = line.split('\"');
				nameDict[int(values[0])] = values[1].strip()
	return nameDict

def countCoOccurances(line):
	values = line.split()
	return (int(values[0]), len(values)-1)

namesDict = sc.broadcast(getNames())

lines = sc.textFile("./PracticeData/Marvel-Graph.txt")

rdd = lines.map(countCoOccurances).reduceByKey(lambda x, y : x + y).map(lambda (hero, friendCount) : (friendCount, namesDict.value[hero])).sortByKey().collect()

print rdd[-1][1] + " is the most popular hero with " +str(rdd[-1][0]) +" appearences"
  		

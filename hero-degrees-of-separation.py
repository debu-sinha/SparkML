# this code demonstrates how to use pyspark to analyze a dummy super hero social graph and calculating degree of separation between two superheroes
#boiler plate
from pyspark import SparkConf, SparkContext

#spark confand context setup
conf = SparkConf().setMaster('local').setAppName('degree of separationi using BFS')
sc =  SparkContext(conf = conf)

#inout the start and target heroes
startCharId = 5306 #Spiderman
targetCharId = 14 #ADAM  3,031

#Accumulator used to signal when the target character has been found during our BFS
hitCounter = sc.accumulator(0)

#convert a line to BFS
def convertToBFS(line):
	values = line.split()
	connections = []
	heroId = int(values[0])
	for connection in values[1:]:
		connections.append(int(connection))

	color = 'WHITE'
	distance = 9999
	
	if(startCharId==heroId):
		color = 'GRAY'
		distance = 0
	
	return (heroId,(connections, distance, color))

#initiate the graph data and return rdd 
def initializeNodes():
	lines = sc.textFile("./PracticeData/Marvel-Graph.txt")
	return lines.map(convertToBFS)

#
def bfsMap(node):
	characterId = node[0]
	data = node[1]
	connections =  data[0]
	distance = data[1]
	color = data[2]
	
	results = []
	
	#if this node needs to be processed
	if(color == 'GRAY'):
		for connection in connections:
			newCharId = connection
			newDistance = distance + 1
			newColor = 'GRAY'
			if(targetCharId == newCharId):
				hitCounter.add(1)
			newEntry = (newCharId,([], newDistance, newColor)) 
			results.append(newEntry)
		color = 'BLACK'
	results.append((characterId, (connections, distance, color)))
	return results

def bfsReduce(data1, data2):
	edges1 = data1[0]
	edges2 = data2[0]
	distance1 = data1[1]
	distance2 = data2[1]
	color1 = data1[2]
	color2 = data2[2]
	
	distance = 9999
	color = 'WHITE'
	edges = []
	
	#check if one is original node with connection
	if(len(edges1) > 0):
		edges = edges1
	if(len(edges2) > 0):
		for connection in edges2:
			edges.append(connection)
	
	#preserve minimum distance
	if(distance1 < distance):
		distance = distance1
	
	if(distance2 < distance):
		distance = distance2

	#preserve darkest color
	if(color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')): 
		color = color2
	
	if(color1 == 'GRAY' and color2 == 'BLACK'): 
		color = color2
	
	if(color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')): 
		color = color1
	
	if(color2 == 'GRAY' and color1 == 'BLACK'): 
		color = color1

	return (edges, distance, color)

iterationRdd = initializeNodes() 

for iter in range(0,10):
	print "Running BFS iteration# "+str(iter +1)
	
	#creat new vertices as needed to darken or reduce distances in the 
	#reduce stage. If we encounter the node we're looking for as a GRAY
	#node, increment our accumulator to signal that we're done
	
	mapped  = iterationRdd.flatMap(bfsMap)

	#Note that mapped.count() action here forces the RDD to be evaluated, and
	#thats the only reason our accumulator is actually updated

	print "Processing "+str(mapped.count()) +" values."
	if(hitCounter.value > 0):
		print "Hit the target character! from " + str(hitCounter.value) + " different directions."
		break


	#Reducer compbines data for each character ID, preserving the darkest
	#color and shortest path
	iterationRdd = mapped.reduceByKey(bfsReduce)
	

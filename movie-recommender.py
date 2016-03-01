#boiler plate
import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

conf = SparkConf().setMaster("local[*]").setAppName('movie recommender/ collaborative filtering')
sc = SparkContext(conf = conf)

def loadMovieNames():
	movienames = {}
	with open("./PracticeData/ml-100k/u.item") as f:
		for line in f:
			fields = line.split('|')
			movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
	return movieNames

def makePairs((user, ratings)):
	(movie1, rating1) = ratings[0]
	(movie2, rating2) = ratings[1]
	return ((movie1, movie2), (rating1, rating2))

def filterDuplicates((userId, ratings)):
	(movie1, rating1) = ratings[0]
	(movie2, rating2) = ratings[1]
	return movie1 < movie2

def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)



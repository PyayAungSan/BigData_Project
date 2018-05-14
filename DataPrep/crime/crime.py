from pyspark import SparkContext, SparkFiles, SparkConf
from csv import reader
import sys
import string

sc = SparkContext()

lines2 = sc.textFile(sys.argv[1],1)
lines = lines2.mapPartitions(lambda x: reader(x))
lines = lines.map(lambda x: (x[13],x[5],x[6],x[7],x[21],x[22]))

fixing = lines.sortBy(lambda x:x[0])
fixing2 = fixing.filter(lambda x: x[0] not in '')

fixing2.map(lambda x: x).saveAsTextFile("crime_spark.txt")


sc.stop()








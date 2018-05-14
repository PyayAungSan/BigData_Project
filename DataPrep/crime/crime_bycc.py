from pyspark import SparkContext, SparkFiles, SparkConf
from csv import reader
import sys
import string

sc = SparkContext()

lines2 = sc.textFile(sys.argv[1],1)
lines2 = lines2.mapPartitions(lambda x:reader(x))
fixing = lines2.map(lambda x: ((x[0],x[1],x[2]),1))

fixing2 = fixing.countByKey().items()
fixing2 = sorted(fixing2)
fixing2 = sc.parallelize(fixing2)

fixing2.map(lambda x: "%s,%s,%s,%s"%(x[0][0],x[0][1],x[0][2],x[1])).saveAsTextFile("crime_bycc.txt")



sc.stop()



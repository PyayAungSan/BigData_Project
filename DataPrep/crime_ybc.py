from pyspark import SparkContext, SparkFiles, SparkConf
from csv import reader
import sys
import string

def borofix(x):
    conv = ''
    if x == 'BRONX':
        conv = 'Bronx'
    if x == 'BROOKLYN':
        conv = 'Brooklyn'
    if x == 'MANHATTAN':
        conv = 'Manhattan'
    if x == 'QUEENS':
        conv = 'Queens'
    if x == 'STATEN ISLAND':
        conv = 'Staten Island'
    return conv

def year(x):
    fix = x.split('/')
    year = fix[2]
    return year


sc = SparkContext()

lines2 = sc.textFile(sys.argv[1],1)
lines2 = lines2.mapPartitions(lambda x:reader(x))
fixing = lines2.map(lambda x: (x[0],x[1],x[2]))

fixing2 = fixing.filter(lambda x: x[0] not in '')
fixing2 = fixing2.sortBy(lambda x: x[0])

fixing2.map(lambda x: "%s,%s,%s"%(x[0],x[1],x[2])).saveAsTextFile("crime_ybc.txt")



sc.stop()







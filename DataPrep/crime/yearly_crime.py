
from __future__ import print_function

import sys
from operator import add
from csv import reader 
from pyspark import SparkContext
import datetime



def remove_header(itr_index, itr):
 return iter(list(itr)[1:]) if itr_index == 0 else itr 
 
  
def val_date(report_time): 

 try:
  report = datetime.datetime.strptime(report_time,'%m/%d/%Y')
  if report.year<2006 or report.year>2016: 
   return "-1"
  else: 
   return "1"
 except:
  if report_time == '':
   return "-1"
  else:
   return "1" 
    
  
if __name__ == "__main__":

 sc = SparkContext()
 
 lines = sc.textFile(sys.argv[1], 1)
 line = lines.mapPartitions(lambda x:reader(x))
 line = line.mapPartitionsWithIndex(remove_header).map(lambda x: [x[5],val_date(x[5])])
 line = line.filter(lambda x:x[1]== "1").map(lambda x: (datetime.datetime.strptime(x[0],'%m/%d/%Y').year,1))
 line = line.reduceByKey(lambda x,y: x+y)
 line = line.map(lambda x: '%s\t%s' %(x[0],x[1]))
 
 line.saveAsTextFile("yearly_crime.txt") 

 sc.stop()


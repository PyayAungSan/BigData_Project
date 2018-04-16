from csv import reader
import sys

count = 0

for line in sys.stdin:
    entry = next(reader([line]))
    '''
    if(count==0):
        pass
        count = 1
    if('2017' in line):
        break

    if(count!=0):
        key = entry[0]
        year = key[:4]
        time = entry[1]
        spd = entry[2]
        visb = entry[3]
        temp = entry[4]
        prcp = entry[5]
        sd = entry[6]
        sdw = entry[7]
        print("%s\t%s,%s,%s,%s,%s,%s"%(year,spd,visb,temp,prcp,sd,sdw))

    '''
    key = entry[0]
    year = key[:4]
    spd = entry[2]
    #visb = entry[3]
    temp = entry[4]
    prcp = entry[5]
    sd = entry[6]
    sdw = entry[7]
    #print("%s\t%s,%s,%s,%s,%s,%s"%(year,spd,visb,temp,prcp,sd,sdw))
    print("%s\t%s,%s,%s,%s,%s"%(year,spd,temp,prcp,sd,sdw))

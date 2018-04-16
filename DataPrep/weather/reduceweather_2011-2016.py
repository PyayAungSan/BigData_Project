import csv
from csv import reader
import sys

currentKey = None
spd = 0.0
visb = 0
temp = 0.0
Prcp = 0.0
SD = 0.0
SDW = 0.0
count = 1
key = 'a'

with open("edit_weather-2011-2016.csv","w+") as my_csv:
    csvWriter = csv.writer(my_csv,delimiter=',')
    for line in sys.stdin:
        line = line.strip()
        key, value = line.split('\t')
        value = value.split(',')
        if(key==currentKey):
            spd += float(value[0])
            visb = visb + float(value[1])
            temp += float(value[2])
            Prcp += float(value[3])
            SD += float(value[4])
            #SDW += float(value[5])
            count +=1
        else:
            if(currentKey):
                avgspd = spd/count
                avgvisb = visb/count
                avgtemp = temp/count
                avgprcp = Prcp/count
                avgsd = SD/count
                #avgsdw = SDW/count
                #into = [[key,avgspd, avgvisb,avgtemp,avgprcp,avgsd,avgsdw]]
                into = [[key,avgspd, avgvisb,avgtemp,avgprcp,avgsd]]
                csvWriter.writerows(into)
            currentKey = key
            spd = float(value[0])
            visb = float(value[1])
            temp = float(value[2])
            Prcp = float(value[3])
            SD = float(value[4])
            #SDW = float(value[5])
            count = 1
    '''
    avgspd = spd/count
    avgvisb = visb/count
    avgtemp = temp/count
    avgprcp = Prcp/count
    avgsd = SD/count
    #avgsdw = SDW/count
    #into = [[key,avgspd, avgvisb,avgtemp,avgprcp,avgsd,avgsdw]]
    into = [[key,avgspd, avgvisb,avgtemp,avgprcp,avgsd]]
    '''
    #csvWriter.writerows(into)
        







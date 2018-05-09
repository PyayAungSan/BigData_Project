#!/usr/bin/env python

import sys
import csv
import operator

import collections

currentKey = None
key = 0
count = 1

storing = dict([])

sumspd = 0
sumvisb = 0
sumtemp = 0
sumprcp = 0
sumsd = 0
sumsdw = 0
identify = 0

skip = 1

for line in sys.stdin:

    key, value = line.split('\t')
    key = key.replace(' ', '')
    value = value.replace(' ','')
    value = value.split(',')
    spd = value[0]
    visb = value[1]
    temp = value[2]
    prcp = value[3]
    sd = value[4]
    sdw = value[5]

    if(not key.isdigit())and(skip==1):
        skip=0
        print("%s, %s, %s, %s, %s, %s, %s"%(key,spd,visb,temp,prcp,sd,sdw))
        continue

    key = int(key)

    if(not visb.isdigit())or('999' in line):
        continue

    if(currentKey == key):
        sumspd += float(spd)
        sumvisb += float(visb)
        sumtemp += float(temp)
        sumprcp += float(prcp)
        sumsd += float(sd)
        sumsdw += float(sdw)
        count +=1
    else:
        if(currentKey):
            avgspd = sumspd/count
            avgvisb = sumvisb/count
            avgtemp = sumtemp/count
            avgprcp = sumprcp/count
            avgsd = sumsd/count
            avgsdw = sumsdw/count
            #print("{0:d}, {1:.2f}, {2:.2f}, {3:.2f}, {4:.2f}, {5:.2f}, {6:.2f}".format(key,avgspd,avgvisb,avgtemp,avgprcp,avgsd,avgsdw))
            storing[currentKey]=[avgspd,avgvisb,avgtemp,avgprcp,avgsd,avgsdw]
        currentKey = key
        sumspd = float(spd)
        sumvisb = float(visb)
        sumtemp = float(temp)
        sumprcp = float(prcp)
        sumsd = float(sd)
        sumsdw = float(sdw)
        count = 1

#'''
avgspd = sumspd/count
avgvisb = sumvisb/count
avgtemp = sumtemp/count
avgprcp = sumprcp/count
avgsd = sumsd/count
avgsdw = sumsdw/count
#print("{0:s}, {1:.2f}, {2:.2f}, {3:.2f}, {4:.2f}, {5:.2f}, {6:.2f}".format(key,avgspd,avgvisb,avgtemp,avgprcp,avgsd,avgsdw))
#storing[key] = [avgspd,avgvisb,avgtemp,avgprcp,avgsd,avgsdw]
if(currentKey):
    storing[currentKey] = [avgspd,avgvisb,avgtemp,avgprcp,avgsd,avgsdw]

#store = dict(sorted(storing.items(), key=operator.itemgetter(0)))
#'''


'''
for item in store:
    #print(item)

    stuff = store[item]
    Spd = stuff[0]
    Visb = stuff[1]
    Temp = stuff[2]
    Prcp = stuff[3]
    SD = stuff[4]
    SDW = stuff[5]
    print("{0:d},{1:.2f},{2:.2f},{3:.2f},{4:.2f},{5:.2f},{6:.2f}".format(item,Spd,Visb,Temp,Prcp,SD,SDW))
'''

#store2 = sorted(storing.items(), key=operator.itemgetter(0))

store2 = collections.OrderedDict(sorted(storing.items()))
for p, stuff in store2.items():
    item = int(p)
    Spd = stuff[0]
    Visb = stuff[1]
    Temp = stuff[2]
    Prcp = stuff[3]
    SD = stuff[4]
    SDW = stuff[5]
    print("{0:d}, {1:.2f}, {2:.2f}, {3:.2f}, {4:.2f}, {5:.2f}, {6:.2f}".format(item,Spd,Visb,Temp,Prcp,SD,SDW))


'''
for stuff in store2:
    item = stuff[0]
    Spd = stuff[1][0]
    Visb = stuff[1][1]
    Temp = stuff[1][2]
    Prcp = stuff[1][3]
    SD = stuff[1][4]
    SDW = stuff[1][5]
    print("{0:d},{1:.2f},{2:.2f},{3:.2f},{4:.2f},{5:.2f},{6:.2f}".format(item,Spd,Visb,Temp,Prcp,SD,SDW))
'''







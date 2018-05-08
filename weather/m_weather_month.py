#!/usr/bin/env python

import sys
#import numpy as np
#import pandas as pd
from csv import reader
import math

#goingin = sys.stdin

skip = 0

for line in sys.stdin:
    entry = next(reader([line]))

    if skip==0:
        date = entry[0][:6]
        print("%s\t%s, %s, %s, %s, %s, %s"%(date,entry[2],entry[3],entry[4],entry[5],entry[6],entry[7]))
        skip =1
        continue


    #date = entry[0][:6]   # in string
    date = int(entry[0][:6])

    print("%d\t%s, %s, %s, %s, %s, %s"%(date,entry[2],entry[3],entry[4],entry[5],entry[6],entry[7]))








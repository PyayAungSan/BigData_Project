from pyspark import SparkContext, SparkFiles, SparkConf
from csv import reader
import sys
import string

def zip2boro(x):
    if x.isdigit():
        pass
    else:
        return 'Borough'

    zipcomp = int(x)
    Bronx = [10453, 10457, 10460, 10458, 10467, 10468, 10451, 10452, 10456, 10454, 10455, 10459, 10474, 10463, 10471, 10466, 10469, 10470, 10475, 10461, 10462,10464, 10465, 10472, 10473]
    Brooklyn = [11212, 11213, 11216, 11233, 11238,  11209, 11214, 11228, 11204, 11218, 11219, 11230, 11234, 11236, 11239, 11223, 11224, 11229, 11235, 11201, 11205, 11215, 11217, 11231, 11203, 11210, 11225, 11226, 11207, 11208, 11211, 11222, 11220, 11232, 11206, 11221, 11237]
    Manhattan = [10026, 10027, 10030, 10037, 10039, 10001, 10011, 10018, 10019, 10020, 10036, 10029, 10035, 10010, 10016, 10017, 10022, 10012, 10013, 10014, 10004, 10005, 10006, 10007, 10038, 10280, 10002, 10003, 10009, 10021, 10028, 10044, 10065, 10075, 10128, 10023, 10024, 10025, 10031, 10032, 10033, 10034, 10040]
    Queens = [11361, 11362, 11363, 11364, 11354, 11355, 11356, 11357, 11358, 11359, 11360, 11365, 11366, 11367, 11412, 11423, 11432, 11433, 11434, 11435, 11436, 11101, 11102, 11103, 11104, 11105, 11106,  11374, 11375, 11379, 11385, 11691, 11692, 11693, 11694, 11695, 11697, 11004, 11005, 11411, 11413, 11422, 11426, 11427, 11428, 11429, 11414, 11415, 11416, 11417, 11418, 11419, 11420, 11421, 11368, 11369, 11370, 11372, 11373, 11377, 11378]
    StatenIsland = [10302, 10303, 10310, 10306, 10307, 10308, 10309, 10312, 10301, 10304, 10305, 10314]

    #    Bronx   10451 ~ 10499
    #    Brooklyn  11201 ~ 11256
    #    Staten Island 10301 ~ 10314
    #    Manhattan   10001 ~ 10292
    #    Queens  11002, 11004, 11005, 11101 ~ 11106, 11109, 11120, 11351, 11352, 11354~11499

    Queens_part = [11002,11004,11005,11101,11102,11103,11104,11105,11106,11109,11120,11351,11352]

    conv = ''
    if(zipcomp>=10451)and(zipcomp<=10499):
        conv = 'Bronx'
    if(zipcomp>=11201)and(zipcomp<=11256):
        conv = 'Brooklyn'
    if(zipcomp>=10301)and(zipcomp<=10314):
        conv = 'Staten Island'
    if(zipcomp>=10001)and(zipcomp<=10292):
        conv = 'Manhattan'
    if(zipcomp in Queens_part)or((zipcomp>=11354)and(zipcomp<=11499)):
        conv = 'Queens'

    return conv

sc = SparkContext()

lines2 = sc.textFile(sys.argv[1],1)
lines2 = lines2.mapPartitions(lambda x:reader(x))
fixing = lines2.map(lambda x: (zip2boro(x[8]),x[8],x[3],x[5],x[0]))

fixing2 = fixing.filter(lambda x: x[0] not in '')
fixing2 = fixing2.distinct()
fixing2 = fixing2.sortBy(lambda x: x[0])

fixing2.map(lambda x: x).saveAsTextFile("prop_yb.txt")



sc.stop()







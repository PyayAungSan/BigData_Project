import sys
import numpy as np
import pandas as pd


first = sys.argv[1]
second = sys.argv[2]

dff = pd.read_csv(first, sep=",", header=0) # for txt file
dfs = pd.read_csv(second, header=0)

cols_dff = dff.shape[1]     #-1

'''
adjust =0
if(len(dff)>len(dfs)):
 adjust = len(dfs)
else:
 adjust = len(dff)

#comp = pd.concat([dff[:adjust],dfs[:adjust]], axis=0).corr(method="spearman").iloc[:cols_dff,cols_dff:]
#comp = pd.concat([dff,dfs], axis=0).corr(method="spearman")#.iloc[:cols_dff,cols_dff:]

print(cols_dff)


#comp = dfs.append(dff,ignore_index=True)
print(cols_dff)
'''

comp = pd.concat([dff,dfs],axis=1)
comp = comp.corr(method="spearman").iloc[:cols_dff,cols_dff:]

print(comp)









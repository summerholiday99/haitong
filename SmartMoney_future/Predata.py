# -*- coding: utf-8 -*-
""" To do
#38之后的数据继续下载  ok
#把所需数据做在一起
#下载主力合约分钟数据

统计每个月有哪些品种可以加入组合（加入条件：上个月最后5天有数据) 这个可以出张图
算名单计算收益率
"""


import pandas as pd
import os

path = r'E:\Data\future'
def file_name(file_dir):   
    a = []
    for root, dirs, files in os.walk(file_dir):  
        a.append(files) #当前路径下所有非目录子文件
    return a[0] 
code = file_name(path) 

#%%挑选天数摆一起
#对每个文件标注月份，然后对时间开始月内倒序（留下月label）取月内倒序前10

contain = []
for i in range(len(code)):
    f = pd.read_excel(path+'\\'+code[i])     
    f['month_label'] = f['time'].apply(lambda x:str(x.year)+'-'+str(x.month).zfill(2))  
    f['date_label'] = f['time'].apply(lambda x:x.date())  
    f['inmonth_rank'] = f['date_label'].groupby(f.month_label).rank(ascending=False, method='dense')
    contain.append(f[f['inmonth_rank']<=10])
    print(code[i])
alldata = pd.concat(contain)
alldata.to_csv('future.csv') 


#%%统计每个月有多少  
ff = pd.read_csv(r'E:\Data\form_future\future.csv')  
ff = ff.fillna(0)
ff = ff[ff.volume!=0]

a = ff.code.groupby(ff.month_label)
bb = a.apply(lambda x: sorted(list(set(list(x)))))
bb = dict(bb)

import xlwt
workbook = xlwt.Workbook(encoding = 'utf-8')
worksheet = workbook.add_sheet('My Worksheet')
k = list(bb.keys())
for i in range(len(k)):
    for j in range(1+len(bb[k[i]])):
        if j==0:
            worksheet.write(i, j, label=k[i])
        else:
            worksheet.write(i, j, label=bb[k[i]][j-1])
workbook.save('Excel_test.xls')



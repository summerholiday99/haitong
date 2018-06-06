# -*- coding: utf-8 -*-
"""
已有名单，算收益率
#心得:通道是每个时刻的净值；收益率文件是每个时间段的收益率
"""

import pandas as pd 
import matplotlib.pyplot as plt 
import  datetime as dt
import numpy as np
#%%时间范围(调用6月名单, 用7月收益率算)
ff = pd.read_csv(r'E:\Data\form_future\future.csv')
month_list = sorted(list(set(list(ff['month_label']))))
del ff

#%%名单df
scope = 7
trans = {}
path = r'C:\Users\lenovo\Desktop\Master\金融衍生产品吴冲锋\期末大论文\factor.xlsx'
for i in range(len(list(month_list[:-1]))): 
    print(month_list[i])
    a = pd.read_excel(path, sheetname=month_list[i])
    trans[month_list[i]] = list(a.loc[(a.index<scope)|(a.index>=len(a)-scope),'code'])
code_df = pd.DataFrame(trans).T
#code_df.to_excel(str(scope)+'持仓名单.xlsx')

#%% 做通道 收益还不是一个个填上去的  做空的收益率怎么填?

#读取处理收益文件   revdata['PCT_CHG']
revdata = pd.read_excel(r'E:\Data\form_future\future_daily.xlsx')
revdata = revdata[revdata.VOLUME!=0]
revdata['number'] = revdata.time.apply(lambda x:10000*x.year+100*x.month+x.day)
revdata = revdata[(revdata.number>=20150701)&(revdata.number<=20180531)]
#读取交易日(用来搭建分段excel)
cd = pd.read_csv(r'E:\Data\htsc\ic500-support\全交易日.csv', engine= 'python')
tradeday = cd[(cd.number>=20150701)&(cd.number<=20180531)]

def portrev(tradeday, code_df, revdata):
    
    #把交易时间分成很多段,准备为纵轴
    fd = list(tradeday.loc[tradeday['month-rank']==1,'time'])#没问题
    ttime = list(tradeday.time)#ttime前面tradeday已近挑过了
    point = [ttime.index(i) for i in fd]
    design = []
    for i in range(len(point)):   #tradeday列表切片操作
        if i == len(point)-1: 
            design.append(ttime[point[i]:])
        else:
            design.append(ttime[point[i]:point[i+1]])      
    
    #在收益率信息里, 用0,1,2,3标属于的段数 (每支股票的index都是0到1923吗? nope)
    revdata['period'] = 0
    revdata['ii'] = revdata.time.groupby(revdata.code).rank()-1 #rank从1开始排,python从0开始取
    for z in range(len(point)):
        if  z==len(point)-1:
            revdata.loc[(revdata.ii>=point[z]),'period'] = z
        else:
            revdata.loc[(revdata.ii>=point[z])&(revdata.ii<point[z+1]),'period'] = z
    
    #用code_df和每段时间做出df 纵轴时间,横轴通道 (这里要修改)
    #算纯多头
    frames = [] 
    for i in range(len(design)):
        print(design[i][0])
        a = pd.DataFrame(index=design[i], columns=range(0,7)) #创建一个空df
        for j in range(0,7):
            try:
                a.iloc[:,j] = list(revdata.loc[(revdata.period==i)&(revdata.code==code_df.iloc[i,7+j]),'PCT_CHG']) #code名
            except:
                a.iloc[:,j] = [0]*len(design[i])
        frames.append(a)
    
    #计算收益率 每个矩阵算 最后pd.concat(frames)
    rights = [1]
    new_frames = []
    for i in range(len(frames)):
        temp = frames[i]
        temp = 1 + temp/100   
        tt = temp.cumprod()
        new_frames.append(tt)
        rights.append(rights[-1]*tt.iloc[-1,:].mean()) #初始资金
    for i in range(len(new_frames)):
        new_frames[i] = new_frames[i]*rights[i]    
    port = pd.concat(new_frames)
    port['port_net_value'] = port.T.mean()
    port.dropna(axis=0, how='all', inplace=True) 
    return port


time = np.array([dt.datetime.strptime(i, "%Y/%m/%d")  for i in port_qian7.index])
port_qian7.index = time
port_hou7.index = time

port_qian7.port_net_value

plt.style.use("ggplot") 
plt.plot(port_qian7)
plt.plot(port_hou7)

#['bmh', 'classic', 'dark_background', 'fast', 'fivethirtyeight', 'ggplot', 'grayscale', 'seaborn-bright', 'seaborn-colorblind', 'seaborn-dark-palette', 'seaborn-dark', 'seaborn-darkgrid', 'seaborn-deep', 'seaborn-muted', 'seaborn-notebook', 'seaborn-paper', 'seaborn-pastel', 'seaborn-poster', 'seaborn-talk', 'seaborn-ticks', 'seaborn-white', 'seaborn-whitegrid', 'seaborn', 'Solarize_Light2', '_classic_test']

plt.style.use("seaborn-whitegrid") 
fig = plt.figure()
ax1 = fig.add_subplot(111)
ax1.plot(port_qian7.port_net_value,alpha=0.6)
ax1.plot(port_hou7.port_net_value,alpha=0.6)
ax2 = ax1.twinx()
ax2.plot(port_qian7.port_net_value-port_hou7.port_net_value,color='red',alpha=0.75)

plt.plot(port_qian7.port_net_value)
plt.plot(port_hou7.port_net_value)
plt.plot(port_qian7.port_net_value-port_hou7.port_net_value)
plt.tight_layout()

#%%

a = port_qian7.port_net_value-port_hou7.port_net_value






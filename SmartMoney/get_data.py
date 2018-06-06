# -*- coding: utf-8 -*-
""" To do
读取

"""
import WindPy as wind
import pandas as pd
import matplotlib.pyplot  as plt
wind.w.start()

#%%下载分钟数据
code = ['M.DCE','Y.DCE','A.DCE','AG.SHF','AL.SHF','AP.CZC',\
'AU.SHF','B.DCE','BB.DCE','FB.DCE','BU.SHF','C.DCE',\
'CF.CZC','CS.DCE','CU.SHF','CY.CZC','FG.CZC','FU.SHF',\
'HC.SHF','I.DCE','J.DCE','JD.DCE','JM.DCE','JR.CZC',\
'L.DCE','LR.CZC','MA.CZC','NI.SHF','OI.CZC','P.DCE',\
'PB.SHF','PM.CZC','PP.DCE','RB.SHF','RI.CZC','RM.CZC',\
'RS.CZC','RU.SHF','SF.CZC','SM.CZC','SN.SHF',\
'SR.CZC','TA.CZC','V.DCE','WH.CZC','WR.SHF','ZC.CZC','ZN.SHF']
#you = code[0:38] 

code = code[38:]
path = 'E:\Data\future'
future = {}
for i in range(len(code)):    
    a = wind.w.wsi(code[i], "volume,amt,pct_chg,close",\
          "2012-01-01 09:00:00", "2018-06-01 09:12:00", 'periodstart=09:00:00;periodend=15:00:10')
    if a.ErrorCode!=0:
        break
    future[code[i]] = a 
    print (code[i])

for i in list(future.keys()): #只有一只股票一只股票地倒
    k = future[i]
    invoke = {m:n for m,n in [(k.Fields[h],k.Data[h]) for h in range(len(k.Fields))]}
    invoke['time'] = k.Times
    invoke['code'] = i
    temp = pd.DataFrame(invoke, columns=['code','time'] + k.Fields)
    temp.to_excel(i[:-4]+".xlsx",index=False)

#%%下载收益率序列
wind.w.start()

wind_code = ['A.DCE','AG.SHF','AL.SHF','AP.CZC','AU.SHF','BU.SHF','C.DCE',\
 'CF.CZC','CS.DCE','CU.SHF','CY.CZC','FG.CZC','HC.SHF','I.DCE','J.DCE',\
 'JD.DCE','JM.DCE','L.DCE','M.DCE','MA.CZC','NI.SHF','OI.CZC',\
 'P.DCE','PB.SHF','PP.DCE','RB.SHF','RM.CZC','RU.SHF','SM.CZC','SN.SHF',\
 'SR.CZC','TA.CZC','V.DCE','Y.DCE','ZC.CZC','ZN.SHF']

future_state = {}
for i in range(len(wind_code)):    
    a = wind.w.wsd(wind_code[i], "pre_close,open,high,low,close,volume,amt,pct_chg,turn,oi", \
                "2015-06-01", "2018-06-03", "")
    if a.ErrorCode!=0:
        break
    future_state[wind_code[i]] = a 
    print(wind_code[i])

frames_state = []
for i in wind_code: #只有一只股票一只股票地倒
    k = future_state[i]
    invoke = {m:n for m,n in [(k.Fields[h],k.Data[h]) for h in range(len(k.Fields))]}
    invoke['time'] = k.Times
    invoke['code'] = i
    temp = pd.DataFrame(invoke, columns=['code','time'] + k.Fields)
    frames_state.append(temp)

future_df = pd.concat(frames_state)
future_df.to_excel("future_daily.xlsx", index=False)
len(set(list(future_df.code)))

#%%
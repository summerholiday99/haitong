# -*- coding: utf-8 -*-

"""
1.做出月数据
def month_cpt(month_list, objm)
def one_month(om, tm_code)
2.复权、时间格式调整(只需要分钟涨跌幅暂时不用 ♪(＾∀＾●)ﾉ)
#整体回测从2010年开始  因为股票数据从20100104开始
#一个月中如201001 有的股票excel文件不存在，无法处理
"""

import pandas as pd
import datetime as dt

#全局变量
month_list = pd.read_excel(r'D:\Data\ic500-support\全每月中证500成分.xlsx')
split_point = pd.read_excel(r'D:\Data\ic500-support\全每月最后10天.xlsx')
#E:\Data\htsc\ic500-support

def one_month(om,tm_code,tp):    
    pick = []
    omit = pd.DataFrame(columns=range(0,500),index=[om])
    for i in range(len(tm_code)):
        code = tm_code[i]
        try:
            one_stock = pd.read_csv('D:\\Data\\kline'+'\\'+code+'.csv', encoding='gbk')    
            one_stock.drop([0], axis=0, inplace=True)                              #删除多余英文表头
            one_stock['日期'] = one_stock['日期'].astype(int)
            pick.append(one_stock[(one_stock['日期']>=tp[0])&(one_stock['日期']<=tp[1])])
            omit.iloc[0,i] = 2400 - pick[-1].groupby(pick[-1]['日期']).size().sum()
        except:
            omit.iloc[0,i] = 2400                                              #通过这个来判定这天数据是否缺失
        print (i)
    tmdata = pd.concat(pick)
    return tmdata,omit

def month_cpt(month_list, om_list):
    
    omit_sheet = []
    for j in range(len(om_list)):
        #准备输入参数
        om = om_list[j]                                                        #哪一个月
        print(om)
        start = list(split_point.loc[split_point.screen_label==om,'start_num'])
        end = list(split_point.loc[split_point.screen_label==om,'end_num'])
        tp = [start[0],end[0]]
        tm_code = month_list.loc[om,:].apply(lambda x:x[0:6])                  #哪些code
        #执行函数
        tmdata, omit = one_month(om, tm_code, tp)
        omit_sheet.append(omit)
        #转换为int方便后续计算,这四个都有可能出问题
        
        try:
            tmdata['成交量'] = tmdata['成交量'].astype(float)
        except:
            a = list(tmdata['成交量'])
            for i in range(len(a)):
                try:
                    a[i] = float(i)    
                except:
                    a[i] = a[i-1]
            tmdata['成交量'] = a
        
        try:
             tmdata['成交额'] = tmdata['成交额'].astype(float)         
        except:
            a = list(tmdata['成交额'])
            for i in range(len(a)):
                try:
                    a[i] = float(i)    
                except:
                    a[i] = a[i-1]
            tmdata['成交额'] = a
        
        try:
            tmdata['收盘价'] = tmdata['收盘价'].astype(float) 
        except:
            a = list(tmdata['收盘价'])
            for i in range(len(a)):
                try:
                    a[i] = float(i)    
                except:
                    a[i] = a[i-1]
            tmdata['收盘价'] = a
        
        try:
            tmdata['开盘价'] = tmdata['开盘价'].astype(float)
        except:
            a = list(tmdata['开盘价'])
            for i in range(len(a)):
                try:
                    a[i] = float(i)    
                except:
                    a[i] = a[i-1]
            tmdata['开盘价'] = a
        #计算分钟内涨跌幅
        tmdata['pct_change'] = (tmdata['收盘价'] - tmdata['开盘价'])/tmdata['开盘价']
        tmdata = tmdata.drop(columns = ['开盘价', '最高价', '最低价', '收盘价', '成交笔数', '持仓量/IOPV/利息'])
        #时间的整理(耗时)
        tmdata['日期'] = tmdata['日期'].astype(str)
        tmdata['时间'] = tmdata['时间'].astype(str) 
        tmdata['newtime'] = tmdata['日期'] + tmdata['时间'].apply(lambda x:x[:-3])
        try:
            tmdata['newtime'] = tmdata['newtime'].apply(lambda x: dt.datetime.strptime(x,"%Y%m%d%H%M%S"))
        except:
            a = list(tmdata['newtime'])
            for i in range(len(a)):
                try:
                    a[i] = dt.datetime.strptime(a[i],"%Y%m%d%H%M%S")
                except:
                    a[i] = a[i-1] + dt.timedelta(minutes=1)     
        tmdata = tmdata.drop(columns = ['日期', '时间'])
        tmdata.columns = ['code','name','volume','amount','pct_change','time']
        #Excel的输出(耗时)
        tmdata.to_csv(om + '.csv', encoding='gbk', index=False)
    return omit_sheet

#把错误找出来
om_list = [month_list.index[i] for i in range(81,135)]

omit_sheet = month_cpt(month_list, om_list)
omit_df = pd.concat(omit_sheet)
omit_df.to_excel('omit.xlsx')

# -*- coding: utf-8 -*-
#成交量包含集合竞价和连续竞价的 集合竞价9.25有，15.00有
'''
主要目的是计算，并输出股票列表
#1就是要留下的  0就是要毙掉的
-------------------------------------------------------------------
坑纪念
df[df.code==i]['停牌处理'] = 1  达不到你想要的意思,赋值在另一个地方去了，不要这么写，用loc
__main__:1: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead
-------------------------------------------------------------------
经验：
#apply函数没法直接用可以定义个函数再用
def str_datetime(k): return datetime.datetime.strptime(k, "%Y-%m-%d %H:%M:%S") 
ic500['time'] = ic500['time'].apply(str_datetime)
从效率上看太慢太垃圾
pd.to_datetime(df.time)  str--Timestamp用这个!

#筛选不一定非要重新写一遍index可以..
for i in list(halt.index):
#mutable对象在原值上修改的,所以不打印
a.extend([1,2,3])
#df命名方式 新创列也可以这么命名...才怪
df.agg_auc = 1
#按住ctrl点函数名跳跃
#猜猜哪种更快？ 第二种
df['time_date'] = [i.date() for i  in list(df['time'])]
df['time_date'] = df.time.apply(lambda x:x.date())
starttime = dt.datetime.now()
df['time_date'] = df.time.apply(lambda x:x.date())
endtime = dt.datetime.now()
print (endtime - starttime)
'''

import math
import datetime as dt
import pandas as pd 
import warnings
import copy
warnings.filterwarnings("ignore")

#import matplotlib.pyplot as plt

#%%------------处理数据
ff = pd.read_csv(r'E:\Data\form_future\future.csv')
ff = ff.fillna(0)
ff = ff[ff.volume!=0] #这两步一定要有的
ff['time'] = pd.to_datetime(ff.time) #高效率,太棒啦

#%%------------计算S值及Q值

def S_comp_positive(df,smart_pct):
    '''输入:ic500
    输出:有值，有smart标记的ic500
    '''
    #df['abs_pct'] = df['pctchange'].apply(abs)
    df = copy.deepcopy(df)
    df['sqrt_v'] = df['volume'].apply(math.sqrt) 
    df['S'] = df['pctchange']/df['sqrt_v'] #前面剔除了0成交量的数据，应该不会有NaN    
    
    #计算累积成交量 (这里按S值排了顺序的, false代表大的排第一)
    df.sort_values(by=['code','S'], ascending=[True, False], inplace=True)    
    volume_groupbycode = df.volume.groupby(df.code)   
    df['volume_acc'] = volume_groupbycode.cumsum() #OK经检验正确        
    #对累计成交量分组排名, 把每组最终的累计成交量 (第一名,最大的那个数) 挑出来
    volume_acc_groupbycode = df.volume_acc.groupby(df.code)
    df['volume_acc_rank'] = volume_acc_groupbycode.rank(ascending=False)     
    
    #计算每个成交量占总成交量百分比
    l = list(df.loc[df['volume_acc_rank']==1,'volume_acc'])  #code和累计成交量的一个对应关系
    l2 = list(df.groupby(df.code).size())
    sta = []
    for i in range(len(l)):
        sta.extend([l[i]]*l2[i])    
    df['final_volume_acc'] = sta 
    df['acc_pct'] = df['volume_acc']/df['final_volume_acc']
    df.drop(columns=['volume_acc','volume_acc_rank','final_volume_acc'], inplace=True)
    #和smart_pct比较
    df['SmartOrNot'] = 1                                                                                       
    df.loc[df['acc_pct']>smart_pct, 'SmartOrNot'] = 0
    return df

def S_comp_negative(df,smart_pct):
    '''输入:ic500
    输出:有值，有smart标记的ic500
    '''
    #df['abs_pct'] = df['pctchange'].apply(abs) 
    df = copy.deepcopy(df)
    df['sqrt_v'] = df['volume'].apply(math.sqrt) 
    df['S'] = df['pctchange']/df['sqrt_v'] #前面剔除了0成交量的数据，应该不会有NaN    
    
    #计算累积成交量 (这里按S值排了顺序的, false代表大的排第一)
    df.sort_values(by=['code','S'], ascending=[True, True], inplace=True)    
    volume_groupbycode = df.volume.groupby(df.code)   
    df['volume_acc'] = volume_groupbycode.cumsum() #OK经检验正确        
    #对累计成交量分组排名, 把每组最终的累计成交量 (第一名,最大的那个数) 挑出来
    volume_acc_groupbycode = df.volume_acc.groupby(df.code)
    df['volume_acc_rank'] = volume_acc_groupbycode.rank(ascending=False)     
    #计算每个成交量占总成交量百分比
    l = list(df.loc[df['volume_acc_rank']==1,'volume_acc'])  #code和累计成交量的一个对应关系
    l2 = list(df.groupby(df.code).size())
    sta = []
    for i in range(len(l)):
        sta.extend([l[i]]*l2[i])    
    df['final_volume_acc'] = sta 
    df['acc_pct'] = df['volume_acc']/df['final_volume_acc']
    df.drop(columns=['volume_acc','volume_acc_rank','final_volume_acc'], inplace=True)
    #和smart_pct比较
    df['SmartOrNot'] = 1                                                                                       
    df.loc[df['acc_pct']>smart_pct, 'SmartOrNot'] = 0
    return df


def v_comp(dfp, dfn):
    '''输入：正常股票list, ic500 DataFrame
    输出：正常股票因子值 DataFrame
    '''
    investi = sorted(list(set(list(dfp.code)))) #正常股票list
    posi = []
    nega = []
    for i in range(len(investi)): 
        p_part = dfp[(dfp.code==investi[i])&(dfp['SmartOrNot']==1)] 
        p_part['weight'] = (10 - p_part['inmonth_rank'])/55
        p_part['timeweightedaveragevolume'] = p_part['volume']*p_part['weight']
        posi.append(p_part['timeweightedaveragevolume'].sum())
    for i in range(len(investi)): 
        n_part = dfn[(dfn.code==investi[i])&(dfn['SmartOrNot']==1)] 
        n_part['weight'] = (10 - n_part['inmonth_rank'])/55
        n_part['timeweightedaveragevolume'] = n_part['volume']*n_part['weight']
        nega.append(n_part['timeweightedaveragevolume'].sum())
           
    final = [(posi[i]-nega[i])/(posi[i]+nega[i]) for i in range(len(investi))]   
    factor = pd.DataFrame({'code':investi}) #一只股票就一行
    factor['V_value'] = final
    
    factor['rank'] = factor.V_value.rank(ascending=False)
    factor.sort_values('V_value', inplace=True)
    return factor

#%%-----------主程序 
#获得完整time_para
month_list = sorted(list(set(list(ff['month_label']))))

writer = pd.ExcelWriter('factor.xlsx')
for time_para in month_list:    
    print(time_para)
    obj = ff[ff['month_label']==time_para] 
    objp = S_comp_positive(obj, smart_pct=0.1)
    objn = S_comp_negative(obj, smart_pct=0.1)
    factor = v_comp(objp, objn)
    factor.to_excel(writer,sheet_name=time_para, index=False)
writer.save()
#%% 挑选股票导出因子数据
#def output_detail(inv_code, pic500, ic500):
#    '''输入: 股票code list
#    输出: 打印信息
#    '''    
#    ic500['lkey'] = ic500.index
#    pic500['rkey'] = pic500.index
#    a = pd.merge(ic500, pic500, left_on='lkey', right_on='rkey', how='outer')
#    aim_columns = ['code_x', 'time_x', 'volume_x', 'amount_x',
#       'agg_auc_x', 'halt_x', 'risemuch_x', 'volume_mini_x',
#       'lock_x', 'abs_pct', 'sqrt_v', 'S', 'final_volume_acc',
#       'acc_pct', 'SmartOrNot']
#    writer = pd.ExcelWriter('detail_info.xlsx')  
#    for i in range(len(inv_code)):
#        jk = inv_code[i]
#        d1 = a.loc[a.code_x==jk, aim_columns]
#        d2 = factor.loc[factor.code==jk,:]
#        d1.to_excel(writer, sheet_name=inv_code[i]+'分钟数据', index=False)
#        d2.to_excel(writer, sheet_name=inv_code[i]+'汇总计算', index=False)
#    writer.save()
#    return 
 
#inv_code = ['002405.SZ','300115.SZ'] #输入股票
#output_detail(inv_code, pic500, ic500)


#%% 统计聪明分钟分布
import matplotlib.pyplot as plt
spic500 = pic500[pic500.SmartOrNot==1] 
spic500['minute'] = [i[-8:] for i in list(spic500['time'].astype(str))]
group_smart = spic500.groupby(spic500.minute)
a = list(group_smart.size())
plt.plot(a)

#%%
group_smart = pic500.SmartOrNot.groupby(pic500.code) 
a = group_smart.size()
k = group_smart.sum()
p = k/a

import numpy as np
plp = np.array(p)

plt.style.use('ggplot')
colors = np.random.rand(460)
plt.scatter(list(range(0,460)),  plp, s=50 , c=colors, alpha=0.5)
plt.hlines(np.mean(plp), 0, 460, color='red', alpha=0.5,linestyle=':')  
plt.show()

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
warnings.filterwarnings("ignore")

#import matplotlib.pyplot as plt
#%%------------处理数据
#新的数据没有9.25和15.00
#def time_process(df):
#    '''把事情做漂亮！--检验OK
#    输入：ic500，挑选的月份time_para
#    输出：ic500
#    '''
#    #标记集合竞价时间数据
#    df['time_minute'] = [i[-8:] for i in list(df['time'].astype(str))]
#    df['exchange'] = [i[-2] for i in list(df.code)]
#    df['agg_auc'] = 1
#    df.loc[df.time_minute =='09:25:00','agg_auc'] = 0
#    df.loc[(df.exchange=='SZ')&(df.time_minute=='15:00:00'),'agg_auc'] = 0
#    df.drop(columns=['time_minute','exchange'], inplace=True)
#    return df

def halt_process(df_state, df, halt_line):
    '''输入:--检验OK
    ic500
    ic500_state 
    ----------------------
    输出:
    ic500 多一列halt_label (停牌过多的股票删除，有停牌但不多(根据halt_line确定)的股票，停牌时间删除)  
    halt_info 包含 停牌股票 停牌天数 停牌原因 一个停牌天数小于1的normal_stock列表
    '''        
    #准备输出的停牌详细情况表格，以供参考(下午停牌暂不考虑)
    ori = df_state[['code','time','TRADE_STATUS','SUSP_REASON']][df_state['TRADE_STATUS']!='交易']
    groupbycode = ori.groupby(df_state['code'])                       # 分组统计
    halt = pd.DataFrame(groupbycode.size(), columns=['停牌天数'])     # 每支停过牌的股票停几天                                        
    halt['具体时间'] = [list(i[1]) for i in list(groupbycode.time)]   # 哪几天停牌,把停牌天数都取出来
    halt['停牌原因'] = [list(set(list(i[1]))) for i in list(groupbycode.SUSP_REASON)]
    halt['交易状态'] = [list(set(list(i[1]))) for i in list(groupbycode.TRADE_STATUS)]    
    
    #做停牌信息列
    df['halt'] = 1  #df分钟数据，要留的等于1
    df['time_date'] = df.time.apply(lambda x: x.date()) #这种循环就很慢
    for i in list(halt.index):
        if halt.loc[i,'停牌天数']>halt_line:             #停牌天数大于halt_line = 1天
            df.loc[df.code==i, 'halt'] = 0
        elif halt.loc[i,'停牌天数']<=halt_line:          #停牌天数小于等于halt_line = 1天的，该股票那一天的留下，其余的毙掉
            df.loc[(df.code==i)&(df.time_date==halt.loc[i,'具体时间'][0].date()),'halt'] = 0
            #这里就直接删除只有1天的,至于下午停牌的没管
    df.drop(columns=['time_date'], inplace=True)          
    halt_info = [halt, list(set(list(df.loc[df['halt']==1,'code'])))]    
    return df,halt_info

def rise_process(df, deleteratio, rise_window):
    '''
    输入：path,ic500,rise_window
    输出：删除涨幅过大的股票数据后的df
    '''
    #用df_state分析前期涨幅过高的五分之一样本 前期涨幅过大的样本(当月数据涨幅在前20%的股票剔除掉)
    #总共500支 停牌40支每组多少支?剔除了涨幅前20%的股票,每组多少支? 
    df_state = pd.read_excel('D:\Data\ic500-support'+'\\'+'ic500_state.xlsx')#重新读取state 因为要算月的收益率
    
    #首先取ic500_state当月数据
    df_state['year'] = df_state.time.apply(lambda x: x.year) 
    df_state['month'] = df_state.time.apply(lambda x: x.month) 
    inv_time = [int(time_para[:4]),int(time_para[-2:])]
    inv = df_state[(df_state.year==inv_time[0])&(df_state.month==inv_time[1])] 

    tt = inv.time.groupby(inv.code)    
    if rise_window == 'month':
        #前期表示一个月: 当月第一天开OPEN  当月最后一天收CLOSE (数据有缺，比如2018-03就是499) 
        inv['po_seq'] = list(tt.rank(method='first'))
        inv['ne_seq'] = list(tt.rank(method='first',ascending = False))
        a = inv[inv['po_seq']==1].OPEN
        b = inv[inv['ne_seq']==1].CLOSE
    elif rise_window == 'twoweek':
        inv['seq'] = list(tt.rank(method='first', ascending = False))
        a = inv[inv['seq']==1].CLOSE
        b = inv[inv['seq']==10].OPEN
    elif rise_window == 'week':
        inv['seq'] = list(tt.rank(method='first', ascending = False))
        a = inv[inv['seq']==1].CLOSE
        b = inv[inv['seq']==5].OPEN
        
    #前20%的股票列表的code名单 涨幅最大的前五分之一 这个涨字值得商榷
    rise = pd.DataFrame({'code':list(inv.loc[a.index,'code']),'open':list(a),'close':list(b)})
    rise['change'] = (rise.close - rise.open)/rise.open
    rise.sort_values('change',ascending = False,inplace=True)
    rise['rank'] = list(rise.change.rank(ascending = False,pct=True))#用百分比排的名
    delete_list = list(rise.loc[rise['rank']<deleteratio,'code'])    #
    
    #删除涨幅过大的股票(0的不要)
    df['risemuch'] = list((-df.code.isin(delete_list)).astype('int'))
    return df,rise

def volume_process(df, extra_drop):
    '''
    输入:ic500 extra_drop 去除成交量为0的时段后, 剩余还要去除的成交量的比例
    输出:每支股票 考察时段  前80%交易量的时间
    '''
    #先删除成交量为0的，这个是真的可以删，因为S根本无法排序 删除的不需要用标记了
    #这里删除的包括（1）停牌的（2）本来是0的（3）数据缺失的
    df = df.fillna(0) 
    df = df[df.volume!=0]  
    #在这之上再删除成交量最低的20%(extra_drop)对因子收益影响不大   
    ingroup = df['volume'].groupby(df.code) 
    df['volume_rank'] = ingroup.rank(method='first', pct=True)#越小的排名越前面 百分比排序是等顺序增长的
    df['volume_mini'] = 1 
    df.loc[df['volume_rank']<=extra_drop,'volume_mini'] = 0
    df.drop(columns='volume_rank', inplace=True)
    return df

def read_and_process(path, time_para):
    print('数据处理')
    #取ic500待考察时间段数据
    df = pd.read_csv(path + '\\' + time_para +'.csv', encoding='gbk') 
    splitp = pd.read_excel(r'D:\Data\ic500-support\全每月最后10天.xlsx')  #最后10天的名单还是一直要用的，暂时写死在这里
    df.time = pd.to_datetime(df.time)  #ic500.csv时间读入是str转timestamp, 需要的
#    ms_start = list(splitp.loc[splitp['screen_label'] == time_para,'start'])[0]
#    ms_end = list(splitp.loc[splitp['screen_label'] == time_para,'end'])[0] + dt.timedelta(hours=16)
#    df = df[(ms_start<df.time)&(df.time<ms_end)]
   
    #取ic500_state对应数据(2018-03缺了一支股票的数据)
    df_state = pd.read_excel(r'D:\Data\ic500-support\全中证500状态.xlsx')
    start = list(splitp.loc[splitp['screen_label'] == time_para, 'start'])[0]
    end = list(splitp.loc[splitp['screen_label'] == time_para, 'end'])[0]
    df_state = df_state[(start<=df_state.time)&(df_state.time<=end)]  #正常的,有很多股票没有上市所以很多NaN
    
    #主要处理函数
    #df = time_process(df) 
    df, halt_info = halt_process(df_state, df, halt_line=0) #这里标记了停牌但没有删除停牌
    #df, rise = rise_process(df, rise_window='month', deleteratio=0.2)
    df = volume_process(df, extra_drop=0) #这里是唯一删了数据的地方        
    return df, halt_info #, rise

def data_pick(df):
    #在这里需要按要求删除一些数据
    df['lock'] = 0
    df.loc[(df.halt==1),'lock'] = 1  #(df.volume_mini==1) 这个也暂时不用
    pdf = df[df.lock==1] 
    return df,pdf

#%%------------计算S值及Q值

def S_comp(df, smart_pct=0.2):
    '''输入:ic500
    输出:有值，有smart标记的ic500
    '''
    print('计算S值')
    df['abs_pct'] = df['pct_change'].apply(abs) 
    df['sqrt_v'] = df['volume'].apply(math.sqrt) 
    df['S'] = df['abs_pct']/df['sqrt_v']    #前面剔除了0成交量的数据，应该不会有NaN
    #计算累积成交量 (这里按S值排了顺序的, false代表大的排第一)
    df.sort_values(by=['code','S'], ascending=[True, False], inplace=True)    
    volume_groupbycode = df.volume.groupby(df.code)   
    df['volume_acc'] = volume_groupbycode.cumsum()    #OK经检验正确    
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

def Q_comp(df, halt_info):
    '''输入：正常股票list, ic500 DataFrame
    输出：正常股票因子值 DataFrame
    '''
    print('计算Q值')
    investi = halt_info[1]  #正常股票list
    Q_value = []
    smart_vwap = []
    all_vwap = []
    
    for i in range(len(investi)): #500支票一支一支地算
        smart_part = df[(df.code==investi[i])&(df['SmartOrNot']==1)] 
        all_part = df[df.code==investi[i]]
        k1 = smart_part.amount.sum()/smart_part.volume.sum()    
        smart_vwap.append(k1)
        k2 = all_part.amount.sum()/all_part.volume.sum() 
        all_vwap.append(k2)
        Q_value.append(k1/k2)
    
    factor = pd.DataFrame({'code':investi}) #一只股票就一行
    factor['name'] = wind.w.wss(list(factor.code), "sec_name").Data[0] #这个应该一开始下好的，后面改掉
    factor['smart_vwap'] = smart_vwap  
    factor['all_vwap'] = all_vwap
    factor['Q_value'] = Q_value
    factor['rank'] = factor.Q_value.rank()
    factor.sort_values('Q_value', inplace=True)
    return factor

#%%------------主程序 
import WindPy as wind
wind.w.start()    

path = r'D:\Data\form'
month_list = pd.read_excel(r'D:\Data\ic500-support\全每月中证500成分.xlsx')
om_list = list(month_list.index)

for i in range(131,134): 
    print (om_list[i])
    time_para = om_list[i]
    ic500, halt_info = read_and_process(path, time_para)
    ic500, pic500 = data_pick(ic500) #pic500删除了不需要的数据
    #halt_info[0].to_excel('停牌信息.xlsx', index=True)
    #rise.to_excel('涨跌信息.xlsx', index=False)
    pic500 = S_comp(pic500, smart_pct=0.2)
    factor = Q_comp(pic500, halt_info)
    factor.to_excel(time_para + 'SmartFactor.xlsx', index=False)
    #tt = factor[factor['rank']<=100] #这里按index重新排序
    #tt.to_excel(time_para + 'SmartFactor.xlsx', index = False)
    
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
# 
#inv_code = ['002405.SZ','300115.SZ'] #输入股票
#output_detail(inv_code, pic500, ic500)
#

##%% 统计聪明分钟分布
#
#spic500 = pic500[pic500.SmartOrNot==1] 
#spic500['minute'] = [i[-8:] for i in list(spic500['time'].astype(str))]
#group_smart = spic500.groupby(spic500.minute)
#a = list(group_smart.size())
#plt.plot(a)
##%%
#group_smart = pic500.SmartOrNot.groupby(pic500.code) 
#a = group_smart.size()
#k = group_smart.sum()
#p = k/a
#import numpy as np
#plp = np.array(p)
#
#plt.style.use('ggplot')
#colors = np.random.rand(460)
#plt.scatter(list(range(0,460)),  plp, s=50 , c=colors, alpha=0.5)
#plt.hlines(np.mean(plp), 0, 460, color='red', alpha=0.5,linestyle=':')  
#plt.show()

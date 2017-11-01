#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 21 08:52:09 2017

@author: Carnec
"""

import pandas as pd
import numpy as np
import jsonlines
import requests
import json
from urllib.request import urlopen
import matplotlib.pyplot as plt

#to google sheets
import gspread
from oauth2client.client import SignedJwtAssertionCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

#execute code every....
import schedule
import time

''' Parse NDJSON From Url '''

#response1 = urllib.request.urlopen('https://s3-eu-west-1.amazonaws.com/data-analyst-challenge/invoices_per_customer.json')
#url_invoice = response1.read().decode('utf-8')
#response2 = urllib.request.urlopen('https://s3-eu-west-1.amazonaws.com/data-analyst-challenge/information_customer.json')
#url_information = response2.read().decode('utf-8')

''' Alternatively from Paths '''

path_invoice = r'/Users/Carnec/Desktop/Business_Analytics/Codacy_challenge/invoices_per_customer.json'
path_information = r'/Users/Carnec/Desktop/Business_Analytics/Codacy_challenge/information_customer.json'

''' NDJSON to pandas '''
#
#with open(url_invoice) as f:
#    df_invoice = pd.DataFrame(json.loads(line) for line in f)
#    
#with open(url_information) as f:
#    df_information = pd.DataFrame(json.loads(line) for line in f)  

with open(path_invoice) as f:
    df_invoice = pd.DataFrame(json.loads(line) for line in f)
    
with open(path_information) as f:
    df_information = pd.DataFrame(json.loads(line) for line in f)    

''' Setting up months of interest and start/end timestamp '''

day = 86400
month = 2592000
df_invoice_index = df_invoice.set_index(['timestampPaid'])
df_invoice_index = df_invoice_index.sort_index(ascending=True)

full_period = (df_invoice_index.index[-1] - df_invoice_index.index[1])
number_days = full_period/day
number_months = int(full_period/month) #round down
start = df_invoice_index.index[0]
end = df_invoice_index.index[-1]

''' Timestamp to month - using bins!!''' 
montharray = np.arange(start-1,end,month)
montharray = np.append(montharray,end)
monthbinarray = np.arange(15)
df_invoice['month'] = pd.cut(df_invoice.timestampPaid, montharray, labels = monthbinarray )

df_invoice = df_invoice.sort_values('timestampPaid').reset_index()
del df_invoice['index']
#df_invoice = df_invoice.set_index(['timestampPaid'])
#df_invoice = df_invoice.sort_index(ascending=True)

        

''' Exploratory Analysis '''

len(df_information['customerId']) # Number of unique customers

df_invoice[df_invoice['amountPaid']==0] #Paid zero

df_invoice['amountPaid'].describe() #describe amount paid

# only one session
df_information[df_information['nrSessions']==1]
df_invoice[df_invoice['customerId']==904440]
df_invoice[df_invoice['customerId']==880038]

# paid per customer
df_invoice_monthindex = df_invoice.set_index(['month'])
paid_per_customer1 = df_invoice.reset_index().groupby(['customerId','amountPaid']).sum()

paid_per_customer2 = df_invoice.groupby(['customerId']).sum()
paid_per_customer2.idxmax() #max sum paid by ID 766972

paid_per_customer2.describe()
plt.hist(paid_per_customer2['amountPaid'],bins=10)
plt.xlabel('Paid Sum')
plt.ylabel('Frequency Customers')
plt.title('Histogram Frequency of Paid Sum')
plt.show()

sorted_df_invoice = df_invoice.sort_index(ascending=True)
plt.plot(sorted_df_invoice['amountPaid'])
plt.ylabel('Amount Paid')
plt.xlabel('Timestamp')
plt.title('Amount Paid Over Time')
plt.show()

df_information['nrSessions'].describe()
plt.hist(df_information['nrSessions'],bins=20)
plt.xlabel('Number of Sessions')

''' ID 274354 '''
df_invoice[df_invoice['customerId']==274354] # Paid 9.0 in month 0
df_information[df_information['customerId']==274354] # 5 seats and 179 sessions

''' ID 766972 '''
df_invoice[df_invoice['customerId']==766972] # Paid 10800 and 1800 month 7 
df_information[df_information['customerId']==766972] # 20 seats and 36 sessions

''' Random ID - Data exploration: looks like monthly subscriptions '''
uniqueIDs = df_information.customerId.unique()
np.random.seed(500)
random_ID = np.random.choice(uniqueIDs)
df_invoice[df_invoice['customerId']==random_ID] # Paid 36 from month 8 to 12 
df_information[df_information['customerId']==random_ID] # 1 seat and 12 sessions

np.random.seed(400)
random_ID = np.random.choice(uniqueIDs)
df_invoice.amountPaid[df_invoice['customerId']==random_ID].sum() 
df_invoice[df_invoice['customerId']==random_ID] 
len(df_invoice[df_invoice['customerId']==random_ID])

np.random.seed(400)
random_ID = np.random.choice(uniqueIDs)
df_invoice.amountPaid[df_invoice['customerId']==random_ID].sum() 
df_invoice[df_invoice['customerId']==random_ID] 
len(df_invoice[df_invoice['customerId']==random_ID])

sorted_df_invoice = df_invoice.sort_index(ascending=True)
plt.plot(df_invoice.timestampPaid[df_invoice['customerId']==random_ID],df_invoice.amountPaid[df_invoice['customerId']==random_ID],'ro')
plt.ylabel('Amount Paid')
plt.xlabel('Timestamp')
plt.title('Amount Paid Over Time')
plt.show()

''' * * Calculate MRR - We do not have subscription start/end dates. Looking at random 
cusomterId, subscription (larger payments) seems to have same two digits in timestamp 
since they come exactly one month away from the previous (for example;
1505036659           90.0      703708    13
1507628659           85.1      703708    14
).
We will not include smaller payments which do not fall a month later as we are interested
in monthly RECURRING revenue. * * '''



''' Find first pay timestamp for each customerId '''
df_firstpaylist = [['timestampPaid','customerId']]
for Id in uniqueIDs:    
    for index in range(len(df_invoice['customerId'])):
        if df_invoice['customerId'][index] == Id:
            #print(index,df_invoice['timestampPaid'][index])
            df_firstpaylist.append([df_invoice['timestampPaid'][index],df_invoice['customerId'][index]])
            break
df_firstpay=pd.DataFrame(df_firstpaylist)   
header = df_firstpay.iloc[0]
df_firstpay = df_firstpay[1:]
df_firstpay = df_firstpay.rename(columns = header)

''' return last two digits and calculate most frequent for each user'''
def last_2_digits(n):
    return int(str(n)[-2:])
last_2_digits(766972)

df_firstpay['firstpaylast2'] = df_firstpay.apply(lambda x: last_2_digits(x['timestampPaid']), axis=1)

maxcounts = []
for Id in uniqueIDs: 
    mostfreq = []
    for i in df_invoice.timestampPaid[df_invoice['customerId']==Id]:
        mostfreq.append(last_2_digits(i))
    counts = np.bincount(mostfreq)
    maxcounts.append(np.argmax(counts))

df_lastdigits=pd.DataFrame({'customerId': uniqueIDs,'freqlast2': maxcounts})   

        
''' check if firstpay day is also most frequent by joining on index'''

#first set customerId as Index
df_lastdigits_index = df_lastdigits.set_index(['customerId'])
df_firstpay_index = df_firstpay.set_index(['customerId']) 
lastjoin = df_lastdigits_index.join(df_firstpay_index)
del lastjoin['timestampPaid']
(lastjoin['freqlast2'] == lastjoin['firstpaylast2']).all() 

''' Firstday not always most frequent day - use last 2 digits of most frequent '''
#get first timestamp of most frequent for each Id
df_invoice[df_invoice['customerId']==766972] # Paid 10800 and 1800 month 7 

for Id in uniqueIDs:  
    freqlast2 = None
    freqlast2 = df_lastdigits.freqlast2[df_lastdigits['customerId']==Id]
    for index in range(len(df_invoice['customerId'])):
        if last_2_digits(df_invoice['timestampPaid'][index]) == int(freqlast2):
            df_firstpaylist.append([df_invoice['timestampPaid'][index],df_invoice['customerId'][index]])
            break
df_firstpay=pd.DataFrame(df_firstpaylist)  
header = df_firstpay.iloc[0]
df_firstpay = df_firstpay[1:]
df_firstpay = df_firstpay.rename(columns = header)

''' Remove customers that do not pay one month after start timestamp - 
Ended up using first pay day instead of first day of most frequent!
first start day : 172 customers to remove
most frequent start day: 350 to remove'''

customers_remove = []
for index,row in df_firstpay.iterrows():
    month1 = df_invoice.month[(df_invoice['customerId']==row['customerId'])&(df_invoice['timestampPaid']==row['timestampPaid'])]
    month1 = month1.tolist() 
    month1 = month1[0]+1 # ewwww :'(
    if df_invoice.month[(df_invoice['customerId']==row['customerId'])].isin([month1]).any() == True:
        pass
    else:
        customers_remove.append(row['customerId'])

#check to see if working correctly: missing some but minimal impact          
#for c in customers_remove:
#    print('customer:',c)
#    print(df_invoice[df_invoice['customerId']==c])
  
''' Same method but getting last two digits for each timestamp and getting most frequent '''       

maxcounts = []
for Id in uniqueIDs: 
    mostfreq = []
    for i in df_invoice.timestampPaid[df_invoice['customerId']==Id]:
        mostfreq.append(last_2_digits(i))
    counts = np.bincount(mostfreq)
    maxcounts.append(np.argmax(counts))
  
df_mostfreq_last2=pd.DataFrame({'customerId': uniqueIDs,'freqlast2': maxcounts}) 
    
#return first occurence of last 2 digits
timestamp_start_list = []
timestamp_start_list_month = []
df_first_last2= [['timestampPaid','customerId']]
for Id in uniqueIDs:    
       for index,row in df_invoice[df_invoice['customerId']==Id].iterrows():
           if last_2_digits(row['timestampPaid']) == int(df_mostfreq_last2.freqlast2[df_mostfreq_last2['customerId']==Id]):
               timestamp_start_list.append(row['timestampPaid'])
               timestamp_start_list_month.append(row['month'])
               break

           
''' Create Dataframe to hold info of recurring revenue for each customer '''

''' https://www.cobloom.com/blog/how-to-calculate-saas-arr-mrr -
THINGS TO INCLUDE IN YOUR MRR CALCULATION:

All recurring revenue from customers. This includes monthly subscription fees, and any additional recurring charges for extra users, seats, etc.
Upgrades and downgrades. It's important to track any successful upselling, and any customers that downgrade to a lower-priced package.
All lost recurring revenue. Customers churn, and this reduction in MRR needs to be accounted for.
Discounts. If your customer is on a $200/month package, but pays a discounted monthly fee of $150, their MRR contribution is $150, not $200.
'''
#add month column
df_firstpay_index['month'] = pd.cut(df_firstpay_index.timestampPaid, montharray, labels = monthbinarray )
mrr_df = pd.DataFrame()
mrr_df['startmonth'] = df_firstpay_index['month']
mrr_df['start'] = df_firstpay_index['timestampPaid']
mrr_df['customerId'] = df_firstpay_index.index
mrr_df = mrr_df.reset_index(drop=True)

mrr_df_everyone = pd.DataFrame(columns=('customerId', 'start', 'end','m0','m1','m2','m3','m4','m5','m6','m7','m8','m9','m10','m11','m12','m13','m14'))     
mrr_df_conservative_startmonth=pd.DataFrame({'customerId': uniqueIDs,'start': timestamp_start_list,'startmonth':timestamp_start_list_month}) 
mrr_df_conservative = pd.DataFrame(columns=('customerId', 'start', 'end','m0','m1','m2','m3','m4','m5','m6','m7','m8','m9','m10','m11','m12','m13','m14'))     
mrr_df_firstpay = pd.DataFrame(columns=('customerId', 'start', 'end','m0','m1','m2','m3','m4','m5','m6','m7','m8','m9','m10','m11','m12','m13','m14'))     
mrr_df_largestpayment = pd.DataFrame(columns=('customerId', 'start', 'end','m0','m1','m2','m3','m4','m5','m6','m7','m8','m9','m10','m11','m12','m13','m14'))     

mrr_df = pd.read_csv('/Users/Carnec/Desktop/Business_Analytics/Codacy_challenge/mrr_df.csv')     
mrr_df = mrr_df.drop(mrr_df.columns[[0]],axis=1)

j=0
g=0
for m in range(len(monthbinarray)):
    for i in mrr_df.customerId[mrr_df['startmonth']==m]:
        j=j+1
        mrr_df_conservative.at[j,'customerId'] = i
        mrr_df_conservative.at[j,'start'] = m
        mrr_df_everyone.at[j,'customerId'] = i
        mrr_df_everyone.at[j,'start'] = m
        mrr_df_largestpayment.at[j,'customerId'] = i
        mrr_df_largestpayment.at[j,'start'] = m
        for mm in range(m,len(monthbinarray)):
            mrr_df_everyone.at[j,'m%i'%mm] = df_invoice.amountPaid[(df_invoice['customerId']==i)&(df_invoice['month']==mm)].sum()
            mrr_df_largestpayment.at[j,'m%i'%mm] = df_invoice.amountPaid[(df_invoice['customerId']==i)&(df_invoice['month']==mm)].max()            
        nextmonth = m+1
        if df_invoice.month[df_invoice['customerId']==i].isin([nextmonth]).any(): #only considered to be recurring revenue if pays next month.
            mrr_df_conservative.at[j,'m%i'%m] = df_invoice.amountPaid[(df_invoice['customerId']==i)&(df_invoice['month']==m)].sum()
            maxmonth = df_invoice.month[df_invoice['customerId']==random_ID].max()
            for k in range(nextmonth,maxmonth+1):
                if df_invoice.month[df_invoice['customerId']==i].isin([k]).any():
                    mrr_df_conservative.at[j,'m%i'%k] = df_invoice.amountPaid[(df_invoice['customerId']==i)&(df_invoice['month']==k)].sum()
                else:
                    mrr_df_conservative.at[j,'end'] = k
                    break
    for i in df_firstpay_index.index[df_firstpay_index['month']==m]:
        g = g+1
        mrr_df_firstpay.at[g,'customerId'] = i
        mrr_df_firstpay.at[g,'start'] = int(m)
        last2 = int(df_firstpay_index.firstpaylast2[df_firstpay_index.index==i])
        for item in df_invoice.timestampPaid[(df_invoice['customerId']==i)]:
            if last_2_digits(item) == last2:
                m = df_invoice.month[df_invoice['timestampPaid']==item]
                mrr_df_firstpay.at[g,'m%i'%m] = int(df_invoice.amountPaid[df_invoice['timestampPaid']==item])
            else:
                pass

mrr_df_everyone = mrr_df_everyone.replace(0,np.NaN)
                
mrr_df_everyone.to_csv('/Users/Carnec/Desktop/Business_Analytics/Codacy_challenge/mrr_df_everyone.csv')                
mrr_df_conservative.to_csv('/Users/Carnec/Desktop/Business_Analytics/Codacy_challenge/mrr_df_conservative.csv')                
mrr_df_firstpay.to_csv('/Users/Carnec/Desktop/Business_Analytics/Codacy_challenge/mrr_df_firstpay.csv')                
mrr_df_largestpayment.to_csv('/Users/Carnec/Desktop/Business_Analytics/Codacy_challenge/mrr_df_largestpayment.csv')                
    

''' Calculate Mrr ''' 
def createTable(df_i, df_m):       
    kpi_df = pd.DataFrame(columns=('Month','totalRevenue','MRR','NewMRRabs',
    'NewMRRpc','expMRRabs','expMRRpc', 'churn','cancelled','MRRdowngradesabs','MRRdowngradespc'))    
    kpi_df.Month = monthbinarray
    for i in monthbinarray:
        kpi_df.at[i,'totalRevenue'] = df_i.amountPaid[(df_i['month']==i)].sum()
        kpi_df.at[i,'MRR'] = df_m['m%i'%i].sum()  
        kpi_df.at[i,'NewMRRabs'] = df_m['m%i'%i][df_m['start']==i].sum()
        kpi_df['NewMRRpc'] = kpi_df['NewMRRabs']/kpi_df['MRR']
        if i > 0:
            mrrexpsum = (df_m['m%i'%(i)][df_m['m%i'%(i)]>df_m['m%i'%(i-1)]] - df_m['m%i'%(i-1)][df_m['m%i'%(i)]>df_m['m%i'%(i-1)]]).sum()
            kpi_df.at[i,'expMRRabs'] = mrrexpsum
            mrrdownsum = (df_m['m%i'%(i)][df_m['m%i'%(i)]<df_m['m%i'%(i-1)]] - df_m['m%i'%(i-1)][df_m['m%i'%(i)]<df_m['m%i'%(i-1)]]).sum()
            kpi_df.at[i,'MRRdowngradesabs'] = mrrdownsum
            previousm = (df_m['m%i'%(i-1)].isnull()==False).sum()
            nextm = df_m['m%i'%(i)][df_m[(df_m['m%i'%(i-1)].isnull() == False)].index].isnull().sum()
            kpi_df.at[i,'churn'] = nextm/previousm
            getindicenull = ((df_m['m%i'%(i)][df_m['m%i'%(i-1)].notnull()]).isnull())
            getindicenull = getindicenull[getindicenull==True]
            cancel = df_m['m%i'%(i-1)][getindicenull.index].sum()
            kpi_df.at[i,'cancelled'] = -cancel
    kpi_df['expMRRpc'] = kpi_df['expMRRabs']/kpi_df['MRR']
    kpi_df['MRRdowngradespc'] = kpi_df['MRRdowngradesabs']/kpi_df['MRR']
    costpm=4000
    listcost = [costpm for i in range(len(monthbinarray))]
    kpi_df['cost'] = listcost 
    kpi_df['growtheffort_profitable'] = kpi_df['NewMRRabs'] - kpi_df['cost']
    kpi_df['positive'] = kpi_df['growtheffort_profitable'] >=0
    return kpi_df

xf = createTable(df_invoice, mrr_df_firstpay)
xe = createTable(df_invoice, mrr_df_everyone)    
xc = createTable(df_invoice, mrr_df_conservative)    
xl = createTable(df_invoice, mrr_df_largestpayment)


xf.to_csv('/Users/Carnec/Desktop/Business_Analytics/Codacy_challenge/xf.csv')     
xe.to_csv('/Users/Carnec/Desktop/Business_Analytics/Codacy_challenge/xe.csv')                
xc.to_csv('/Users/Carnec/Desktop/Business_Analytics/Codacy_challenge/xc.csv')                
xl.to_csv('/Users/Carnec/Desktop/Business_Analytics/Codacy_challenge/xl.csv')                
    



''' Task 1 - Analyze the growth of the company as well as the retention trends of the company. ''' 
def netnewmrrBreakdown(df):
    df['cancelled'] = -df['cancelled']
    plot = df[['MRRdowngradesabs','cancelled','expMRRabs','NewMRRabs',]].plot(kind='bar', stacked=True)
    plot.legend(loc=2, fontsize = 'small');
    return plot
netnewmrrBreakdown(xf)    
netnewmrrBreakdown(xe)    
netnewmrrBreakdown(xc)  
netnewmrrBreakdown(xl)  


def mrrPlot(df):
    plot = df['MRR'].plot(kind='bar')
    plot.legend(loc=2, fontsize = 'small');
    return plot
mrrPlot(xf)    
mrrPlot(xe)    
mrrPlot(xc)   
mrrPlot(xl)  


def churnPlot(df):
    plot = df['churn'].plot(kind='bar')
    plot.legend(loc=2, fontsize = 'small');
    return plot
churnPlot(xf)    
churnPlot(xe)    
churnPlot(xc) 
churnPlot(xl)  
 

''' Task 2 - Analyze if the company’s growth efforts are being profitable. '''
costpm = 4000
def profitablePlot(df,costpm):
    listcost = [costpm for i in range(df.Month.count())]
    df['cost'] = listcost 
    plot = df[['cost','NewMRRabs']].plot()
    return plot
profitablePlot(xf,costpm)    
profitablePlot(xe,costpm)    
profitablePlot(xc,costpm) 
profitablePlot(xl,costpm) 


def profitablequestion(df,costpm):
    listcost = [costpm for i in range(xf.Month.count())]
    df['cost'] = listcost 
    df['profitable'] = df['NewMRRabs'] - df['cost']
    df['positive'] = df['profitable'] >=0
    plot = df['profitable'].plot(kind='bar',color=df.positive.map({True: 'b', False: 'r'}))
    return plot
profitablequestion(xf,costpm)    
profitablequestion(xe,costpm)    
profitablequestion(xc,costpm)    
profitablequestion(xl,costpm)    


''' Task 3 - Provide a profile of what we could call “the core users” meaning, how do the company’s best customer look? '''

''' Looking at current core users - more than 2 years (not including current month (14) because some may not have paid yet.)'''
def returncoreC(df,montharray,lastnmonths,df_customer_information):
    lastmonth = montharray.max()-1 #month 14 -1 because month 14 is not over.
    for i in range(lastnmonths):
        df = df[df['m%i'%(lastmonth-i)].notnull()==True]
    df['monthspaying'] = (df.iloc[:,3:(3+(len(montharray)-1))].count(axis=1))+1
    df['totalpayed'] = df.iloc[:,3:(3+(len(montharray)-1))].sum(axis=1)
    df['averagepayedpermonth'] = df['totalpayed'] / df['monthspaying']
    corecustomers = df_customer_information.loc[df_customer_information['customerId'].isin(df.customerId)]
    corecustomers = pd.merge(corecustomers, df[['customerId','monthspaying','totalpayed','averagepayedpermonth']], on='customerId')
    corecustomers['sessions/months.ratio'] = corecustomers['nrSessions']/corecustomers['monthspaying']
    corecustomers['seats/averagepaypermonth.ratio'] = corecustomers['nrSeats']/corecustomers['averagepayedpermonth']
    return corecustomers

''' Correlation between what customers pay and number of seats ''' 
  
correlation_seats_totalpayed= cc['nrSeats'].corr(cc['totalpayed'])
correlation_seats_averagepayed = cc['nrSeats'].corr(cc['averagepayedpermonth'])
correlation_seats_months = cc['nrSeats'].corr(cc['monthspaying'])

correlation_sessions_totalpayed= cc['nrSessions'].corr(cc['totalpayed'])
correlation_sessions_averagepayed = cc['nrSessions'].corr(cc['averagepayedpermonth'])
correlation_sessions_months = cc['nrSessions'].corr(cc['monthspaying'])

''' Compare largest payment that month with everything coming through '''

cc_l_3 = returncoreC(mrr_df_largestpayment,monthbinarray,3,df_information) 
cc_l_6 = returncoreC(mrr_df_largestpayment,monthbinarray,6,df_information)   
cc_l_10 = returncoreC(mrr_df_largestpayment,monthbinarray,10,df_information) 

cc_l_3describe = cc_l_3.loc[:, cc_l_3.columns != 'customerId'][np.isfinite(cc_l_3['nrSeats'])].describe()
cc_l_3describe.index.name = 'newhead'
cc_l_3describe.reset_index(inplace=True)

cc_l_6describe = cc_l_6.loc[:, cc_l_6.columns != 'customerId'][np.isfinite(cc_l_6['nrSeats'])].describe()
cc_l_6describe.index.name = 'newhead'
cc_l_6describe.reset_index(inplace=True)

cc_top20paying = cc_l_3.nlargest(20, 'totalpayed')
describecc_top20paying = cc_top20paying.loc[:, cc_top20paying.columns != 'customerId'].describe()
describecc_top20paying.index.name = 'newhead'
describecc_top20paying.reset_index(inplace=True)
''' To google Sheets '''

#json_key = json.load(open('example-c03643311607.json')) # json credentials you downloaded earlier
#scope = ['https://spreadsheets.google.com/feeds']
#
#credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'].encode(), scope) # get email and key from creds
#
#file = gspread.authorize(credentials) # authenticate with Google
#
#
#sheet1 = file.open("Codacy Challenge").worksheet('ConservativeMRR') # open sheet
#set_with_dataframe(sheet1,xc)
#
#sheet2 = file.open("Codacy Challenge").worksheet('EveryoneMRR')
#set_with_dataframe(sheet2,xe)
#
#sheet3 = file.open("Codacy Challenge").worksheet('FirstpayMRR')
#set_with_dataframe(sheet3,xf)
#
#sheet4 = file.open("Codacy Challenge").worksheet('LargestMRR')
#set_with_dataframe(sheet4,xl)
#
#sheet5 = file.open("Codacy Challenge").worksheet('returningCustomerslast3months')
#set_with_dataframe(sheet5,cc_l_3describe)
#
#sheet6 = file.open("Codacy Challenge").worksheet('returningCustomerslast6months')
#set_with_dataframe(sheet6,cc_l_6describe)
#
#sheet8 = file.open("Codacy Challenge").worksheet('top20paying_returningCustomerslast3months')
#set_with_dataframe(sheet8,describecc_top20paying)
#


''' Execute Code every.... '''

json_key = json.load(open('example-c03643311607.json')) # json credentials you downloaded earlier
scope = ['https://spreadsheets.google.com/feeds']
credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'].encode(), scope) # get email and key from creds
file = gspread.authorize(credentials) # authenticate with Google

def job(xc,xe,xf,xl,json_key,scope,credentials,file):
    
    json_key = json.load(open('example-c03643311607.json')) # json credentials you downloaded earlier
    scope = ['https://spreadsheets.google.com/feeds']
    
    credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'].encode(), scope) # get email and key from creds
    
    file = gspread.authorize(credentials) # authenticate with Google
    
    
    sheet1 = file.open("Codacy Challenge").worksheet('ConservativeMRR') # open sheet
    set_with_dataframe(sheet1,xc)
    
    sheet2 = file.open("Codacy Challenge").worksheet('EveryoneMRR')
    set_with_dataframe(sheet2,xe)
    
    sheet3 = file.open("Codacy Challenge").worksheet('FirstpayMRR')
    set_with_dataframe(sheet3,xf)
    
    sheet4 = file.open("Codacy Challenge").worksheet('LargestMRR')
    set_with_dataframe(sheet4,xl)
    
    sheet5 = file.open("Codacy Challenge").worksheet('returningCustomerslast3months')
    set_with_dataframe(sheet5,cc_l_3describe)
    
    sheet6 = file.open("Codacy Challenge").worksheet('returningCustomerslast6months')
    set_with_dataframe(sheet6,cc_l_6describe)
    
    sheet8 = file.open("Codacy Challenge").worksheet('top20paying_returningCustomerslast3months')
    set_with_dataframe(sheet8,describecc_top20paying)


    schedule.every().day.at("12:00").do(job)

    while True:
        schedule.run_pending()
        time.sleep(60) # wait one minute



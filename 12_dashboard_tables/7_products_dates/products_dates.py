import pandas as pd
from datetime import datetime
import re
import datetime as DT
import yaml

with open("config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

df = pd.read_csv(cfg['root'] + cfg['dir_data_shopify'] + cfg["ip_orders"], low_memory=False)

df = df[['Lineitem name', 'Lineitem sku', 'Created at', 'Lineitem quantity', 'Lineitem price']]
df['Revenue'] = df['Lineitem quantity'] * df['Lineitem price']

df = df[['Lineitem name', 'Lineitem sku', 'Created at', 'Revenue']]
df.columns = ['Product', 'SKU', 'Date', 'Revenue']
df['Product'] = df['Product'].apply(lambda x: x.upper())
df['SKU'] = df['SKU'].apply(lambda x: str(x).upper())
prods = df[['Product', 'SKU', 'Revenue']]
df['Date1'] = df['Date']

def changeDate(data):
    matchobj = re.match(r'(.*) (.*) (.*).*',data)
    if (pd.to_datetime(datetime.strptime(matchobj.group(1), '%Y-%m-%d')).date().strftime("%A") == 'Wednesday'):
        return datetime.strptime(matchobj.group(1), '%Y-%m-%d').date() - DT.timedelta(days=6)
    if (pd.to_datetime(datetime.strptime(matchobj.group(1), '%Y-%m-%d')).date().strftime("%A") == 'Tuesday'):
        return datetime.strptime(matchobj.group(1), '%Y-%m-%d').date() - DT.timedelta(days=5)
    if (pd.to_datetime(datetime.strptime(matchobj.group(1), '%Y-%m-%d')).date().strftime("%A") == 'Monday'):
        return datetime.strptime(matchobj.group(1), '%Y-%m-%d').date() - DT.timedelta(days=4)
    if (pd.to_datetime(datetime.strptime(matchobj.group(1), '%Y-%m-%d')).date().strftime("%A") == 'Sunday'):
        return datetime.strptime(matchobj.group(1), '%Y-%m-%d').date() - DT.timedelta(days=3)
    if (pd.to_datetime(datetime.strptime(matchobj.group(1), '%Y-%m-%d')).date().strftime("%A") == 'Saturday'):
        return datetime.strptime(matchobj.group(1), '%Y-%m-%d').date() - DT.timedelta(days=2)
    if (pd.to_datetime(datetime.strptime(matchobj.group(1), '%Y-%m-%d')).date().strftime("%A") == 'Friday'):
        return datetime.strptime(matchobj.group(1), '%Y-%m-%d').date() - DT.timedelta(days=1)
    if (pd.to_datetime(datetime.strptime(matchobj.group(1), '%Y-%m-%d')).date().strftime("%A") == 'Thursday'):
        return datetime.strptime(matchobj.group(1), '%Y-%m-%d').date() - DT.timedelta(days=0)
df['Date'] = df.Date.apply(changeDate)

df1 = df[['Product', 'SKU', 'Revenue', 'Date1']]

def cutDate(data):
    matchobj = re.match(r'(.*) (.*) (.*).*',data)
    data = matchobj.group(1)
    matchobj = re.match(r'(.*)-(.*)-(.*).*',data)
    return matchobj.group(1) + "-" + matchobj.group(2) + "-01"
df1['Date1'] = df1.Date1.apply(cutDate)

df = df[['Product', 'SKU', 'Revenue', 'Date']]
df = df.groupby(['Date','Product', 'SKU'], as_index=False).sum()
df1 = df1.groupby(['Date1','Product', 'SKU'], as_index=False).sum()
prods = prods.groupby(['Product', 'SKU'], as_index=False).sum()
prods = prods.drop_duplicates().reset_index().drop('index',1)

final = pd.DataFrame(columns = ['Product', 'SKU'] + df1['Date1'].drop_duplicates().tolist()+ ['Revenue'] )
final[['Product']] = prods[['Product']]
final[['Revenue']] = prods[['Revenue']]
final[['SKU']] = prods[['SKU']]
final.fillna(0, inplace=True)

final['New'] = final.apply(lambda x: str(x['Product']) +","+ str(x['SKU']), axis=1)
df1['New'] = df1.apply(lambda x: str(x['Product']) +","+ str(x['SKU']), axis=1)
final = final.set_index('New')
final.index.name = None

for i in range(0, max(df1.index)+1):
    final.loc[df1.loc[i, 'New'],df1.loc[i, 'Date1']] = df1.loc[i, 'Revenue']
    
final = final.reset_index().drop('index',1)

for i in range(0,max(final.index)+1):
    for j in range(2,len(final.columns)-1):
        #To get the first non zero revenue month
        if(final.iloc[i,j]!=0):
            t=1
            break
    #TO get the total no of months        
    n=len(final.columns)-1-j
    m=n**(-1)
    y=final.loc[i, 'Revenue']/final.iloc[i,j]
    final.loc[i,'CMGR']=(pow(y,m)-1)*100
    final.loc[i,'Period'] = n
    
final = final[['Product', 'SKU', 'CMGR', 'Period']]

final['New'] = final.apply(lambda x: str(x['Product']) +","+ str(x['SKU']), axis=1)
df['New'] = df.apply(lambda x: str(x['Product']) +","+ str(x['SKU']), axis=1)
final = final.set_index('New')
final.index.name = None

for i in range(0, max(df.index)+1):
    df.loc[i, 'CMGR'] = final.loc[df.loc[i, 'New'], 'CMGR']
    df.loc[i, 'Period'] = final.loc[df.loc[i, 'New'], 'Period']
    
total = df['Revenue'].sum(1) #total revenue
df['%Total Revenue'] = df['Revenue'].apply(lambda x: x*100/total) #%total revenue
totprods = max(df.index) + 1
temp = total/totprods #total revenue / total products
df['Average Revenue'] = temp #avg revenue

df = df[['Product', 'SKU', 'Date', 'Revenue', 'CMGR', 'Period', '%Total Revenue', 'Average Revenue']]

df.to_csv('C:\Users\saisree849\Documents\GitHub\NGS\\12_dashboard tables\output\products_dates.csv', index=False)

df.to_csv(cfg['root']+cfg['dir_data_output']+cfg['op_products_dates'], index=False)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    

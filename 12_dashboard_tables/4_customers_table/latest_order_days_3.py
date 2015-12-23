import pandas as pd
from datetime import datetime
import time
import os
import yaml

with open("config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

df = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["ip_orders"],low_memory=False)

#get date in datetime format
def getDate(data):
    return pd.to_datetime(datetime.strptime(data[:10], '%Y-%m-%d')).date()
    
#get days from today to order date
def diffDates(data):
    return (today - data).days
    
#get required columns
df1=df[['Email', 'Created at' ]]

df1=df1.drop_duplicates()
df1= df1.reset_index().drop('index',1)

#rename columns
df1.columns = ['Email', 'Date']

df1['Date'] = df1.Date.apply(getDate)

df1=df1.drop_duplicates()
df1= df1.reset_index().drop('index',1)

#get today's date in datetime format
today = pd.to_datetime(datetime.strptime(time.strftime("%Y-%m-%d"), '%Y-%m-%d')).date()

df1['Days'] = df1.Date.apply(diffDates)

df1 = df1.sort(['Email', 'Days'])
df1= df1.reset_index().drop('index',1)

#get the email and the min days row
df2 = df1.groupby(['Email'], axis=0, as_index=False).min()
    
df2 = df2[['Email' , 'Days']]
df2.columns =['Email', 'Days from Last order']
df2.to_csv(cfg['root']+cfg['dir_customers_table']+cfg['io_latest_order_days_3'],index = False)
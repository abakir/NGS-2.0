#!/home/cloudera/local/lib/python2.6/site-packages/bin/python

import pandas as pd
from datetime import datetime
import yaml
import logging
import time

with open("/home/cloudera/Documents/12_dashboard_tables/config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

# create logger
logger = logging.getLogger(cfg['log_latest_order_days_3'])
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.FileHandler(cfg['root'] + cfg['dir_logs'] + cfg['log_latest_order_days_3'] + "_" + time.strftime("%Y_%m_%d_%H_%M_%S") + ".log" )
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

df = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["ip_orders"],low_memory=False)
logger.debug("Data Frame df created")

#get date in datetime format
def getDate(data):
    return pd.to_datetime(datetime.strptime(data[:10], '%Y-%m-%d')).date()
    
#get days from today to order date
def diffDates(data):
    return (today - data).days
    
#get required columns
df1=df[['Email', 'Created at' ]]

df1=df1.drop_duplicates().reset_index().drop('index',1)

#rename columns
df1.columns = ['Email', 'Date']

df1.loc[:, 'Date'] = df1.Date.apply(getDate)

df1=df1.drop_duplicates().reset_index().drop('index',1)

#get today's date in datetime format
today = pd.to_datetime(datetime.strptime(time.strftime("%Y-%m-%d"), '%Y-%m-%d')).date()

df1.loc[:, 'Days'] = df1.Date.apply(diffDates)

df1 = df1.sort_values(by = ['Email', 'Days'])
df1= df1.reset_index().drop('index',1)

#get the email and the min days row
df2 = df1.groupby(['Email'], axis=0, as_index=False).min()
    
df2 = df2[['Email' , 'Days']]
df2.columns =['Email', 'Days from Last order']
df2.to_csv(cfg['root']+cfg['dir_customers_table']+cfg['io_latest_order_days_3'],index = False)

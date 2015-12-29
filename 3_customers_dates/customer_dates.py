#!/home/cloudera/local/lib/python2.6/site-packages/bin/python

import pandas as pd
from datetime import datetime
import datetime as DT
import re
import yaml
import logging
import time
import os

def make_sure_path_exists(path):
    if (!os.path.isdir(path)):
        os.makedirs(path)

with open("/home/cloudera/Documents/12_dashboard_tables/config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

# create logger
logger = logging.getLogger(cfg['log_customer_dates'])
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
make_sure_path_exists(cfg['root'] + cfg['dir_logs'])
ch = logging.FileHandler(cfg['root'] + cfg['dir_logs'] + cfg['log_customer_dates'] + "_" + time.strftime("%Y_%m_%d_%H_%M_%S") + ".log" )
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

df = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["ip_orders"], low_memory=False)
logger.debug("Data Frame df created")

df = df[['Name', 'Email', 'Created at', 'Lineitem quantity', 'Lineitem price']]
#calculate revenue
df.loc[:, 'Revenue'] = df.apply(lambda x: x['Lineitem quantity']*x['Lineitem price'], axis=1)

#get required columns and rename
df = df[['Name', 'Email', 'Created at', 'Revenue']]
df.columns = ['Name', 'Email', 'Date', 'Revenue']

df['Date1'] = df['Date']

#get date as day
def getDay(data):
    matchobj = re.match(r'(.*) (.*) (.*).*',data)
    data = matchobj.group(1)
    return pd.to_datetime(datetime.strptime(data, '%Y-%m-%d')).date().strftime("%A")

#get hours of the order
def getHours(data):
    matchobj = re.match(r'(.*) (.*) (.*).*',data)
    data = matchobj.group(2)
    num = pd.to_datetime(datetime.strptime(data, '%H:%M:%S')).hour
    if(num < 2):
        return 0
    elif((num >= 2) & (num < 4)):
        return 2
    elif((num >= 4) & (num < 6)):
        return 4
    elif((num >= 6) & (num < 8)):
        return 6
    elif((num >= 8) & (num < 10)):
        return 8
    elif((num >= 10) & (num < 12)):
        return 10
    elif((num >= 12) & (num < 14)):
        return 12
    elif((num >= 14) & (num < 16)):
        return 14
    elif((num >= 16) & (num < 18)):
        return 16
    elif((num >= 18) & (num < 20)):
        return 18
    elif((num >= 20) & (num < 22)):
        return 20
    elif(num >= 22):
        return 22

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
        
                        
df.loc[:, 'Day'] = df.Date1.apply(getDay)
df.loc[:, 'Hours'] = df.Date1.apply(getHours)

df = df[['Name', 'Email', 'Date', 'Revenue', 'Day', 'Hours']]

df.loc[:, 'Date'] = df.Date.apply(changeDate)

df = df.drop_duplicates().reset_index().drop('index',1)

data = df[['Email', 'Date', 'Revenue']]
df = df[['Email', 'Date', 'Day', 'Hours']]

df = df.drop_duplicates().reset_index().drop('index',1)

df1 = pd.DataFrame(columns = ['Email', 'Date', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22])
df1['Email'] = df['Email']
df1['Date'] = df['Date']
df1['Sunday'] = df1['Monday'] = df1['Tuesday'] = df1['Wednesday'] = df1['Thursday'] = df1['Friday'] = df1['Saturday'] = df1[0] = df1[2] = df1[4] = df1[6] = df1[8] = df1[10] = df1[12] = df1[14] = df1[16] = df1[18] = df1[20] = df1[22] =0

#assign values to hours and days columns
for i in range(0, max(df.index)+1):
    df1.loc[i,(df.iloc[i,2])] = 1
    df1.loc[i,(df.iloc[i,3])] = 1
    
#count total orders per hours, days
df1 = df1.groupby(['Email', 'Date'], axis = 0, as_index=False).sum()

#rename columns
df1.columns = ['Email', 'Date', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', '0:00 - 2:00', '2:00 - 4:00', '4:00 - 6:00', '6:00 - 8:00', '8:00 - 10:00', '10:00 - 12:00', '12:00 - 14:00', '14:00 - 16:00', '16:00 - 18:00', '18:00 - 20:00', '20:00 - 22:00', '22:00 - 0:00']

df5 = data[['Email', 'Date', 'Revenue']]
df2 = data[['Email', 'Date']]
df2 = df2.drop_duplicates().reset_index().drop('index',1)
df2.loc[:, 'Total orders'] = 1
df5 = df5.groupby(['Email', 'Date'], as_index=False).sum()
df2 = df2.groupby(['Email', 'Date'], as_index=False).sum()

#join data frames
df3 = df2.merge(df5, on = ['Email', 'Date'], how = 'inner')

#calculate basket value and join data frames
df3.loc[:, 'Basket Value'] = df3.apply(lambda x: x['Revenue']/float(x['Total orders']), axis=1)
df3 = df3.merge(df1, on = ['Email', 'Date'], how = 'inner')

df = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["ip_orders"], low_memory=False)

#get required columns
df1 = df[['Lineitem quantity', 'Lineitem price']]

#calculate revenue
df1.loc[:, 'Revenue'] = df1.apply(lambda x: x['Lineitem quantity'] * x['Lineitem price'], axis=1)

#get required columns
df1=df1[['Lineitem quantity', 'Revenue']]
df = df[['Email']]

df = df.drop_duplicates().reset_index().drop('index',1)

#calculate avg revenue, avg basket size
df1 = df1.sum()
df3.loc[:, 'Average Revenue'] = df1['Revenue']/float(max(df.index)+1)
df3.loc[:, 'Average Basket Size'] = df1['Lineitem quantity']/float(max(df.index)+1)

customers = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["ip_customers"], low_memory=False)

#concatenate name, address
customers.loc[:, 'Name'] = customers['First Name'] + " " + customers['Last Name']
customers.loc[:, 'Address'] = customers['Address1'] + " " + customers['Address2'] + " " + customers['City']

customers = customers[['Name', 'Address', 'Phone', 'Email']]

df3 = customers.merge(df3, on = ['Email'], how = 'inner')

make_sure_path_exists(cfg['root']+cfg['dir_data_output'])

df3.to_csv(cfg['root']+cfg['dir_data_output']+cfg['op_customer_dates'], index=False)

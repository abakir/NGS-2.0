#!/home/cloudera/local/lib/python2.6/site-packages/bin/python

import pandas as pd
from datetime import datetime, timedelta
import re
import yaml
import logging
import time
import os

with open("/home/cloudera/Documents/12_dashboard_tables/config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)
        
def make_sure_path_exists(path):
    if (os.path.isdir(path) == False):
        os.makedirs(path)

make_sure_path_exists(cfg['root'] + cfg['dir_logs'])
make_sure_path_exists(cfg['root']+cfg['dir_data_output'])

# create logger
logger = logging.getLogger(cfg['log_cust_retention'])
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
make_sure_path_exists(cfg['root'] + cfg['dir_logs'])
ch = logging.FileHandler(cfg['root'] + cfg['dir_logs'] + cfg['log_cust_retention'] + "_" + time.strftime("%Y_%m_%d_%H_%M_%S") + ".log" )
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

df = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["ip_orders"], low_memory=False)
logger.debug("Data Frame df created")

#get required columns and rename
df = df[['Email', 'Created at']] 
df.columns = ['Email', 'Date'] 

#get only date from date in df
def convertDate(data):
    matchobj = re.match(r'(.*) (.*) (.*).*',data)
    return matchobj.group(1)

#get only date from new date created
def convertNewDate(data):
    matchobj = re.match(r'(.*) (.*).*',data)
    return matchobj.group(1)

#calculate days between two dates
def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return (d2 - d1).days
    
df.loc[:, 'Date'] = df.Date.apply(convertDate) 
df = df.drop_duplicates().reset_index().drop('index',1) #remove products with same names in single order

df.loc[:, 'YearBefore'] = (str)(datetime.now() - timedelta(days=365)) #365 days from today(Wednesday)
df.loc[:, 'YearBefore'] = df.YearBefore.apply(convertNewDate) #call function

df.loc[:, 'DaysBetween'] = df.apply(lambda x: days_between(x['YearBefore'], x['Date']), axis=1)
df = df[(df.DaysBetween <= 365) & (df.DaysBetween >= 0)] #get rows whose order is with in 365 days

df1 = df[['Email']] #create new df with emails
df1 = df1.drop_duplicates().reset_index().drop('index',1)

df2 = df[['Email', 'Date']] #create new df with emails, dates

df.loc[:, 'Days30Before'] = (str)(datetime.now() - timedelta(days=30)) #30 days from today(Wednesday)
df.loc[:, 'Days30Before'] = df.Days30Before.apply(convertNewDate) 

df2.loc[:, 'Days20Before'] = (str)(datetime.now() - timedelta(days=20)) #20 days from today(Wednesday)
df2.loc[:, 'Days20Before'] = df2.Days20Before.apply(convertNewDate) 

df.loc[:, 'DaysBetween'] = df.apply(lambda x: days_between(x['Days30Before'], x['Date']), axis=1)
df = df[(df.DaysBetween <= 30) & (df.DaysBetween >= 0)] #get rows whose order is with in 30 days

df2.loc[:, 'DaysBetween'] = df2.apply(lambda x: days_between(x['Days20Before'], x['Date']), axis=1)
df2 = df2[(df2.DaysBetween <= 20) & (df2.DaysBetween >= 0)] # get rows whose order is with in 20 days

#count number of times a customer has ordered
df.loc[:, 'Count'] = 1
df = df[['Email', 'Count']]
df = df.groupby(['Email'], axis = 0, as_index=False).sum()

#count number of times a customer has ordered
df2.loc[:, 'Count'] = 1
df2 = df2[['Email', 'Count']]
df2 = df2.groupby(['Email'], axis = 0, as_index=False).sum()

#retain all cutomers who ordered more than once
df = df[(df.Count > 1)]
df = df.drop_duplicates().reset_index().drop('index',1)

#new df with all required columns, assign values to columns
df3 = pd.DataFrame(columns = ['Cutomers 30 days', 'Cutomers 20 days', 'Total Count', 'Retention Rate', 'Regular Customers'])
df3.loc[0, 'Cutomers 30 days'] = max(df.index) + 1
df3.loc[0, 'Cutomers 20 days'] = max(df2.index) + 1
df3.loc[0, 'Total Count'] = max(df1.index) + 1
df3.loc[0, 'Retention Rate'] = df3.loc[0, 'Cutomers 30 days']/float(df3.loc[0, 'Total Count'])
df3.loc[0, 'Regular Customers'] = df3.loc[0, 'Cutomers 20 days']/float(df3.loc[0, 'Total Count'])

make_sure_path_exists(cfg['root']+cfg['dir_data_output'])
df3.to_csv(cfg['root']+cfg['dir_data_output']+cfg['op_cust_retention'], index = False)

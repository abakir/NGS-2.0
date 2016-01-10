#!/home/cloudera/local/lib/python2.6/site-packages/bin/python

import pandas as pd
import re
from datetime import datetime
import datetime as DT
import yaml
import logging
import time
import os

with open("config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)
        
def make_sure_path_exists(path):
    if (os.path.isdir(path) == False):
        os.makedirs(path)

make_sure_path_exists(cfg['root'] + cfg['dir_logs'])
make_sure_path_exists(cfg['root']+cfg['dir_data_output'])

# create logger
logger = logging.getLogger(cfg['log_yr_unfulfilled_orders'])
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.FileHandler(cfg['root'] + cfg['dir_logs'] + cfg['log_yr_unfulfilled_orders'] + "_" + time.strftime("%Y_%m_%d_%H_%M_%S") + ".log" )
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

df = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["input_orders"],low_memory=False)
logger.debug("Data Frame df created")

#df.columns
df = df[['Name', 'Fulfillment Status', 'Created at']]
df.columns = ['Name', 'Fulfillment Status', 'Created']

#get date
def getDate(data):
    matchobj = re.match(r'(.*) (.*) (.*).*',data)
    return matchobj.group(1)

#get month
def getMonth(data):
    matchobj = re.match(r'(.*)-(.*).*',data)
    return matchobj.group(1)
    
df.loc[:, 'Created'] = df.Created.apply(getDate)
    
df1 = df.drop_duplicates('Name', keep='first').reset_index().drop('index', 1)

dt = DT.date.today() #today's date
#dt = dt - DT.timedelta(days=1) #wednesday date
dt1 = datetime.strptime('2015-01-01', '%Y-%m-%d').date() #1st day of year

#update rows by comparing dates
df1.loc[:, 'Yes'] = df1.apply(lambda x: (datetime.strptime(x['Created'], '%Y-%m-%d').date() <= dt) & (datetime.strptime(x['Created'], '%Y-%m-%d').date() >= dt1), axis = 1)

df2 = df1.loc[df1['Yes'] == True] #get orders within last week

df2 = df2[['Name', 'Fulfillment Status', 'Created']]
df2 = df2.reset_index().drop('index', 1)
df2.loc[:, 'Created'] = df2.Created.apply(getMonth)    
df2.loc[:, 'All'] = 1
df2.loc[:, 'Unfulfilled'] = df2.apply(lambda x: 1 if x['Fulfillment Status'] == 'unfulfilled' else 0 , axis = 1) #update column based on fulfillement status

df2 = df2.groupby(['Created'], axis=0, as_index=False).sum() #count total unfulfilled orders
df2.loc[:, '% Unfulfilled'] = df2.apply(lambda x: x['Unfulfilled']*100/float(x['All']), axis = 1)

df2.to_csv(cfg['root']+cfg['dir_data_output']+cfg['output_yr_unfulfilled_orders'], index = False)

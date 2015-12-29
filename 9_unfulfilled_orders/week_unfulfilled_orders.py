#!/home/cloudera/local/lib/python2.6/site-packages/bin/python

import pandas as pd
import re
from datetime import datetime
import datetime as DT
import yaml
import logging
import time
import os

def make_sure_path_exists(path):
    if (!os.path.isdir(path)):
        os.makedirs(path)

make_sure_path_exists(cfg['root'] + cfg['dir_logs'])
make_sure_path_exists(cfg['root']+cfg['dir_data_output'])

with open("/home/cloudera/Documents/12_dashboard_tables/config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

# create logger
logger = logging.getLogger(cfg['log_week_unfulfilled_orders'])
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.FileHandler(cfg['root'] + cfg['dir_logs'] + cfg['log_week_unfulfilled_orders'] + "_" + time.strftime("%Y_%m_%d_%H_%M_%S") + ".log" )
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

df = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["ip_orders"],low_memory=False)
logger.debug("Data Frame df created")

df = df[['Name', 'Fulfillment Status', 'Created at']]

#get date
def getDate(data):
    matchobj = re.match(r'(.*) (.*) (.*).*',data)
    return matchobj.group(1)
df.loc[:, 'Created at'] = df.apply(lambda x: getDate(x['Created at']), axis=1)
    
#take last item of the total order
df1 = df.drop_duplicates('Name', keep = 'first').reset_index().drop('index', 1)

#get todays date
today = DT.date.today()
dt = today - DT.timedelta(days=8) #Wednesday date
df1.loc[:, 'Yes'] = df1.apply(lambda x: datetime.strptime(x['Created at'], '%Y-%m-%d').date() > dt, axis = 1) #update rows by comparing dates

df2 = df1.loc[df1['Yes'] == True] #get orders within last week

df2 = df2[['Name', 'Fulfillment Status', 'Created at']]

df2.loc[:, 'All'] = 1
df2.loc[:, 'Unfulfilled'] = df2.apply(lambda x: 1 if x['Fulfillment Status'] == 'unfulfilled' else 0 , axis = 1) #update column based on fulfillement status

df2 = df2.groupby(['Created at'], axis=0, as_index=False).sum() #count total unfulfilled orders
df2.loc[:, '% Unfulfilled'] = df2.apply(lambda x: x['Unfulfilled']*100/float(x['All']), axis = 1)

df2.to_csv(cfg['root']+cfg['dir_data_output']+cfg['op_week_unfulfilled_orders'], index = False)

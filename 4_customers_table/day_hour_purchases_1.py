#!/home/cloudera/local/lib/python2.6/site-packages/bin/python

# Author : Sai Sree Kamineni
# Date created : Jan 10, 2016
# Execution frequency : Weekly
# Input refresh frequency : Weekly

# Input : data_input/shopify/export_orders.csv
# Output : 4_customers_table/day_hour_purchases_1.csv
# Purpose : Gives the count of orders made by each customer on each day of the week and every time interval.

import pandas as pd
from datetime import datetime
import re
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
logger = logging.getLogger(cfg['log_day_hour_purchases_1'])
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.FileHandler(cfg['root'] + cfg['dir_logs'] + cfg['log_day_hour_purchases_1'] + "_" + time.strftime("%Y_%m_%d_%H_%M_%S") + ".log" )
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

df = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["input_orders"],low_memory=False)
logger.debug("Data Frame df created")

#get required columns and rename
df = df[['Name','Email', 'Created at']]
df.columns = ['Name','Email', 'Date']

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
        
df.loc[:, 'Day'] = df.Date.apply(getDay)
df.loc[:, 'Hours'] = df.Date.apply(getHours)

df = df[['Name', 'Email', 'Day', 'Hours']]

df = df.drop_duplicates().reset_index().drop('index',1)

#create new df and initialize columns
df1 = pd.DataFrame(columns = ['Email', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22])
df1['Email'] = df['Email']
df1['Sunday'] = df1['Monday'] = df1['Tuesday'] = df1['Wednesday'] = df1['Thursday'] = df1['Friday'] = df1['Saturday'] = df1[0] = df1[2] = df1[4] = df1[6] = df1[8] = df1[10] = df1[12] = df1[14] = df1[16] = df1[18] = df1[20] = df1[22] =0

#assign values to hours and days columns
for i in range(0, max(df.index)+1):
    df1.loc[i,(df.iloc[i,2])] = 1
    df1.loc[i,(df.iloc[i,3])] = 1
    
#count total orders per hours, days
df1 = df1.groupby('Email', axis = 0, as_index=False).sum()

df1.columns = ['Email', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', '0:00 - 2:00', '2:00 - 4:00', '4:00 - 6:00', '6:00 - 8:00', '8:00 - 10:00', '10:00 - 12:00', '12:00 - 14:00', '14:00 - 16:00', '16:00 - 18:00', '18:00 - 20:00', '20:00 - 22:00', '22:00 - 0:00']

df1.to_csv(cfg['root']+cfg['dir_customers_table']+cfg['io_day_hour_purchases_1'], index = False)

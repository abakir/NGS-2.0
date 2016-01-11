#!/home/cloudera/local/lib/python2.6/site-packages/bin/python

# Author : Sai Sree Kamineni
# Date created : Jan 10, 2016
# Execution frequency : Weekly
# Inputs refresh frequency : Weekly

# Input : data_input/shopify/export_orders.csv
# Output : data_output/order_frequency.csv
# Purpose : Gives the count of customers whose average days between orders fall in the given ranges

import pandas as pd
import numpy as np
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
logger = logging.getLogger(cfg['log_order_frequency'])
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.FileHandler(cfg['root'] + cfg['dir_logs'] + cfg['log_order_frequency'] + "_" + time.strftime("%Y_%m_%d_%H_%M_%S") + ".log" )
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

df = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["input_orders"],low_memory=False)
logger.debug("Data Frame df created")

# take required fields
df1=df[['Email', 'Created at' ]]

#let there be only one row for each order
df1=df1.drop_duplicates().reset_index().drop('index',1)

#getting only date from created at field
df1['Created at'] = df1.apply(lambda x: x['Created at'][:10], axis = 1)
    
df1=df1.drop_duplicates().reset_index().drop('index',1)

df1.columns=['Email', 'Date']
df1.loc[:, 'Difference'] = 0
df1.loc[:, 'Count'] = 0

df1.loc[:, 'Date'] =pd.to_datetime(df1.Date)
df1=df1.sort_values(by = ['Email', 'Date']).reset_index().drop('index',1)

n = 1
for i in range(0,max(df1.index)):
    if(df1.iloc[i,0]==df1.iloc[i+1,0]):
        n = n + 1 #count total number of orders
        df1.iloc[i+1,2]=(df1.iloc[i+1,1]-df1.iloc[i,1]).days #count days between orders
        if (i == max(df1.index) - 1):
            df1.iloc[i + 1,3] = n
    else:
        df1.iloc[i,3] = n
        n = 1

df1 = df1.groupby('Email', axis=0, as_index=False).sum()

#calculate average days between orders
df1.loc[:, 'Average'] = df1.apply(lambda x: x['Difference']/float(x['Count']), axis = 1)

df2 = df1[['Average']]
df2 = df2[df2.Average != 0] #retain average > 0
df2 = df2.reset_index().drop('index',1)
a = df2['Average'].tolist()
a = np.asarray(a)

df4 = pd.DataFrame(columns = ['Days between orders', 'Customers'])
df4['Days between orders'] = pd.Series(['1 to 4', '4 to 7', '7 to 15', '15 to 30', '30 to 60', '60 to 100', '100+'])

#count orders in given range
df4.iloc[0, 1] = np.compress((0 < a) & (a < 4), a).size
df4.iloc[1, 1] = np.compress((4 <= a) & (a < 7), a).size
df4.iloc[2, 1] = np.compress((7 <= a) & (a < 15), a).size
df4.iloc[3, 1] = np.compress((15 <= a) & (a < 30), a).size
df4.iloc[4, 1] = np.compress((30 <= a) & (a < 60), a).size
df4.iloc[5, 1] = np.compress((60 <= a) & (a < 100), a).size
df4.iloc[6, 1] = np.compress((100 <= a), a).size

df4.to_csv(cfg['root']+cfg['dir_data_output']+cfg['output_order_frequency'], index = False)

#!/home/cloudera/local/lib/python2.6/site-packages/bin/python

# Author : Sai Sree Kamineni
# Date created : Jan 10, 2016
# Execution frequency : Weekly
# Inputs refresh frequency : Weekly

# Input1 : data_input/vend/vend-total_revenue-for-type-by-month.csv
# Input2 : data_input/vend/vend-gross_profit-for-type-by-month.csv
# Output : data_output/revenue_by_type.csv
# Purpose : Gives the revenue, gross profit of each type of product for each month

import pandas as pd
from datetime import datetime
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
logger = logging.getLogger(cfg['log_revenue_by_type'])
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.FileHandler(cfg['root'] + cfg['dir_logs'] + cfg['log_revenue_by_type'] + "_" + time.strftime("%Y_%m_%d_%H_%M_%S") + ".log" )
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

df = pd.read_csv(cfg['root']+cfg['dir_data_vend']+cfg["input_total_revenue_type"])
logger.debug("Data Frame df created")

def convertDate(data):
    return pd.to_datetime(datetime.strptime(data, '%b %Y')).date()

df = df[:max(df.index)-4] #remove aggregated rows
df = df[range(0,len(df.columns)-5)] #remove unwanted columns

df3 = df.groupby('Type', axis=0, as_index=True).sum() #columns include only months
df = df.groupby('Type', axis=0, as_index=False).sum() # columns include type and months

df1 = pd.DataFrame(columns = ['Date', 'Revenue', 'Gross Profit', 'Type'])

b = []
a = df['Type'].tolist() #create a list of type
for i in range(0, len(df3.columns)): #count of months
    b = b + a
df1['Type'] = pd.Series(b)


b = []
x = df3.columns.tolist() #create a list of months
for a in x: #take each month
    for i in range(0, len(df.index)): #count of types
        b.append(a)
df1['Date'] = pd.Series(b)

df1.loc[:, 'Date'] = df1.Date.apply(convertDate)

b = []
for i in df3.columns: #get each month
    a = df[i].tolist() #each month column to list
    b = b + a
df1['Revenue'] = pd.Series(b)
df = pd.read_csv(cfg['root']+cfg['dir_data_vend']+cfg["input_gross_profit_type"])
df = df[:max(df.index)-4]
df = df[range(0,len(df.columns)-5)]

df3 = df.groupby('Type', axis=0, as_index=True).sum() #columns include only months
df = df.groupby('Type', axis=0, as_index=False).sum() # columns include type and months

b = []
for i in df3.columns: #get each month
    a = df[i].tolist() #each month column to list
    b = b + a
df1['Gross Profit'] = pd.Series(b)

df1.to_csv(cfg['root']+cfg['dir_data_output']+cfg['output_revenue_by_type'], index = False)

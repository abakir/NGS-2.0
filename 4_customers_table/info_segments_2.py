#!/home/cloudera/local/lib/python2.6/site-packages/bin/python

# Author : Sai Sree Kamineni
# Date created : Jan 10, 2016
# Execution frequency : Weekly
# Input(1-2) refresh frequency : Weekly
# Input3 refresh frequency : Constant

# Input1 : data_input/shopify/export_orders.csv
# Input2 : data_input/shopify/customers_export.csv
# Input3 : data_input/enodos/segments.csv
# Output : 4_customers_table/info_segments_2.csv
# Purpose : Gives customer details along with thier segment, total revenue, total basket value
# Basket value  =  total revenue / number of orders

import pandas as pd
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
logger = logging.getLogger(cfg['log_info_segments_2'])
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.FileHandler(cfg['root'] + cfg['dir_logs'] + cfg['log_info_segments_2'] + "_" + time.strftime("%Y_%m_%d_%H_%M_%S") + ".log" )
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

df = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["input_orders"],low_memory=False)
logger.debug("Data Frame df created")
df1 = df

#get required columns and rename
df = df[[ 'Email', 'Lineitem quantity', 'Lineitem price']]
df.columns = ['Email', 'Quantity', 'Price']

#calculate revenue
df.loc[:, 'Revenue'] = df['Quantity'] * df['Price']
df = df[['Email', 'Revenue']]

#calculate total revenue per customer
df = df.groupby('Email', axis = 0, as_index=False).sum()

#get required columns and rename
df1 = df1[['Name', 'Email']]
df1.columns = ['Orders', 'Email']
df1 = df1.drop_duplicates().reset_index().drop('index', 1)

#calculate total orders per customer
df1['Orders'] = 1
df1 = df1.groupby('Email', axis = 0, as_index=False).sum()
df = df.merge(df1, on = ['Email'], how = 'inner')

#calculate basket value
df.loc[:, 'Basket Value'] = df['Revenue'] / df['Orders']
customer_value = df[['Email', 'Revenue', 'Basket Value']]

customers = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["input_customers"], low_memory=False)

#concatenate name, address
customers.loc[:, 'Name'] = customers.loc[:, 'First Name'] + " " + customers.loc[:, 'Last Name']
customers.loc[:, 'Address'] = customers.loc[:, 'Address1'] + " " + customers.loc[:, 'Address2'] + " " + customers.loc[:, 'City']


customers = customers[['Name', 'Address', 'Phone', 'Email']]

customer_value = customers.merge(customer_value, on = ['Email'], how = 'inner')

raw_segments = pd.read_csv(cfg['root']+cfg['dir_data_enodos']+cfg["io_segments"])

#join with new dataframe
customer_info_with_segments = raw_segments.merge(customer_value, on = ['Email'], how = 'outer')
customer_info_with_segments = customer_info_with_segments[['Name', 'Revenue', 'Basket Value', 'Segment', 'Email', 'Address', 'Phone' ]]


customer_info_with_segments.to_csv(cfg['root']+cfg['dir_customers_table']+cfg['io_info_segments_2'], index=False)

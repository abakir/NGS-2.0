#!/home/cloudera/local/lib/python2.6/site-packages/bin/python

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
logger = logging.getLogger(cfg['log_all_bought_together'])
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
make_sure_path_exists(cfg['root'] + cfg['dir_logs'])
ch = logging.FileHandler(cfg['root'] + cfg['dir_logs'] + cfg['log_all_bought_together'] + "_" + time.strftime("%Y_%m_%d_%H_%M_%S") + ".log" )
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

df = pd.read_csv(cfg['root'] + cfg['dir_data_shopify'] + cfg["input_orders"], low_memory=False)
logger.debug("Data Frame df created")

# rows of type 21 and 12 are same, retain one instance of such rows
def combineProds(name1, name2):
    if name1 < name2:
        return name1 + "," + name2
    else:
        return name2 + "," + name1

# get required columns and rename
df = df[['Name', 'Lineitem name']]
df.columns = ['Order_id', 'Product']

# change product names to upper, remove duplicates, group the orders with same product
df['Product'] = df.Product.apply(lambda x: x.upper())
df = df.drop_duplicates().reset_index().drop('index', 1)
df = df.groupby(['Product'], axis=0, as_index=False)['Order_id'].sum()
# will be changed into a list by splitting with # (the start of order number)
df['Order_id'] = df.Order_id.apply(lambda x: x.split("#"))
# remove the first , in the list
df['Order_id'] = df.Order_id.apply(lambda x: x[1:])

key = []
for i in range(0, max(df.index)+1):
    key.append(1)
df['key'] = key

df.columns = ['Product1', 'Order1', 'key']
df1 = pd.DataFrame(columns = ['key', 'Product2', 'Order2'])

df1['key'] = df['key']
df1['Product2'] = df['Product1']
df1['Order2'] = df['Order1']

#cartesian product
df = pd.merge(df, df1,on='key')[['Product1', 'Product2', 'Order1', 'Order2']]

df = df[df.Product1 != df.Product2]

df['newcol'] = df.apply(lambda x: combineProds(x['Product1'], x['Product2']), axis=1)
df = df.drop_duplicates('newcol').reset_index().drop('index', 1)
df.loc[:, 'Count'] = df.apply(lambda x: len(list(set(x['Order1']).intersection(x['Order2']))), axis=1)
df = df[['Product1', 'Product2', 'Count']]

df = df[df.Count != 0]
df = df.sort_values(by = ['Count'], ascending=False).reset_index().drop('index', 1)

make_sure_path_exists(cfg['root'] + cfg['dir_data_output'] )

df.to_csv(cfg['root'] + cfg['dir_data_output'] + cfg['output_all_bought_together'])
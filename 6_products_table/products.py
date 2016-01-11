#!/home/cloudera/local/lib/python2.6/site-packages/bin/python

# Author : Sai Sree Kamineni
# Date created : Jan 10, 2016
# Execution frequency : Weekly
# Input refresh frequency : Weekly

# Input1 : data_input/vend/vend-total_revenue-for-product_variant-by-month.csv
# Output : data_output/products.csv
# Purpose : Gives product details along with measurements like Revenue, Gross profit, 
# CMGR, period for calculating CMGR, % Total Revenue, Average Revenue, Average Gross Profit, 
# % Variation from Average, % Total Gross Profit
# CMGR = ((1st month revenue/total revenue)^(1/period)-1)*100
# period = number of months from the month product is sold till now
# % Total Revenue, % Total Gross Profit = measure of that product*100/total
# Average Revenue, Average Gross Profit = total / number of products
# % Variation from average = (revenue of the product - average revenue)*100/ average revenue

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
logger = logging.getLogger(cfg['log_products'])
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.FileHandler(cfg['root'] + cfg['dir_logs'] + cfg['log_products'] + "_" + time.strftime("%Y_%m_%d_%H_%M_%S") + ".log" )
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

df = pd.read_csv(cfg['root']+cfg['dir_data_vend']+cfg["input_total_revenue_prod"])
logger.debug("Data Frame df created")

#remove the last rows which are aggregated values
df = df[:max(df.index)-4]

#remove null products, change to upper case
df = df[pd.notnull(df['Product'])]
df['Product'] = df['Product'].apply(lambda x: x.upper())
df = df.sort_values(by = 'Product')
df.fillna(0, inplace=True) #replace na values with 0
df = df[~df['Product'].str.contains("CAIRO")] #remove unnecessary rows
df = df.reset_index().drop('index',1)
df = df.groupby(['Product', 'Brand', 'Type'], axis=0, as_index=False).sum()

#list of column numbers
n=range(4,len(df.columns)-4)
n=[0]+[1]+[2]+n
n.append(len(df.columns)-3)

#subset the columns
df=df[n]

for i in range(0,max(df.index)+1):
    for j in range(3,len(df.columns)-1):
        #To get the first non zero revenue month
        if(df.iloc[i,j]!=0):
            t=1
            break
    #TO get the total no of months        
    n=len(df.columns)-1-j
    m=n**(-1)
    y=df.loc[i, 'Revenue']/df.iloc[i,j]
    df.loc[i,'CMGR']=(pow(y,m)-1)*100
    df.loc[i,'Period'] = n
    
gprofit = df[['Product', 'Brand', 'Type', 'Revenue' , 'Gross Profit', 'CMGR', 'Period']]

total = gprofit['Revenue'].sum() #total revenue
totalgp = gprofit['Gross Profit'].sum() #total gross profit
gprofit.loc[:, '%Total Revenue'] = gprofit['Revenue'].apply(lambda x: x*100/total) #%total revenue
totprods = max(gprofit.index) + 1
temp = total/totprods #total revenue / total products
gprofit.loc[:, 'Average Revenue'] = temp #avg revenue
gprofit.loc[:, 'Average Gross Profit'] = totalgp/totprods #avg gross profit

gprofit.loc[:, '% Variation from Average'] = gprofit['Revenue'].apply(lambda x: (x-temp)*100/temp)
total = gprofit['Gross Profit'].sum()
gprofit.loc[:, '%Total Gross Profit'] = gprofit['Gross Profit'].apply(lambda x: x*100/total)

gprofit.to_csv(cfg['root']+cfg['dir_data_output']+cfg['output_products'], index=False)

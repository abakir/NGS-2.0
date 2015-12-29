#!/home/cloudera/local/lib/python2.6/site-packages/bin/python

import pandas as pd
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
logger = logging.getLogger(cfg['log_customers'])
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.FileHandler(cfg['root'] + cfg['dir_logs'] + cfg['log_customers'] + "_" + time.strftime("%Y_%m_%d_%H_%M_%S") + ".log" )
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

df = pd.read_csv(cfg['root']+cfg['dir_customers_table']+cfg['io_info_segments_2'])
df1 = pd.read_csv(cfg['root']+cfg['dir_customers_table']+cfg['io_latest_order_days_3'])
df3 = pd.read_csv(cfg['root']+cfg['dir_customers_table']+cfg['io_order_frequency_customer_4'])
df5 = pd.read_csv(cfg['root']+cfg['dir_customers_table']+cfg['io_day_hour_purchases_1'])
logger.debug("Data Frames opened")
df.columns = ['Name', 'Revenue', 'Basket Value', 'Segment', 'Email', 'Address', 'Phone' ]

#merge data frames
df10 = df1.merge(df3, on = ['Email'], how = 'inner')
df6 = df10.merge(df5, on = ['Email'], how = 'inner')
df6 = df6.merge(df, on = ['Email'], how = 'outer')

df2 = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["ip_customers"])
df2 = df6.merge(df2, on = ['Email'], how = 'inner')

df2 = df2[['First Name', 'Last Name', 'Total orders', 'Average days between orders', 'Email', 'Days from Last order', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', '0:00 - 2:00', '2:00 - 4:00', '4:00 - 6:00', '6:00 - 8:00', '8:00 - 10:00', '10:00 - 12:00', '12:00 - 14:00', '14:00 - 16:00', '16:00 - 18:00', '18:00 - 20:00', '20:00 - 22:00', '22:00 - 0:00']]

#concatenate name
for i in range(0, max(df2.index)+1):
    df2.loc[i, 'Name'] = df2.loc[i, 'First Name'] + " " + df2.loc[i, 'Last Name']

df4 = df2.merge(df, on = ['Name', 'Email'], how = 'inner')

df4 = df4[['Name', 'Revenue', 'Basket Value', 'Segment', 'Address','Phone', 'Total orders', 'Average days between orders', 'Email', 'Days from Last order', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', '0:00 - 2:00', '2:00 - 4:00', '4:00 - 6:00', '6:00 - 8:00', '8:00 - 10:00', '10:00 - 12:00', '12:00 - 14:00', '14:00 - 16:00', '16:00 - 18:00', '18:00 - 20:00', '20:00 - 22:00', '22:00 - 0:00']]

df = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["ip_orders"],low_memory=False)
df1 = df[['Lineitem quantity', 'Lineitem price']]

#calculate revenue
df1.loc[:, 'Revenue'] = df1.apply(lambda x: x['Lineitem quantity'] * x['Lineitem price'], axis=1)

df1=df1[['Lineitem quantity', 'Revenue']]
df = df[['Email']]
df = df.drop_duplicates().reset_index().drop('index',1)

#calculate avg revenue, avg basket size
df1 = df1.sum()
df4['Average Revenue'] = df1['Revenue']/float(max(df.index)+1)
df4['Average Basket Size'] = df1['Lineitem quantity']/float(max(df.index)+1)

df3 = pd.read_csv(cfg['root']+cfg['dir_monthly_segments']+cfg['op_cust_score'])
df3.columns = [u'Email', u'Beef', u'Dried fruits and nuts', u'Fresh Beef & Poultry', u'Fruits', u'General', u'Rice and pasta', u'Vegetables', u'Beverages', u'Cosmetics', u'Dairy', u'Food supplements', u'Grains_Seeds_Cereal', u'Herbs', u'Oil_ Vinegar_Sauces', u'Bakery', u'Condiments and paste', u'Poultry', u'Chocolate_cookies_snacks', u'Jams & spreads & honey', u'Non-dairy', u'Seafood', u'Gluten free', u'Herbal pharmacy', u'Tea and herbal drinks', u'Accessories', u'Lamb', u'Prepared food', u'Home appliance', u'Books']
df4 = df4.merge(df3, on = ['Email'], how='left')

df4[['Total orders', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', '0:00 - 2:00', '2:00 - 4:00', '4:00 - 6:00', '6:00 - 8:00', '8:00 - 10:00', '10:00 - 12:00', '12:00 - 14:00', '14:00 - 16:00', '16:00 - 18:00', '18:00 - 20:00', '20:00 - 22:00', '22:00 - 0:00']] = df4[['Total orders', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', '0:00 - 2:00', '2:00 - 4:00', '4:00 - 6:00', '6:00 - 8:00', '8:00 - 10:00', '10:00 - 12:00', '12:00 - 14:00', '14:00 - 16:00', '16:00 - 18:00', '18:00 - 20:00', '20:00 - 22:00', '22:00 - 0:00']].astype(float)
df4.to_csv(cfg['root']+cfg['dir_data_output']+cfg['op_customers'],index = False)

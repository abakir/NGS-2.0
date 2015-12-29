#!/home/cloudera/local/lib/python2.6/site-packages/bin/python

import pandas as pd
import re
from datetime import datetime
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
logger = logging.getLogger(cfg['log_year_to_date_by_day'])
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.FileHandler(cfg['root'] + cfg['dir_logs'] + cfg['log_year_to_date_by_day'] + "_" + time.strftime("%Y_%m_%d_%H_%M_%S") + ".log" )
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

df = pd.read_csv(cfg['root']+cfg['dir_data_vend']+cfg["ip_total_revenue_daily"])
logger.debug("Data Frame df created")

#get date
def convertDate(data):
    matchobj = re.match(r'(.*) (.*) (.*) (.*).*',data)
    data = matchobj.group(2)[:-2] + " " + matchobj.group(3) + " " + matchobj.group(4)
    return pd.to_datetime(datetime.strptime(data, '%d %b %Y')).date()
    
#get required columns
df = df[1:5]
df = df.set_index('Unnamed: 0')

df = df.transpose() #transpose the df
df = df.reset_index()

#remove aggregated rows
df = df[:max(df.index)-4]
df.columns = ['Date', 'Revenue', 'Cost of Goods', 'Gross Profit', 'Margin']

df.loc[:, 'Date'] = df.Date.apply(convertDate)
df.to_csv(cfg['root']+cfg['dir_data_output']+cfg['op_year_to_date_by_day'], index = False)

import pandas as pd
import re
from datetime import datetime
import yaml

with open("config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

df = pd.read_csv(cfg['root']+cfg['dir_data_vend']+cfg["ip_total_revenue_daily"])

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

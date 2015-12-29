import pandas as pd
from datetime import datetime
import yaml

with open("config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

df = pd.read_csv(cfg['root']+cfg['dir_data_vend']+cfg["ip_total_revenue_type"])

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
df = pd.read_csv(cfg['root']+cfg['dir_data_vend']+cfg["ip_gross_profit_type"])
df = df[:max(df.index)-4]
df = df[range(0,len(df.columns)-5)]

df3 = df.groupby('Type', axis=0, as_index=True).sum() #columns include only months
df = df.groupby('Type', axis=0, as_index=False).sum() # columns include type and months

b = []
for i in df3.columns: #get each month
    a = df[i].tolist() #each month column to list
    b = b + a
df1['Gross Profit'] = pd.Series(b)

df1.to_csv(cfg['root']+cfg['dir_data_output']+cfg['op_revenue_by_type'], index = False)

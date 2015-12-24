import pandas as pd
from datetime import datetime
import yaml

with open("config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

df = pd.read_csv(cfg['root']+cfg['dir_data_vend']+cfg["ip_total_revenue_prod"])

df = df[[0,3,5]+range(7,len(df.columns)-5)]#reqd columns
df = df[:max(df.index)-4] #reqd rows

#remove null products, change to upper case
df = df[pd.notnull(df['Product'])]
df['Product'] = df['Product'].apply(lambda x: x.upper())

df = df.sort('Product')
df.fillna(0, inplace=True) #replace na values with 0
df = df[~df['Product'].str.contains("CAIRO")] #remove unnecessary rows
df = df.reset_index().drop('index',1)
df = df.groupby(['Product', 'Brand', 'Type'], axis=0, as_index=False).sum()


prods = df['Product'].tolist()
new_prods = []
for i in prods:
    for j in range(0, len(df.columns)-3):
        new_prods.append(i) #add each product total products number of times
        
brands = df['Brand'].tolist()
new_brands = []
for i in brands:
    for j in range(0, len(df.columns)-3):
        new_brands.append(i) #add each brand total products number of times
        
types = df['Type'].tolist()
new_types = []
for i in types:
    for j in range(0, len(df.columns)-3):
        new_types.append(i) #add each type total products number of times
        
df1 = df[range(3,len(df.columns))]
dates = df1.columns.tolist()
new_dates = []
for i in range(0, max(df.index)+1):
    new_dates = new_dates + dates #add all dates total products number of times
    
#create new df and initialize columns
df2 = pd.DataFrame(columns=['Product', 'Brand', 'Type', 'Date'])
df2['Product'] = new_prods
df2['Brand'] = new_brands
df2['Type'] = new_types
df2['Date'] = new_dates

#create a concatenated column and make index
df2['New name'] = df2.apply(lambda x: str(x['Product']) +","+ str(x['Brand']) +","+ str(x['Type']), axis=1)
df['New name'] = df.apply(lambda x: str(x['Product']) +","+ str(x['Brand']) +","+ str(x['Type']), axis=1)
df = df.set_index('New name')
df.index.name = None

#retrieve revenue for each product date combination from old df
for i in range(0, max(df2.index)+1):
    df2.loc[i, 'Revenue'] = df.loc[df2.loc[i, 'New name'], df2.loc[i, 'Date']]
    
df = pd.read_csv(cfg['root']+cfg['dir_data_vend']+cfg["ip_gross_profit_prod"])

df = df[[0,3,5]+range(7,len(df.columns)-5)]#reqd columns
df = df[:max(df.index)-4] #reqd rows

#remove null products, change to upper case
df = df[pd.notnull(df['Product'])]
df['Product'] = df['Product'].apply(lambda x: x.upper())

df = df.sort('Product')
df.fillna(0, inplace=True) #replace na values with 0
df = df[~df['Product'].str.contains("CAIRO")] #remove unnecessary rows
df = df.reset_index().drop('index',1)
df = df.groupby(['Product', 'Brand', 'Type'], axis=0, as_index=False).sum()

#create a concatenated column and make index
df['New name'] = df.apply(lambda x: str(x['Product']) +","+ str(x['Brand']) +","+ str(x['Type']), axis=1)
df = df.set_index('New name')
df.index.name = None

#retrieve gross profit for each product date combination from old df
for i in range(0, max(df2.index)+1):
    df2.loc[i, 'Gross Profit'] = df.loc[df2.loc[i, 'New name'], df2.loc[i, 'Date']]
    
df2 = df2[['Product', 'Brand', 'Type', 'Date', 'Revenue', 'Gross Profit']]
df2['Date'] = df2.Date.apply(lambda x: pd.to_datetime(datetime.strptime(x, '%b %Y')).date())

gprofit = df2[['Product', 'Brand', 'Type', 'Date', 'Revenue' , 'Gross Profit']]

total = gprofit['Revenue'].sum() #total revenue
totalgp = gprofit['Gross Profit'].sum() #total gross profit
gprofit['%Total Revenue'] = gprofit['Revenue'].apply(lambda x: x*100/total) #%total revenue
totprods = max(gprofit.index) + 1
temp = total/totprods #total revenue / total products
gprofit['Average Revenue'] = temp #avg revenue
gprofit['Average Gross Profit'] = totalgp/totprods #avg gross profit

gprofit['% Variation from Average'] = gprofit['Revenue'].apply(lambda x: (x-temp)*100/temp)
total = gprofit['Gross Profit'].sum()
gprofit['%Total Gross Profit'] = gprofit['Gross Profit'].apply(lambda x: x*100/total)

df1 = pd.read_csv(cfg['root']+cfg['dir_data_output']+cfg['op_products'])
df1 = df1[['Product', 'Brand', 'Type', 'CMGR']]
df1['Product'] = df1.apply(lambda x: str(x['Product']) +","+ str(x['Brand']) +","+ str(x['Type']), axis=1)
df1 = df1[['Product', 'CMGR']]
df1 = df1.set_index('Product')
df1.index.name = None
                                           
# get CGMR for each product from products table                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
gprofit['Newname'] = gprofit.apply(lambda x: str(x['Product']) +","+ str(x['Brand']) +","+ str(x['Type']), axis=1)
for i in range(0, max(gprofit.index)+1):
    gprofit.loc[i,'CMGR'] = df1.loc[gprofit.loc[i,'Newname'],'CMGR']
    
gprofit = gprofit[['Product', 'Brand', 'Type', 'Date', 'Revenue', 'Gross Profit', 'CMGR', 'Average Revenue', 'Average Gross Profit', '%Total Revenue', '% Variation from Average', '%Total Gross Profit']]
gprofit=gprofit[gprofit['Revenue'] != 0] #remove products with zero revenue
gprofit.to_csv(cfg['root']+cfg['dir_data_output']+cfg['op_products_dates'], index=False)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    

import pandas as pd
import numpy as np
import yaml

with open("config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

df = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["ip_orders"],low_memory=False)

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

df2 = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["ip_customers"])
df2 = df1.merge(df2, on = ['Email'], how = 'inner')


df2 = df2[['Average']]
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

df4.to_csv(cfg['root']+cfg['dir_data_output']+cfg['op_order_frequency'], index = False)

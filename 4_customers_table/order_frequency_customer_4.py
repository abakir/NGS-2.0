import pandas as pd
import yaml

with open("config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

df = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["ip_orders"],low_memory=False)

# take required fields
df1=df[['Name','Email', 'Created at' ]]

df1=df1.drop_duplicates().reset_index().drop('index',1)

#getting only date from created at field
df1['Created at'] = df1.apply(lambda x: x['Created at'][:10], axis = 1)
    
df1=df1.drop_duplicates().reset_index().drop('index',1)

df1.columns=['Name','Email', 'Date']
df1.loc[:, 'Difference'] = 0
df1.loc[:, 'Total orders'] = 0

df1.loc[:, 'Date'] =pd.to_datetime(df1.Date)
df1=df1.sort_values(by = ['Email', 'Date']).reset_index().drop('index',1)

n = 1
for i in range(0,max(df1.index)):
    if(df1.iloc[i,1]==df1.iloc[i+1,1]):
        n = n + 1 #count total orders
        df1.iloc[i+1,3]=(df1.iloc[i+1,2]-df1.iloc[i,2]).days #count days between two orders
        if (i == max(df1.index) - 1):
            df1.iloc[i + 1,4] = n
    else:
        df1.iloc[i,4] = n
        n = 1

#calculate total orders, days between orders for each customer
df1 = df1.groupby('Email', axis=0, as_index=False).sum()

#calculate average days between orders
df1.loc[:, 'Average days between orders'] = df1.apply(lambda x: x['Difference']/float(x['Total orders']), axis = 1)
df2 = df1[['Email', 'Total orders', 'Average days between orders']]
         
df2.to_csv(cfg['root']+cfg['dir_customers_table']+cfg['io_order_frequency_customer_4'],index=False)

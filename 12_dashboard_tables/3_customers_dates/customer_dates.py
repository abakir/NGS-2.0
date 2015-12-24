import pandas as pd
from datetime import datetime
import re
import os
import yaml

with open("config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

df = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["ip_orders"])


df = df[['Name', 'Email', 'Created at', 'Lineitem quantity', 'Lineitem price']]
#calculate revenue
df['Revenue'] = df.apply(lambda x: x['Lineitem quantity']*x['Lineitem price'], axis=1)

#get required columns and rename
df = df[['Name', 'Email', 'Created at', 'Revenue']]
df.columns = ['Name', 'Email', 'Date', 'Revenue']

df['Date1'] = df['Date']

#get date as day
def getDay(data):
    matchobj = re.match(r'(.*) (.*) (.*).*',data)
    data = matchobj.group(1)
    return pd.to_datetime(datetime.strptime(data, '%Y-%m-%d')).date().strftime("%A")

#get hours of the order
def getHours(data):
    matchobj = re.match(r'(.*) (.*) (.*).*',data)
    data = matchobj.group(2)
    num = pd.to_datetime(datetime.strptime(data, '%H:%M:%S')).hour
    if(num < 2):
        return 0
    elif((num >= 2) & (num < 4)):
        return 2
    elif((num >= 4) & (num < 6)):
        return 4
    elif((num >= 6) & (num < 8)):
        return 6
    elif((num >= 8) & (num < 10)):
        return 8
    elif((num >= 10) & (num < 12)):
        return 10
    elif((num >= 12) & (num < 14)):
        return 12
    elif((num >= 14) & (num < 16)):
        return 14
    elif((num >= 16) & (num < 18)):
        return 16
    elif((num >= 18) & (num < 20)):
        return 18
    elif((num >= 20) & (num < 22)):
        return 20
    elif(num >= 22):
        return 22
        
df['Day'] = df.Date1.apply(getDay)
df['Hours'] = df.Date1.apply(getHours)

df = df[['Name', 'Email', 'Date', 'Revenue', 'Day', 'Hours']]


def convertDate(data):
    matchobj = re.match(r'(.*) (.*) (.*).*',data)
    data = matchobj.group(1)
    return data
df['Date'] = df.Date.apply(convertDate)
df = df.sort('Date', ascending=False).reset_index().drop('index',1)

today = DT.date.today()
dt = today - DT.timedelta(days=7)
i=0
while (i <= max(df.index)):
    if (str(df.loc[i, 'Date']) >= str(dt)): #compare dates
        df.loc[i, 'Date'] = dt #set thursday date
        i = i+1
    else:
        dt = dt - DT.timedelta(days=7)
        i=i-1

df = df.drop_duplicates().reset_index().drop('index',1)

data = df[['Email', 'Date', 'Revenue']]
df = df[['Email', 'Date', 'Day', 'Hours']]

df = df.drop_duplicates().reset_index().drop('index',1)

df1 = pd.DataFrame(columns = ['Email', 'Date', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22])
df1['Email'] = df['Email']
df1['Date'] = df['Date']
df1['Sunday'] = df1['Monday'] = df1['Tuesday'] = df1['Wednesday'] = df1['Thursday'] = df1['Friday'] = df1['Saturday'] = df1[0] = df1[2] = df1[4] = df1[6] = df1[8] = df1[10] = df1[12] = df1[14] = df1[16] = df1[18] = df1[20] = df1[22] =0

#assign values to hours and days columns
for i in range(0, max(df.index)+1):
    df1.loc[i,(df.iloc[i,2])] = 1
    df1.loc[i,(df.iloc[i,3])] = 1
    
#count total orders per hours, days
df1 = df1.groupby(['Email', 'Date'], axis = 0, as_index=False).sum()

#rename columns
df1.columns = ['Email', 'Date', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', '0:00 - 2:00', '2:00 - 4:00', '4:00 - 6:00', '6:00 - 8:00', '8:00 - 10:00', '10:00 - 12:00', '12:00 - 14:00', '14:00 - 16:00', '16:00 - 18:00', '18:00 - 20:00', '20:00 - 22:00', '22:00 - 0:00']

df5 = data[['Email', 'Date', 'Revenue']]
df2 = data[['Email', 'Date']]
df2 = df2.drop_duplicates().reset_index().drop('index',1)
df2['Total orders'] = 1
df5 = df5.groupby(['Email', 'Date'], as_index=False).sum()
df2 = df2.groupby(['Email', 'Date'], as_index=False).sum()

#join data frames
df3 = df2.merge(df5, on = ['Email', 'Date'], how = 'inner')

#calculate basket value and join data frames
df3['Basket Value'] = df3.apply(lambda x: x['Revenue']/float(x['Total orders']), axis=1)
df3 = df3.merge(df1, on = ['Email', 'Date'], how = 'inner')

df = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["ip_orders"])

#get required columns
df1 = df[['Lineitem quantity', 'Lineitem price']]

#calculate revenue
df1['Revenue'] = df1.apply(lambda x: x['Lineitem quantity'] * x['Lineitem price'], axis=1)

#get required columns
df1=df1[['Lineitem quantity', 'Revenue']]
df = df[['Email']]

df = df.drop_duplicates().reset_index().drop('index',1)

#calculate avg revenue, avg basket size
df1 = df1.sum()
df3['Average Revenue'] = df1['Revenue']/float(max(df.index)+1)
df3['Average Basket Size'] = df1['Lineitem quantity']/float(max(df.index)+1)

customers = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["ip_customers"])

#concatenate name, address
customers['Name'] = customers['First Name'] + " " + customers['Last Name']
customers['Address'] = customers['Address1'] + " " + customers['Address2'] + " " + customers['City']

customers = customers[['Name', 'Address', 'Phone', 'Email']]

df3 = customers.merge(df3, on = ['Email'], how = 'inner')

df3.to_csv(cfg['root']+cfg['dir_data_output']+cfg['op_customer_dates'], index=False)
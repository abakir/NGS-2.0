import pandas as pd
from datetime import datetime
import re
import os
import yaml

with open("config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

df = pd.read_csv(cfg['root']+cfg['dir_data_shopify']+cfg["ip_orders"], low_memory=False)

#function to get month and year
def convertDate(data):
    matchobj = re.match(r'(.*) (.*) (.*).*',data)
    data = matchobj.group(1)
    matchobj = re.match(r'(.*)-(.*)-(.*).*',data)
    data = matchobj.group(1) + "-" + matchobj.group(2) + "-01"
    return data

#count all the unique orders
def getUnique(data):
    return len(data) - len(set(data))
    
#rows of type 21 and 12 are same, retain one instance of such rows
def combineProds(date1, name1, name2, count):
    if(name1 < name2):
        return date1 + "," + name1 + "," + name2 + "," +count
    else:
        return date1 + "," + name2 + "," + name1 + "," +count


df = df[['Name', 'Lineitem name', 'Created at']] #subset required columns
df.columns = ['Name', 'Product', 'Date'] #rename columns

#change product names to upper, remove duplicates, group the orders with same product, date
df['Date'] = df.Date.apply(convertDate) 
df['Product'] = df.Product.apply(lambda x: x.upper()) 
df = df.drop_duplicates().reset_index().drop('index',1) 
df = df.groupby(['Date', 'Product'], axis = 0, as_index=False)['Name'].sum() 

df['Name'] = df.Name.apply(lambda x: x.split("#")) #split the order no's by #
df['Name'] = df.Name.apply(lambda x: x[1:]) #remove the extra comma

e = df['Date'].tolist() #create a list of dates
#create a list of distinct dates
d = []
for i in e:
    if i not in d:
        d.append(i)
        
df1 = pd.DataFrame(columns = ['Date', 'Product1', 'Product2','Name', 'Count']) #create new dataframe

#creates date as index column
df = df.set_index('Date') 

pr1 = []
pr2 = []
dat = []
for i in d: #traverse through all months
    p1 = []
    p2 = []
    leng =  len(df.loc[i,'Product']) #count the products in each month
    e = df.loc[i,'Product'].tolist() #make a list of all products in a month
    for j in range(0, leng): #iterate through the length
        p1 = p1 + e #add list of product 'leng' number of times
    for k in e: 
        for j in range(0, leng):
            p2.append(k) #add each product 'leng' number of times
            dat.append(i) #add each date 'leng' number of times
    pr1 = pr1 + p1
    pr2 = pr2 + p2
df1['Date'] = pd.Series(dat) #update date column in new df
df1['Product1'] = pd.Series(pr1) #update product1 column in new df
df1['Product2'] = pd.Series(pr2) #update product2 column in new df

na = []
for i in d:
    #create a list of lists having orders for each product. 
    e = df.loc[i,'Name'].tolist()
    #add each list of orders for a product with all the list o orders of one product
    for k in e:
        for j in e:
             p = k + j
             na.append(p)
df1['Name'] = pd.Series(na) #create a series for list 'na' and add to name column

df1 = df1[df1.Product1 != df1.Product2] #remove the rows whose product1 and product2 rows are same

df1['Count'] = df1.Name.apply(getUnique)
df1 = df1[df1.Count != 0] #remove rows with 0 count

df1['newcol'] = df1.apply(lambda x: combineProds(str(x['Date']),x['Product1'], x['Product2'],str(x['Count'])), axis=1)
df1 = df1.drop_duplicates('newcol').reset_index().drop('index',1)

#get required products
df1 = df1[['Date', 'Product1', 'Product2', 'Count']]

#change the date format to datetime
df1['Date'] = df1.Date.apply(lambda x: pd.to_datetime(datetime.strptime(x, '%Y-%m-%d')).date())

#sort based on count in descending order
df1 = df1.sort(['Count'], ascending = False)
df1 = df1.reset_index().drop('index', 1)

df1.to_csv(cfg['root']+cfg['dir_data_output']+cfg['op_monthly_bought_together'])
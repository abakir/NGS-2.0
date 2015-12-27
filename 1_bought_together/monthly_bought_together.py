import pandas as pd
import re
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
    
#rows of type 21 and 12 are same, retain one instance of such rows
def combineProds(date1, name1, name2):
    if(name1 < name2):
        return date1 + "," + name1 + "," + name2
    else:
        return date1 + "," + name2 + "," + name1
        
df = df[['Name', 'Lineitem name', 'Created at']] #subset required columns
df.columns = ['Order_id', 'Product', 'Date'] #rename columns

#change product names to upper, remove duplicates, group the orders with same product, date
df['Date'] = df.Date.apply(convertDate) 
df['Product'] = df.Product.apply(lambda x: x.upper()) 
df = df.drop_duplicates().reset_index().drop('index',1) 
df = df.groupby(['Date', 'Product'], axis = 0, as_index=False)['Order_id'].sum() 

df['Order_id'] = df.Order_id.apply(lambda x: x.split("#")) #split the order no's by #
df['Order_id'] = df.Order_id.apply(lambda x: x[1:]) #remove the extra comma

df.columns = ['Date', 'Product1', 'Order1']
df1 = pd.DataFrame(columns = ['Date', 'Product2', 'Order2'])
df1['Date'] = df['Date']
df1['Product2'] = df['Product1']
df1['Order2'] = df['Order1']

#cartesian product
df = pd.merge(df, df1,on='Date')[['Date', 'Product1', 'Product2', 'Order1', 'Order2']]

df = df[df.Product1 != df.Product2]
df['newcol'] = df.apply(lambda x: combineProds(x['Date'], x['Product1'], x['Product2']), axis=1)
df = df.drop_duplicates('newcol').reset_index().drop('index', 1)
df['Count'] = df.apply(lambda x: len(list(set(x['Order1']).intersection(x['Order2']))), axis=1)

df = df[['Date', 'Product1', 'Product2', 'Count']]
df = df[df.Count != 0]
df = df.sort(['Count'], ascending=False).reset_index().drop('index', 1)


df.to_csv(cfg['root']+cfg['dir_data_output']+cfg['op_monthly_bought_together'])
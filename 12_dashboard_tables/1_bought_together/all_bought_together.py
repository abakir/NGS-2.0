import pandas as pd
import os
import yaml

with open("config.yaml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

df = pd.read_csv(cfg['root'] + cfg['dir_data_shopify'] + cfg["ip_orders"], low_memory=False)

# get required columns and rename
df = df[['Name', 'Lineitem name']]
df.columns = ['Name', 'Product']

# change product names to upper, remove duplicates, group the orders with same product
df['Product'] = df.Product.apply(lambda x: x.upper())
df = df.drop_duplicates().reset_index().drop('index', 1)
df = df.groupby(['Product'], axis=0, as_index=False)['Name'].sum()

# will be changed into a list by splitting with # (the start of order number)
df['Name'] = df.Name.apply(lambda x: x.split("#"))
# remove the first , in the list
df['Name'] = df.Name.apply(lambda x: x[1:])

# create new dataframe
df1 = pd.DataFrame(columns=['Product1', 'Product2', 'Name', 'Count'])

b = []
a = df['Product'].tolist()  # create a list of products
for i in range(0, max(df.index) + 1):  # count of months
    b = b + a  # add list 'a' to list 'b', 'i' number of times
df1['Product1'] = pd.Series(b)  # create a series for list 'b' and add to products column

b = []
x = df['Product'].tolist()  # create a list of products
for a in x:  # take each month
    for i in range(0, max(df.index) + 1):  # count of types
        b.append(a)  # add each element in 'a', 'i' number of times
df1['Product2'] = pd.Series(b)  # create a series for list 'b' and add to products column

# create a list of lists having orders for each product.
na = []
e = df['Name'].tolist()
# add each list of orders for a product with all the list o orders of one product
for k in e:
    for j in e:
        p = k + j
        na.append(p)

df1['Name'] = pd.Series(na)  # create a series for list 'na' and add to name column

df1 = df1[df1.Product1 != df1.Product2]  # remove the rows whose product1 and product2 rows are same


def getUnique(data):
    return len(data) - len(set(data))  # count all the unique orders


df1['Count'] = df1.Name.apply(getUnique)

df1 = df1[df1.Count != 0]  # remove rows with 0 count


# rows of type 21 and 12 are same, retain one instance of such rows
def combineProds(name1, name2, count):
    if name1 < name2:
        return name1 + "," + name2 + "," + count
    else:
        return name2 + "," + name1 + "," + count


df1['newcol'] = df1.apply(lambda x: combineProds(x['Product1'], x['Product2'], str(x['Count'])), axis=1)
df1 = df1.drop_duplicates('newcol').reset_index().drop('index', 1)

# get required products
df1 = df1[['Product1', 'Product2', 'Count']]

# sort based on count in descending order
df1 = df1.sort(['Count'], ascending=False)
df1 = df1.reset_index().drop('index', 1)

df1.to_csv(cfg['root'] + cfg['dir_data_output'] + cfg['op_all_bought_together'])

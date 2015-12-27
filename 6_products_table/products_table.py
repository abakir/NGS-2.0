import pandas as pd
import yaml

with open("config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

df = pd.read_csv(cfg['root']+cfg['dir_data_vend']+cfg["ip_total_revenue_prod"])

#remove the last rows which are aggregated values
df = df[:max(df.index)-4]

#remove null products, change to upper case
df = df[pd.notnull(df['Product'])]
df['Product'] = df['Product'].apply(lambda x: x.upper())
df = df.sort('Product')
df.fillna(0, inplace=True) #replace na values with 0
df = df[~df['Product'].str.contains("CAIRO")] #remove unnecessary rows
df = df.reset_index().drop('index',1)
df = df.groupby(['Product', 'Brand', 'Type'], axis=0, as_index=False).sum()

#list of column numbers
n=range(4,len(df.columns)-4)
n=[0]+[1]+[2]+n
n.append(len(df.columns)-3)

#subset the columns
df=df[n]

for i in range(0,max(df.index)+1):
    for j in range(3,len(df.columns)-1):
        #To get the first non zero revenue month
        if(df.iloc[i,j]!=0):
            t=1
            break
    #TO get the total no of months        
    n=len(df.columns)-1-j
    m=n**(-1)
    y=df.loc[i, 'Revenue']/df.iloc[i,j]
    df.loc[i,'CMGR']=(pow(y,m)-1)*100
    df.loc[i,'Period'] = n
    
gprofit = df[['Product', 'Brand', 'Type', 'Revenue' , 'Gross Profit', 'CMGR', 'Period']]

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

gprofit.to_csv(cfg['root']+cfg['dir_data_output']+cfg['op_products'], index=False)
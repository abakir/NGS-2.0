#!/home/cloudera/local/lib/python2.6/site-packages/bin/python

import yaml
import os
import sys
with open("/home/cloudera/Documents/12_dashboard_tables/config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)


sys.path.insert(0, os.path.join(cfg['root']+cfg['dir_bought_together']))
import all_bought_together
execfile(os.path.join(cfg['root']+cfg['dir_bought_together']+cfg['file_all_bought_together']))


sys.path.insert(0, os.path.join(cfg['root']+cfg['dir_bought_together']))
import monthly_bought_together
execfile(os.path.join(cfg['root']+cfg['dir_bought_together']+cfg['file_monthly_bought_together']))


#sys.path.insert(0, os.path.join(cfg['root']+cfg['dir_customer_retention']))
#import cust_retention
#execfile(os.path.join(cfg['root']+cfg['dir_customer_retention']+cfg['file_cust_retention']))


#sys.path.insert(0, os.path.join(cfg['root']+cfg['dir_customers_dates']))
#import customer_dates
#execfile(os.path.join(cfg['root']+cfg['dir_customers_dates']+cfg['file_customer_dates']))


#sys.path.insert(0, os.path.join(cfg['root']+cfg['dir_customers_table']))
#import day_hour_purchases_1
#execfile(os.path.join(cfg['root']+cfg['dir_customers_table']+cfg['file_day_hour_purchases_1']))


#sys.path.insert(0, os.path.join(cfg['root']+cfg['dir_customers_table']))
#import info_segments_2
#execfile(os.path.join(cfg['root']+cfg['dir_customers_table']+cfg['file_info_segments_2']))


#sys.path.insert(0, os.path.join(cfg['root']+cfg['dir_customers_table']))
#import latest_order_days_3
#execfile(os.path.join(cfg['root']+cfg['dir_customers_table']+cfg['file_latest_order_days_3']))


#sys.path.insert(0, os.path.join(cfg['root']+cfg['dir_customers_table']))
#import order_frequency_customer_4
#execfile(os.path.join(cfg['root']+cfg['dir_customers_table']+cfg['file_order_frequency_customer_4']))


#sys.path.insert(0, os.path.join(cfg['root']+cfg['dir_customers_table']))
#import customers
#execfile(os.path.join(cfg['root']+cfg['dir_customers_table']+cfg['file_customers']))


#sys.path.insert(0, os.path.join(cfg['root']+cfg['dir_order_frequency']))
#import order_frequency
#execfile(os.path.join(cfg['root']+cfg['dir_order_frequency']+cfg['file_order_frequency']))


#sys.path.insert(0, os.path.join(cfg['root']+cfg['dir_product_table']))
#import products_table
#execfile(os.path.join(cfg['root']+cfg['dir_product_table']+cfg['file_products_table']))


#sys.path.insert(0, os.path.join(cfg['root']+cfg['dir_products_dates']))
#import products_dates
#execfile(os.path.join(cfg['root']+cfg['dir_products_dates']+cfg['file_products_dates']))


#sys.path.insert(0, os.path.join(cfg['root']+cfg['dir_revenue_by_type']))
#import revenue_by_type
#execfile(os.path.join(cfg['root']+cfg['dir_revenue_by_type']+cfg['file_revenue_by_type']))


#sys.path.insert(0, os.path.join(cfg['root']+cfg['dir_unfulfilled_orders']))
#import week_unfulfilled_orders
#execfile(os.path.join(cfg['root']+cfg['dir_unfulfilled_orders']+cfg['file_week_unfulfilled_orders']))


#sys.path.insert(0, os.path.join(cfg['root']+cfg['dir_unfulfilled_orders']))
#import yr_unfulfilled_orders
#execfile(os.path.join(cfg['root']+cfg['dir_unfulfilled_orders']+cfg['file_yr_unfulfilled_orders']))


#sys.path.insert(0, os.path.join(cfg['root']+cfg['dir_year_to_date_by_day']))
#import year_to_date_by_day
#execfile(os.path.join(cfg['root']+cfg['dir_year_to_date_by_day']+cfg['file_year_to_date_by_day']))

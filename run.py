#!/home/cloudera/local/lib/python2.6/site-packages/bin/python

import yaml
import os
import sys
with open("config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

execfile(os.path.join(cfg['root']+cfg['dir_bought_together']+cfg['file_all_bought_together']))

execfile(os.path.join(cfg['root']+cfg['dir_bought_together']+cfg['file_monthly_bought_together']))

#execfile(os.path.join(cfg['root']+cfg['dir_customer_retention']+cfg['file_cust_retention']))

#execfile(os.path.join(cfg['root']+cfg['dir_customers_dates']+cfg['file_customer_dates']))

#execfile(os.path.join(cfg['root']+cfg['dir_customers_table']+cfg['file_day_hour_purchases_1']))

#execfile(os.path.join(cfg['root']+cfg['dir_customers_table']+cfg['file_info_segments_2']))

#execfile(os.path.join(cfg['root']+cfg['dir_customers_table']+cfg['file_latest_order_days_3']))

#execfile(os.path.join(cfg['root']+cfg['dir_customers_table']+cfg['file_order_frequency_customer_4']))

#execfile(os.path.join(cfg['root']+cfg['dir_customers_table']+cfg['file_customers']))

#execfile(os.path.join(cfg['root']+cfg['dir_order_frequency']+cfg['file_order_frequency']))

#execfile(os.path.join(cfg['root']+cfg['dir_product_table']+cfg['file_products_table']))

#execfile(os.path.join(cfg['root']+cfg['dir_products_dates']+cfg['file_products_dates']))

#execfile(os.path.join(cfg['root']+cfg['dir_revenue_by_type']+cfg['file_revenue_by_type']))

#execfile(os.path.join(cfg['root']+cfg['dir_unfulfilled_orders']+cfg['file_week_unfulfilled_orders']))

#execfile(os.path.join(cfg['root']+cfg['dir_unfulfilled_orders']+cfg['file_yr_unfulfilled_orders']))

#execfile(os.path.join(cfg['root']+cfg['dir_year_to_date_by_day']+cfg['file_year_to_date_by_day']))

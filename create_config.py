import os

# Author : Sai Sree Kamineni
# Date created : Jan 10, 2016
# Execution frequency : When the environment is changed

# Output file : config.yaml
# Purpose : To create configuration file

f = open('config.yaml', 'w')

# root folder
f.write("root: " + os.getcwd())

# subfolders
f.write("\ndir_logs: /logs")
f.write("\ndir_data_enodos: /data_input/enodos")
f.write("\ndir_data_shopify: /data_input/shopify")
f.write("\ndir_data_vend: /data_input/vend")
f.write("\ndir_bought_together: /1_bought_together")
f.write("\ndir_customer_retention: /2_customer_retention")
f.write("\ndir_customers_dates: /3_customers_dates")
f.write("\ndir_customers_table: /4_customers_table")
f.write("\ndir_order_frequency: /5_order_frequency")
f.write("\ndir_product_table: /6_products_table")
f.write("\ndir_products_dates: /7_products_dates")
f.write("\ndir_revenue_by_type: /8_revenue_by_type")
f.write("\ndir_unfulfilled_orders: /9_unfulfilled_orders")
f.write("\ndir_year_to_date_by_day: /10_year_to_date_by_day")
f.write("\ndir_monthly_segments: /11_monthly_segments")
f.write("\ndir_data_output: /data_output")

# code files with extention
f.write("\nfile_all_bought_together: /all_bought_together.py")
f.write("\nfile_monthly_bought_together: /monthly_bought_together.py")
f.write("\nfile_cust_retention: /cust_retention.py")
f.write("\nfile_customer_dates: /customer_dates.py")
f.write("\nfile_day_hour_purchases_1: /day_hour_purchases_1.py")
f.write("\nfile_info_segments_2: /info_segments_2.py")
f.write("\nfile_latest_order_days_3: /latest_order_days_3.py")
f.write("\nfile_order_frequency_customer_4: /order_frequency_customer_4.py")
f.write("\nfile_customers: /customers.py")
f.write("\nfile_order_frequency: /order_frequency.py")
f.write("\nfile_products: /products.py")
f.write("\nfile_products_dates: /products_dates.py")
f.write("\nfile_revenue_by_type: /revenue_by_type.py")
f.write("\nfile_week_unfulfilled_orders: /week_unfulfilled_orders.py")
f.write("\nfile_yr_unfulfilled_orders: /yr_unfulfilled_orders.py")
f.write("\nfile_year_to_date_by_day: /year_to_date_by_day.py")

# input files
f.write("\nio_segments: /segments.csv")
f.write("\ninput_orders: /orders_export.csv")
f.write("\ninput_customers: /customers_export.csv")
f.write("\ninput_products: /products_export.csv")
f.write("\ninput_gross_profit_type: /vend-gross_profit-for-type-by-month.csv")
f.write("\ninput_total_revenue_prod: /vend-total_revenue-for-product_variant-by-month.csv")
f.write("\ninput_total_revenue_type: /vend-total_revenue-for-type-by-month.csv")
f.write("\ninput_total_revenue_daily: /vend-total_revenue-sales_summary-by-day.csv")

# output files
f.write("\noutput_all_bought_together: /all_bought_together.csv")
f.write("\noutput_monthly_bought_together: /monthly_bought_together.csv")
f.write("\noutput_cust_retention: /cust_retention.csv")
f.write("\noutput_customer_dates: /customer_dates.csv")
f.write("\nio_day_hour_purchases_1: /day_hour_purchases_1.csv")
f.write("\nio_info_segments_2: /info_segments_2.csv")
f.write("\nio_latest_order_days_3: /latest_order_days_3.csv")
f.write("\nio_order_frequency_customer_4: /order_frequency_customer_4.csv")
f.write("\noutput_customers: /customers.csv")
f.write("\noutput_order_frequency: /order_frequency.csv")
f.write("\noutput_products: /products.csv")
f.write("\noutput_products_dates: /products_dates.csv")
f.write("\noutput_revenue_by_type: /revenue_by_type.csv")
f.write("\noutput_week_unfulfilled_orders: /week_unfulfilled_orders.csv")
f.write("\noutput_yr_unfulfilled_orders: /yr_unfulfilled_orders.csv")
f.write("\noutput_year_to_date_by_day: /year_to_date_by_day.csv")
f.write("\noutput_cust_score: /cust_score.csv")

#log files
f.write("\nlog_all_bought_together: /all_bought_together")
f.write("\nlog_monthly_bought_together: /monthly_bought_together")
f.write("\nlog_cust_retention: /cust_retention")
f.write("\nlog_customer_dates: /customer_dates")
f.write("\nlog_day_hour_purchases_1: /day_hour_purchases_1")
f.write("\nlog_info_segments_2: /info_segments_2")
f.write("\nlog_latest_order_days_3: /latest_order_days_3")
f.write("\nlog_order_frequency_customer_4: /order_frequency_customer_4")
f.write("\nlog_customers: /customers")
f.write("\nlog_order_frequency: /order_frequency")
f.write("\nlog_products: /products")
f.write("\nlog_products_dates: /products_dates")
f.write("\nlog_revenue_by_type: /revenue_by_type")
f.write("\nlog_week_unfulfilled_orders: /week_unfulfilled_orders")
f.write("\nlog_yr_unfulfilled_orders: /yr_unfulfilled_orders")
f.write("\nlog_year_to_date_by_day: /year_to_date_by_day")
f.write("\nlog_cust_score: /cust_score")

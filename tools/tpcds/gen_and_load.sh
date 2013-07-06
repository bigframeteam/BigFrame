unzip -n tpcds_data_gen.zip
perl tpcds_gen_data.pl data.properties
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/call_center*.dat tpcds/call_center
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/catalog_page*.dat tpcds/catalog_page
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/catalog_returns*.dat tpcds/catalog_returns
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/catalog_sales*.dat tpcds/catalog_sales
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/customer_address*.dat tpcds/customer_address
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/customer_demographics*.dat tpcds/customer_demographics
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/customer_[0-9]*.dat tpcds/customer
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/date_dim*.dat tpcds/date_dim
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/dbgen_version*.dat tpcds/dbgen_version
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/household_demographics*.dat tpcds/household_demographics
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/income_band*.dat tpcds/income_band
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/inventory*.dat tpcds/inventory
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/item*.dat tpcds/item
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/promotion*.dat tpcds/promotion
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/reason*.dat tpcds/reason
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/ship_mode*.dat tpcds/ship_mode
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/store_returns*.dat tpcds/store_returns
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/store_sales*.dat tpcds/store_sales
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/store_[0-9]*.dat tpcds/store
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/time_dim*.dat tpcds/time_dim
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/warehouse*.dat tpcds/warehouse
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/web_page*.dat tpcds/web_page
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/web_returns*.dat tpcds/web_returns
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/web_sales*.dat tpcds/web_sales
/home/andy/Programs/hadoop/hadoop-1.0.4/bin/hadoop fs -put data/web_site*.dat tpcds/web_site
rm -rf data/*.dat

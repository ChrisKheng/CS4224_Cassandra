#!/bin/bash
rm -r project_files project_files.zip

wget http://www.comp.nus.edu.sg/~cs4224/project_files.zip

unzip project_files.zip

cqlsh --request-timeout=3600 -f create_table_workload_a.cql
cqlsh --request-timeout=3600 -f create_table_workload_b.cql

for folder in data_files_A data_files_B
do
  # For order_line
  awk -F "," '{if ($6=="null") {$6=""}; OFS=","; print}' project_files/${folder}/order-line.csv > project_files/${folder}/order-line-clean.csv
  
  # For order_by_customer
  awk -F "," '{OFS=","; print $1, $2, $4, $8, $3}' project_files/${folder}/order.csv > project_files/${folder}/order-by-customer.csv
  
  # For order_by_item
  awk -F "," '{OFS=","; print $5, $1, $2, $3}' project_files/${folder}/order-line.csv | uniq > project_files/${folder}/order-by-item.csv
done

cqlsh --request-timeout=3600 -f load_data_workload_a.cql
cqlsh --request-timeout=3600 -f load_data_workload_b.cql

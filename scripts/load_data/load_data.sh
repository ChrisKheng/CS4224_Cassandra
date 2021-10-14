#!/bin/bash
archive_name="project_files_4"

rm -r $archive_name ${archive_name}.zip

wget http://www.comp.nus.edu.sg/~cs4224/${archive_name}.zip

unzip ${archive_name}.zip

cqlsh --request-timeout=3600 -f create_table.cql

# For order_line, cassandra would yell at us if a null value is given for timestamp field
awk -F "," '{if ($6 == "null") {$6 = ""}; OFS = ","; print}' ${archive_name}/data_files/order-line.csv > ${archive_name}/data_files/order-line-clean.csv

# For order_by_customer
awk -F "," '{OFS=","; print $1, $2, $4, $8, $3}' ${archive_name}/data_files/order.csv > ${archive_name}/data_files/order-by-customer.csv

# For order_by_item
awk -F "," '{OFS=","; print $5, $1, $2, $3}' ${archive_name}/data_files/order-line.csv | uniq > ${archive_name}/data_files/order-by-item.csv

# For order table, replace o_carrier_id with -1 if it's null
awk -F "," '{if ($5 == "null") {$5 = "-1"}; OFS=","; print}' ${archive_name}/data_files/order.csv > ${archive_name}/data_files/order-clean.csv

cqlsh --request-timeout=3600 -f load_data.cql

USE wholesale;

COPY warehouse (W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD) FROM '/mnt/c/Users/Hoang/Projects/CS4224/src/data/data_files_A/warehouse.csv' WITH DELIMITER=',';
SELECT * FROM warehouse;
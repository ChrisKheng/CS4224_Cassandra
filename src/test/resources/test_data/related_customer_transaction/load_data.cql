USE wholesale_test;

COPY customer (C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA) FROM 'src/test/resources/test_data/related_customer_transaction/customer.csv' WITH DELIMITER=',';
COPY orders (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) FROM 'src/test/resources/test_data/related_customer_transaction/order.csv' WITH PREPAREDSTATEMENTS = False;
COPY order_by_customer (C_W_ID, C_D_ID, C_ID, O_ENTRY_D, O_ID) FROM 'src/test/resources/test_data/related_customer_transaction/order-by-customer.csv' WITH DELIMITER=',';
COPY order_by_item (I_ID, O_W_ID, O_D_ID, O_ID) FROM 'src/test/resources/test_data/related_customer_transaction/order-by-item.csv' WITH DELIMITER=',';
COPY order_line (OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER,OL_I_ID,OL_DELIVERY_D,OL_AMOUNT,OL_SUPPLY_W_ID,OL_QUANTITY,OL_DIST_INFO) FROM 'src/test/resources/test_data/related_customer_transaction/order-line.csv' WITH PREPAREDSTATEMENTS = False;

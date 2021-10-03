-- Creates the keyspace.
DROP KEYSPACE IF EXISTS wholesale_dev_b;
CREATE KEYSPACE IF NOT EXISTS wholesale_dev_b WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
USE wholesale_dev_b;

-- Creates the tables.
DROP TABLE IF EXISTS warehouse;
CREATE TABLE warehouse (
	W_ID int,
	W_NAME text,
	W_STREET_1 text,
	W_STREET_2 text,
	W_CITY text,
	W_STATE text,
	W_ZIP text,
	W_TAX decimal,
	W_YTD decimal,
	PRIMARY KEY (W_ID)
);

DROP TABLE IF EXISTS district;
CREATE TABLE district (
	D_W_ID int,
	D_ID int,
	D_NAME text,
	D_STREET_1 text,
	D_STREET_2 text,
	D_CITY text,
	D_STATE text,
	D_ZIP text,
	D_TAX decimal,
	D_YTD decimal,
	D_NEXT_O_ID int,
	PRIMARY KEY ((D_W_ID, D_ID))
);

DROP TABLE IF EXISTS customer;
CREATE TABLE customer (
	C_W_ID int,
	C_D_ID int,
	C_ID int,
	C_FIRST text,
	C_MIDDLE text,
	C_LAST text,
	C_STREET_1 text,
	C_STREET_2 text,
	C_CITY text,
	C_STATE text,
	C_ZIP text,
	C_PHONE text,
	C_SINCE timestamp,
	C_CREDIT text,
	C_CREDIT_LIM decimal,
	C_DISCOUNT decimal,
	C_BALANCE decimal,
	C_YTD_PAYMENT float,
	C_PAYMENT_CNT int,
	C_DELIVERY_CNT int,
	C_DATA text,
	PRIMARY KEY ((C_W_ID, C_D_ID), C_ID)
);

-- Note the s
-- By default, clustering key is sorted in ASC order.
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
	O_W_ID int,
	O_D_ID int,
	O_ID int,
	O_C_ID int,
	O_CARRIER_ID int,
	O_OL_CNT decimal,
	O_ALL_LOCAL decimal,
	O_ENTRY_D timestamp,
	PRIMARY KEY ((O_W_ID, O_D_ID), O_ID)
);

DROP TABLE IF EXISTS item;
CREATE TABLE item (
	I_ID int,
	I_NAME text,
	I_PRICE decimal,
	I_IM_ID int,
	I_DATA text,
	PRIMARY KEY (I_ID)
);

DROP TABLE IF EXISTS order_line;
CREATE TABLE order_line (
	OL_W_ID int,
	OL_D_ID int,
	OL_O_ID int,
	OL_NUMBER int,
	OL_I_ID int,
	OL_DELIVERY_D timestamp,
	OL_AMOUNT decimal,
	OL_SUPPLY_W_ID int,
	OL_QUANTITY decimal,
	OL_DIST_INFO text,
	PRIMARY KEY ((OL_W_ID, OL_D_ID), OL_O_ID, OL_NUMBER)
);

DROP TABLE IF EXISTS stock;
CREATE TABLE stock (
	S_W_ID int,
	S_I_ID int,
	S_QUANTITY decimal,
	S_YTD decimal,
	S_ORDER_CNT int,
	S_REMOTE_CNT int,
	S_DIST_01 text,
	S_DIST_02 text,
	S_DIST_03 text,
	S_DIST_04 text,
	S_DIST_05 text,
	S_DIST_06 text,
	S_DIST_07 text,
	S_DIST_08 text,
	S_DIST_09 text,
	S_DIST_10 text,
	S_DATA text,
	PRIMARY KEY (S_W_ID, S_I_ID)
);

DROP TABLE IF EXISTS order_by_customer;
CREATE TABLE order_by_customer (
    C_W_ID int,
    C_D_ID int,
    C_ID int,
    O_ENTRY_D timestamp,
    O_ID int,
    PRIMARY KEY ((C_W_ID, C_D_ID, C_ID), O_ENTRY_D, O_ID))
WITH CLUSTERING ORDER BY (O_ENTRY_D DESC, O_ID DESC);

DROP TABLE IF EXISTS order_by_item;
CREATE TABLE order_by_item (
    I_ID int,
    O_W_ID int,
    O_D_ID int,
    O_ID int,
    PRIMARY KEY ((I_ID), O_W_ID, O_D_ID, O_ID)
);

// Note that MATERIALIZED VIEW is still experimental in Cassandra 4.0
// The order of definition of clustering key matters! In the case below, when C_W_ID is the same, the rows are sorted
// by C_BALANCE, then C_D_ID, and finally C_ID.
DROP MATERIALIZED VIEW IF EXISTS customer_balance;
CREATE MATERIALIZED VIEW customer_balance
AS SELECT C_W_ID, C_D_ID, C_ID, C_BALANCE
FROM customer
WHERE C_W_ID IS NOT NULL AND C_D_ID IS NOT NULL AND C_ID IS NOT NULL AND C_BALANCE IS NOT NULL
PRIMARY KEY ((C_W_ID), C_BALANCE, C_D_ID, C_ID)
WITH CLUSTERING ORDER BY (C_BALANCE DESC, C_D_ID ASC, C_ID ASC);
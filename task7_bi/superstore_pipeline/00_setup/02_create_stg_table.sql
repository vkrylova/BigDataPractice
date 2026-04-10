-- ==========================================
-- Create superstore_dump to load raw data
-- ==========================================
DROP TABLE IF EXISTS stg.superstore_dump;

CREATE TABLE stg.superstore_dump (
	row_id INTEGER,
	order_id VARCHAR(50),
	order_date DATE,
	ship_date DATE,
	ship_mode VARCHAR(20),
	customer_id VARCHAR(20),
	customer_name VARCHAR(100),
	segment VARCHAR(20),
	country VARCHAR(50),
	city VARCHAR(50),
	state VARCHAR(50),
	postal_code VARCHAR(20),
	region VARCHAR(50),
	product_id VARCHAR(30),
	category VARCHAR(50),
	sub_category VARCHAR(50),
	product_name VARCHAR(255),
	sales NUMERIC(10, 4),
	quantity INTEGER,
	discount NUMERIC(4, 2),
	profit NUMERIC(10, 4)
);

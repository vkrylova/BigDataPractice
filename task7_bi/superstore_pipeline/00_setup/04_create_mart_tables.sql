-- ==========================================
-- 1. Customers (SCD Type 2)
-- ==========================================
DROP TABLE IF EXISTS mart.dim_customers CASCADE;

CREATE TABLE mart.dim_customers (
	customer_sk SERIAL PRIMARY KEY, -- true primary key for Power BI
	customer_id VARCHAR(20), -- natural key
	customer_name VARCHAR(100),
	segment VARCHAR(20),
	country VARCHAR(50),
	city VARCHAR(50),
	state VARCHAR(50),
	postal_code VARCHAR(20),
	region VARCHAR(50),
	-- SCD Type 2 Tracking Columns
	is_current BOOLEAN DEFAULT TRUE,
	valid_from DATE DEFAULT CURRENT_DATE,
	valid_to DATE DEFAULT NULL
);

-- ==========================================
-- 2. Products (SCD Type 1 or 2)
-- ==========================================
DROP TABLE IF EXISTS mart.dim_products CASCADE;

CREATE TABLE mart.dim_products (
	product_sk SERIAL PRIMARY KEY,
	product_id VARCHAR(30),
	category VARCHAR(50),
	sub_category VARCHAR(50),
	product_name VARCHAR(255)
);

-- ==========================================
-- 3. Dates
-- ==========================================
DROP TABLE IF EXISTS mart.dim_dates CASCADE;

CREATE TABLE mart.dim_dates (
    date_sk INTEGER PRIMARY KEY, -- e.g., 20240402
    full_date DATE,
    day_name VARCHAR(15),
    day_of_month INTEGER,
    month_name VARCHAR(15),
    month_number INTEGER,
    year INTEGER
);

-- ==========================================
-- 4. Orders
-- ==========================================
DROP TABLE IF EXISTS mart.dim_orders CASCADE;

CREATE TABLE mart.dim_orders (
	order_sk SERIAL PRIMARY KEY,
	order_id VARCHAR(50),
	ship_mode VARCHAR(20)
);

-- ==========================================
-- 5. Sales Fact
-- ==========================================
DROP TABLE IF EXISTS mart.fact_sales CASCADE;

CREATE TABLE mart.fact_sales (
	row_id INTEGER PRIMARY KEY,
	customer_sk INTEGER REFERENCES mart.dim_customers (customer_sk),
	product_sk INTEGER REFERENCES mart.dim_products (product_sk),
	order_sk INTEGER REFERENCES mart.dim_orders (order_sk),
	order_date_sk INTEGER REFERENCES mart.dim_dates (date_sk),
	ship_date_sk INTEGER REFERENCES mart.dim_dates (date_sk),
	sales NUMERIC(10, 4),
	quantity INTEGER,
	discount NUMERIC(4, 2),
	profit NUMERIC(10, 4)
);

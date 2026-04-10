-- ==========================================
-- Load raw data into superstore_dump table
-- ==========================================

COPY stg.superstore_dump
FROM
	'/data/Superstore.csv' DELIMITER ',' CSV HEADER ENCODING 'WIN1252';
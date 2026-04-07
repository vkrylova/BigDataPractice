-- ==========================================
-- Dim Dates
-- Generate date from 2010-01-01 to 2030-12-31
-- ==========================================
INSERT INTO
	mart.dim_dates (
		date_sk,
		full_date,
		day_name,
		day_of_month,
		month_name,
		month_number,
		YEAR
	)
SELECT
	TO_CHAR(datum, 'YYYYMMDD')::INTEGER AS date_sk, --Surrogate Key (e.g., 20240402)
	datum AS full_date, -- standard date
	TRIM(TO_CHAR(datum, 'Day')) AS day_name, -- Extract text names
	EXTRACT(
		DAY
		FROM
			datum
	)::INTEGER AS day_of_month,
	TRIM(TO_CHAR(datum, 'Month')) AS month_name, -- Extract numbers
	EXTRACT(
		MONTH
		FROM
			datum
	)::INTEGER AS month_number,
	EXTRACT(
		YEAR
		FROM
			datum
	)::INTEGER AS YEAR
FROM
	(
		SELECT
			datum::DATE
		FROM
			GENERATE_SERIES(
				'2010-01-01'::DATE,
				'2030-12-31'::DATE,
				'1 day'::INTERVAL
			) AS datum
	) AS date_sequence;
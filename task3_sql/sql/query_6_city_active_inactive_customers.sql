-- Output cities with the number of active and inactive customers (active - customer.active = 1).
-- Sort by the number of inactive customers in descending order.
SELECT
	c.city,
	COUNT(CASE WHEN cst.active = 1 THEN 1 END) AS active_cst_count,
	COUNT(CASE WHEN cst.active = 0 THEN 1 END) AS inactive_cst_count
FROM
	customer cst
	LEFT JOIN address USING (address_id)
	LEFT JOIN city c USING (city_id)
GROUP BY
	c.city
ORDER BY
	inactive_cst_count DESC
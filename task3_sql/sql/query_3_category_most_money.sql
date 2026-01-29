-- Output the category of movies on which the most money was spent.
SELECT
	c.name,
	SUM(p.amount) AS total_payment
FROM
	category c
	JOIN film_category USING (category_id)
	JOIN inventory USING (film_id)
	JOIN rental USING (inventory_id)
	JOIN payment p USING (rental_id)
GROUP BY
	c.category_id,
	c.name
ORDER BY
	total_payment DESC
LIMIT
	1
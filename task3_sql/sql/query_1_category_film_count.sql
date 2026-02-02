-- Output the number of movies in each category, sorted descending.
SELECT
	c.name,
	COUNT(DISTINCT fc.film_id) AS film_count
FROM
	category c
	LEFT JOIN film_category fc USING (category_id)
GROUP BY
	c.name
ORDER BY
	film_count DESC
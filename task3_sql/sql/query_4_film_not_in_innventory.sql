-- Print the names of movies that are not in the inventory.
-- Write a query without using the IN operator.
SELECT
	f.title AS
FROM
	film f
WHERE
	NOT EXISTS (
		SELECT
			1
		FROM
			inventory i
		WHERE
			f.film_id = i.film_id
	)
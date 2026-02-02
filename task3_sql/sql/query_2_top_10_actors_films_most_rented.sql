-- Output the 10 actors whose movies rented the most, sorted in descending order.
SELECT
	a.first_name,
	a.last_name,
	COUNT(*) AS rental_count
FROM
	actor a
	JOIN film_actor fa USING (actor_id)
	JOIN inventory i USING (film_id)
	JOIN rental r USING (inventory_id)
GROUP BY
	a.actor_id,
	a.first_name,
	a.last_name
ORDER BY
	rental_count DESC
FETCH FIRST 10 ROWS WITH TIES;
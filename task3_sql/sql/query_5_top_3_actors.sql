-- Output the top 3 actors who have appeared the most in movies in the “Children” category.
-- If several actors have the same number of movies, output all of them.
WITH
	actors_in_chld_cat AS (
		SELECT
			a.first_name,
			a.last_name,
			COUNT(*) AS chld_cat_count,
			DENSE_RANK() OVER (
				ORDER BY
					COUNT(*) DESC
			) AS rnk
		FROM
			actor a
			JOIN film_actor fa USING (actor_id)
			JOIN film_category fc USING (film_id)
			JOIN category c USING (category_id)
		WHERE
			c.name = 'Children'
		GROUP BY
			a.actor_id,
			a.first_name,
			a.last_name
	)
SELECT
	first_name,
	last_name,
	chld_cat_count
FROM
	actors_in_chld_cat
WHERE
	rnk <= 3;
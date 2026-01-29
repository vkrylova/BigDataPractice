-- Output the top 3 actors who have appeared the most in movies in the “Children” category.
-- If several actors have the same number of movies, output all of them.
WITH
	actors_in_chld_cat AS (
		SELECT
			a.first_name,
			a.last_name,
			COUNT(c.name) AS chld_cat_count
		FROM
			actor a
			INNER JOIN film_actor fa USING (actor_id)
			INNER JOIN film f USING (film_id)
			INNER JOIN film_category fc USING (film_id)
			INNER JOIN category c USING (category_id)
		WHERE
			c.name = 'Children'
		GROUP BY
			a.actor_id,
			a.first_name,
			a.last_name
	),
	top_3_count AS (
		SELECT
			chld_cat_count
		FROM
			actors_in_chld_cat
		ORDER BY
			chld_cat_count DESC
		LIMIT
			3
	)
SELECT
	first_name,
	last_name,
	chld_cat_count
FROM
	actors_in_chld_cat
WHERE
	chld_cat_count >= (
		SELECT
			MIN(t.chld_cat_count)
		FROM
			top_3_count t
	)
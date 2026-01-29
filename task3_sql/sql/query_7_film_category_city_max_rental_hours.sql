-- Output the category of movies that have the highest number
-- of total rental hours in the city (customer.address_id in this city)
-- and that start with the letter “a”.
-- Do the same for cities that have a “-” in them.
-- Write everything in one query.
-- NOTE TO MYSELF: read bot to top
SELECT
    t.name as category,
    t.city as city,
    ROUND(t.rent_hours, 2) AS max_rent_hours_city
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY city
               ORDER BY rent_hours DESC) AS rn
    FROM (
        SELECT
            ctg.name,
            c.city,
            SUM(EXTRACT(EPOCH FROM (r.return_date - r.rental_date)) / 3600) AS rent_hours
        FROM category ctg
        JOIN film_category USING (category_id)
        JOIN film USING (film_id)
        JOIN inventory USING (film_id)
        JOIN rental r USING (inventory_id)
        JOIN customer USING (customer_id)
        JOIN address USING (address_id)
        JOIN city c USING (city_id)
        WHERE ctg.name LIKE 'A%' AND c.city LIKE '%-%'
        GROUP BY ctg.name, c.city
    ) s
) t
WHERE rn = 1;

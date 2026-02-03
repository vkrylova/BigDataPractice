-- Q4 -> NOT EXISTS; Q2, Q3, Q7 -> JOINS
CREATE INDEX IF NOT EXISTS idx_inventory_film_id ON inventory(film_id);

-- Q1, 3, 7 -> JOINS
CREATE INDEX IF NOT EXISTS idx_film_category_category_id ON film_category(category_id);

-- Q3 -> sum amount
CREATE INDEX IF NOT EXISTS idx_payment_rental_id ON payment(rental_id);
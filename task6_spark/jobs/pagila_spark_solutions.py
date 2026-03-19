from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc, col, sum, dense_rank
from pyspark.sql.functions import when, unix_timestamp, round
from config.config import JDBC_URL, DB_PROPS, SPARK_MASTER_URL
from pyspark.sql.window import Window

# import time

# Session initialization
spark = (
    SparkSession.builder
    .appName("Pagila Job")
    .master(SPARK_MASTER_URL)
    .config("spark.driver.host", "spark-job")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .getOrCreate()
)

# Set log level
spark.sparkContext.setLogLevel("ERROR")

print("Reading tables from DB")

tables = ["category", "film_category", "actor", "film_actor", "inventory",
          "rental", "payment", "film", "customer", "address", "city"]

dfs = {}

for table in tables:
    dfs[table] = spark.read.jdbc(JDBC_URL, f"public.{table}", properties=DB_PROPS)
    # Cache tables for multiple use (rental - 3, others - 4)
    if table in ["rental", "film_category", "category", "inventory"]:
        dfs[table].cache()

# 1 Output the number of movies in each category, sorted descending.
category_film_count = (
    dfs["category"].alias("c")
    .join(dfs["film_category"].alias("fc"), "category_id", "left")
    .groupBy(col("c.name"))
    .agg(count(col("fc.film_id")).alias("film_count"))
    .orderBy(desc("film_count"))
)
print("#1. Number of movies in each category")
category_film_count.show()

# 2 Output the 10 actors whose movies rented the most, sorted in descending order.
top_10_actors_rented_films = (
    dfs["actor"].alias("a")
    .join(dfs["film_actor"], "actor_id")
    .join(dfs["inventory"], "film_id")
    .join(dfs["rental"].alias("r"), "inventory_id")
    .groupBy(col("a.actor_id"), col("a.first_name"), col("a.last_name"))
    .agg(count(col("r.rental_id")).alias("rental_count"))
    .orderBy(desc("rental_count"))
    .limit(10)
)

print("#2. The 10 actors whose movies rented the most")
top_10_actors_rented_films.show()

# 3 Output the category of movies on which the most money was spent.
category_most_money = (
    dfs["category"].alias("c")
    .join(dfs["film_category"], "category_id")
    .join(dfs["inventory"], "film_id")
    .join(dfs["rental"], "inventory_id")
    .join(dfs["payment"].alias("p"), "rental_id")
    .groupBy(col("c.category_id"), col("c.name"))
    .agg(sum(col("p.amount")).alias("total_payment"))
    .orderBy(desc("total_payment"))
    .limit(1)
)

print("#3. Category of movies that the most money was spent")
category_most_money.show()

# 4 Print the names of movies that are not in the inventory.
movies_not_in_inventory = (
    dfs["film"].alias("f")
    .join(dfs["inventory"], "film_id", "left_anti")
    .select(col("f.title"))
)

print("#4. Movies that are not in the inventory")
movies_not_in_inventory.show(50, truncate=False)

# 5 Output the top 3 actors who have appeared the most
# in movies in the “Children” category.
# If several actors have the same number of movies, output all of them.

actors_chld_category = (
    dfs["actor"].alias("a")
    .join(dfs["film_actor"], "actor_id")
    .join(dfs["film_category"], "film_id")
    .join(dfs["category"].alias("c"), "category_id")
    .filter(col("c.name") == "Children")
    .groupBy(col("a.first_name"), col("a.last_name"))
    .agg(count(col("a.actor_id")).alias("movie_count"))
)

window_spec_actors_category = (
    Window.orderBy(desc("movie_count"))
)

top_3_actors_chld_category = (
    actors_chld_category
    .withColumn("rank", dense_rank().over(window_spec_actors_category))
    .filter(col("rank") <= 3)
    .drop("rank")
)
print("#5. Top 3 actors in 'Children' category")
top_3_actors_chld_category.show(30)

# 6 Output cities with the number of active and inactive customers
# (active - customer.active = 1).
# Sort by the number of inactive customers in descending order.

active_inactive_cst_count = (
    dfs["customer"].alias("cst")
    .join(dfs["address"], "address_id", "left")
    .join(dfs["city"].alias("c"), "city_id", "left")
    .groupBy(col("c.city"))
    .agg(
        sum(when(col("cst.active") == 1, 1).otherwise(0)).alias("active_cst_count"),
        sum(when(col("cst.active") == 0, 1).otherwise(0)).alias("inactive_cst_count")
    )
    .orderBy(desc("inactive_cst_count"))
)
print("#6. Cities with the number of active and inactive customers")
active_inactive_cst_count.show()

# 7 Output the category of movies that have the highest number
# of total rental hours in the city (customer.address_id in this city)
# and that start with the letter “a”.
# Do the same for cities that have a “-” in them.

ctg_city_rental_hours = (
    dfs["category"].alias("ctg")
    .join(dfs["film_category"], "category_id")
    .join(dfs["inventory"], "film_id")
    .join(dfs["rental"].alias("r"), "inventory_id")
    .join(dfs["customer"], "customer_id")
    .join(dfs["address"], "address_id")
    .join(dfs["city"].alias("c"), "city_id")
    .filter((col("c.city").ilike("a%")) | (col("c.city")).like("%-%"))
    .groupBy(col("ctg.name"), col("c.city"))
    .agg(
        round(
            sum(
                (unix_timestamp(col("r.return_date"))
                 - unix_timestamp(col("r.rental_date"))) / 3600), 2
        ).alias("rent_hours")
    )
)

window_spec_city_rental_hours = (
    Window.partitionBy(col("c.city"))
    .orderBy(desc("rent_hours"))
)

ctg_city_max_rental_hours = (
    ctg_city_rental_hours
    .withColumn("rank", dense_rank().over(window_spec_city_rental_hours))
    .filter(col("rank") == 1)
    .orderBy("ctg.name")
    .drop("rank")
)

print("#7. Film category and city with max rental hours")
ctg_city_max_rental_hours.show(truncate=False)

print("All is done!")

# time.sleep(120)

spark.stop()

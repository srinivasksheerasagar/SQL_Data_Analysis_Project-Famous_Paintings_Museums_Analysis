# Delete duplicate records from works, product_price, subject and image_link tables
WITH cte AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION by painting_name, artist_id, style, museum_id ORDER BY work_id) AS row_num
    FROM works
)
DELETE FROM works
WHERE work_id IN (
    SELECT work_id FROM cte WHERE row_num > 1
);

WITH cte AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION by size_id, sale_price, regular_price ORDER BY work_id) AS row_num
    FROM product_price
)
DELETE FROM product_price
WHERE work_id IN (
    SELECT work_id FROM cte WHERE row_num > 1
);

WITH cte AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION by subject ORDER BY work_id) AS row_num
    FROM subject
)
DELETE FROM subject
WHERE work_id IN (
    SELECT work_id FROM cte WHERE row_num > 1
);

WITH cte AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION by url,thumbnail_small_url,thumbnail_large_url ORDER BY work_id) AS row_num
    FROM image_link
)
DELETE FROM image_link
WHERE work_id IN (
    SELECT work_id FROM cte WHERE row_num > 1
);

# Fetch all the paintings which are not displayed on any museums?
SELECT 
    painting_name, museum_id
FROM
    works
WHERE
    museum_id IS NULL;
 
 
# Are there museums without any paintings?
SELECT 
    work_id, museum_id
FROM
    works
WHERE
    work_id IS NULL;


# How many paintings have an asking price of less than their regular price?
SELECT 
    COUNT(DISTINCT work_id) AS total_paintings_on_discount
FROM
    product_price
WHERE
    sale_price < regular_price;
    

# Identify the paintings whose asking price is less than 50% of its regular price
SELECT 
    *
FROM
    product_price
WHERE
    sale_price < regular_price * 0.5;
    

# Which canva size costs the most?
SELECT 
    size_id
FROM
    product_price
WHERE
    sale_price = (SELECT 
            MAX(sale_price) AS max_price
        FROM
            product_price);


# Identify the museums with invalid city information in the given dataset
SELECT 
    *
FROM
    museum
WHERE
    city IS NULL OR city = ''
        OR city REGEXP '[0-9@#$%^&*()_+=]';


# Museum_Hours table has some invalid entries. Identify it and remove it.
CREATE TEMPORARY TABLE temp_invalid AS 
SELECT museum_id, day 
FROM museum_hours
WHERE day NOT IN ('Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday');

DELETE FROM museum_hours 
WHERE
    (museum_id , day) IN (SELECT 
        museum_id, day
    FROM
        temp_invalid);
        













    

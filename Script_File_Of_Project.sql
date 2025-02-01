# Delete duplicate records from works, subject, product_price and image_link tables
WITH cte AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION by work_id, painting_name, artist_id, style, museum_id ORDER BY work_id) AS row_num
    FROM works
)
DELETE FROM works
WHERE work_id IN (
    SELECT work_id FROM cte WHERE row_num > 1
);

WITH cte AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION by work_id, subject ORDER BY work_id) AS row_num
    FROM subject
)
DELETE FROM subject
WHERE work_id IN (
    SELECT work_id FROM cte WHERE row_num > 1
);

WITH cte AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION by work_id, size_id, sale_price, regular_price ORDER BY work_id) AS row_num
    FROM product_price
)
DELETE FROM product_price
WHERE work_id IN (
    SELECT work_id FROM cte WHERE row_num > 1
);

WITH cte AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION by work_id, url, thumbnail_small_url, thumbnail_large_url ORDER BY work_id) AS row_num
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
        
# Fetch the top 10 most famous painting subject? ( top work_id's and top museum_id's)
SELECT 
    subject, COUNT(work_id) AS paintings_count
FROM
    subject
GROUP BY subject
ORDER BY paintings_count DESC
LIMIT 10;


#Identify the museums which are open on both Sunday and Monday. Display museum name, city
SELECT 
    a.museum_id, m.name, m.city
FROM
    (SELECT 
        *
    FROM
        museum_hours
    WHERE
        day = 'sunday') AS a
        JOIN
    (SELECT 
        *
    FROM
        museum_hours
    WHERE
        day = 'monday') AS b ON a.museum_id = b.museum_id
        JOIN
    museum m ON m.museum_id = a.museum_id;
    

# How many museums are open every single day?
SELECT 
    COUNT(*)
FROM
    (SELECT 
        museum_id
    FROM
        museum_hours
    GROUP BY museum_id
    HAVING COUNT(DISTINCT day) = 7) AS a;
    

#Which are the top 5 most popular museums? (Popularity is defined based on most no of paintings in a museum)
SELECT 
    *
FROM
    (SELECT 
        museum_id, COUNT(work_id) AS no_of_paintings
    FROM
        works
    GROUP BY museum_id
    ORDER BY no_of_paintings DESC) AS a
WHERE
    a.museum_id IS NOT NULL
LIMIT 5;


# Who are the top 5 most popular artist? (Popularity is defined based on most no of paintings in museum done by an artist)
SELECT 
    a.artist_id,
    b.full_name AS artist_name,
    a.no_of_paintings_in_museum
FROM
    (SELECT 
        artist_id, COUNT(work_id) AS no_of_paintings_in_museum
    FROM
        works
    WHERE
        museum_id IS NOT NULL
    GROUP BY artist_id) AS a
        JOIN
    (SELECT 
        artist_id, full_name
    FROM
        artist) AS b ON a.artist_id = b.artist_id
ORDER BY a.no_of_paintings_in_museum DESC
LIMIT 10;


# Display the 3 most popular canva sizes
SELECT 
    b.size_id, COUNT(b.work_id) AS no_of_paintings
FROM
    ((SELECT 
        *
    FROM
        works) AS a
    JOIN (SELECT 
        *
    FROM
        product_price) AS b ON a.work_id = b.work_id)
GROUP BY b.size_id
ORDER BY no_of_paintings DESC
LIMIT 3;


# Which museum is open for the longest during a day. Dispay museum name, state and hours open and which day?
SELECT 
    a.name, a.state, b.open, b.close, b.active_hours, b.day
FROM
    (SELECT 
        museum_id, name, state
    FROM
        museum) AS a
        JOIN
    (SELECT 
        museum_id,
            open,
            close,
            TIMEDIFF(STR_TO_DATE(close, '%h:%i:%p'), STR_TO_DATE(open, '%h:%i:%p')) AS active_hours,
            day
    FROM
        museum_hours) AS b ON a.museum_id = b.museum_id
ORDER BY b.active_hours DESC
LIMIT 1;


# Which museum has the most no of most popular painting style ( the most repeated style of paintings in museum by artists )
SELECT 
    a.museum_id, b.name, a.style, a.no_of_paintings
FROM
    (SELECT 
        museum_id, style, COUNT(work_id) AS no_of_paintings
    FROM
        works
    WHERE
        museum_id IS NOT NULL
    GROUP BY museum_id , style
    ORDER BY no_of_paintings DESC
    LIMIT 1) AS a
        JOIN
    (SELECT 
        *
    FROM
        museum) AS b ON a.museum_id = b.museum_id;


# Identify the artists whose paintings are displayed in multiple countries
SELECT 
    b.artist_id,
    c.full_name,
    GROUP_CONCAT(DISTINCT a.museum_id
        SEPARATOR ', ') AS museum_ids,
    GROUP_CONCAT(DISTINCT a.country
        SEPARATOR ', ') AS displayed_countries
FROM
    (SELECT 
        museum_id, country
    FROM
        museum) AS a
        JOIN
    (SELECT 
        museum_id, work_id, artist_id
    FROM
        works) AS b ON a.museum_id = b.museum_id
        JOIN
    (SELECT 
        artist_id, full_name
    FROM
        artist) AS c ON c.artist_id = b.artist_id
GROUP BY b.artist_id , c.full_name
HAVING COUNT(DISTINCT a.country) > 1
;


# Which country has the 5th highest no of paintings?
SELECT 
    country, COUNT(work_id) AS total_paintings
FROM
    (SELECT 
        museum_id, country
    FROM
        museum) AS a
        JOIN
    (SELECT 
        museum_id AS museum_id_works, work_id
    FROM
        works) AS b ON a.museum_id = b.museum_id_works
GROUP BY country
ORDER BY total_paintings DESC
LIMIT 1 OFFSET 4;


/* Display the country and the city with most no of museums. Output 2 seperate 
columns to mention the city and country. If there are multiple value, seperate them
with comma */
with country_info as
(select country, count(museum_id) as no_of_museums_in_country, dense_rank() over(order by count(museum_id) desc) as row_num_1
from museum
group by country
order by no_of_museums_in_country desc
),
cities_info as
(select city, count(museum_id) as no_of_museums_in_city, dense_rank() over(order by count(museum_id) desc) as row_num_2
from museum
group by city
order by no_of_museums_in_city desc
)
select group_concat(distinct a.country separator ", ") as countries_list, 
	   group_concat(distinct b.city separator ", ") as cities_list
from country_info as a, cities_info as b
where a.row_num_1 = 1 and b.row_num_2 = 1
;


/* Which are the 3 most popular and 3 least popular painting styles? */
with cte_1 as (
# most popular painting styles:
select style, count(artist_id) as no_of_artists_used, dense_rank() over (order by count(artist_id) desc) as high_ranks
from works
where museum_id is not null and style is not null
group by style), 

cte_2 as (
# least ones:
select style, count(artist_id) as no_of_artists_used, dense_rank() over (order by count(artist_id) asc) as low_ranks
from works
where museum_id is not null and style is not null
group by style)

select group_concat(distinct c1.style separator ", ") as top_3_most_popular_styles, group_concat(distinct c2.style separator ", ") as top_3_least_popular_styles
from cte_1 as c1, cte_2 as c2
where c1.high_ranks <=3 and c2.low_ranks <=3
;

















    

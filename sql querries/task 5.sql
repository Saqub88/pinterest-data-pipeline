/*
Find how many posts each category had between 2018 and 2022.
Your query should return a DataFrame that contains the following columns:
	post_year, a new column that contains only the year from the timestamp column
	category
	category_count, a new column containing the desired query output
*/

SELECT strftime('%Y', g.timestamp) AS post_year, p.category, count(*) AS category_count
FROM pin_df AS p
JOIN geo_df AS g
ON p.ind = g.ind
GROUP BY post_year, p.category
HAVING post_year BETWEEN '2018' AND '2022'
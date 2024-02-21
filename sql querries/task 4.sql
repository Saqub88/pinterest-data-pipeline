/*
Find the most popular Pinterest category people post to based on their country.
Your query should return a DataFrame that contains the following columns:
	country
	category
	category_count, a new column containing the desired query output
*/

SELECT country, category, max(tally) AS category_count
FROM(SELECT g.country, p.category, count(*) AS tally
		FROM geo_df AS g
		JOIN pin_df AS p
		ON g.ind = p.ind
		GROUP BY g.country, p.category)
GROUP BY country
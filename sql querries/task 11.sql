/*
Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of.
Your query should return a DataFrame that contains the following columns:
	age_group, a new column based on the original age column
	post_year, a new column that contains only the year from the timestamp column
	median_follower_count, a new column containing the desired query output
*/

WITH base AS (
SELECT *, row_number() OVER (ORDER BY post_year, age_group) as row_id
FROM (SELECT u.ind, strftime('%Y', timestamp) AS post_year, follower_count,
		CASE 
		WHEN u.age BETWEEN 18 AND 24 THEN '18-24'
		WHEN u.age BETWEEN 25 AND 35 THEN '25-35'
		WHEN u.age BETWEEN 36 AND 50 THEN '36-50'
		WHEN u.age > 50 THEN '50+'
		END AS age_group
	FROM user_df AS u
	JOIN geo_df AS g ON u.ind = g.ind
	JOIN pin_df as p ON u.ind = p.ind
	WHERE CAST(post_year AS INTEGER) BETWEEN 2015 AND 2020
	ORDER BY post_year, follower_count
	))

SELECT age_group, post_year, ((SELECT follower_count FROM base WHERE row_id = low_pos)+(SELECT follower_count FROM base WHERE row_id = high_pos))/2 AS median_follower_count
FROM (SELECT *, MIN(row_id), MAX(row_id),
		((MIN(row_id)+MAX(row_id))/2) AS low_pos,
		CASE
		WHEN (MAX(row_id)-MIN(row_id))%2 = 0 THEN (MAX(row_id)+MIN(row_id))/2
		WHEN (MAX(row_id)-MIN(row_id))%2 = 1 THEN ((MIN(row_id)+MAX(row_id))/2)+1
		END AS high_pos
	FROM base
	GROUP BY post_year, age_group)
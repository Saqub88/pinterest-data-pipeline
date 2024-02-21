/*
Step 2: Based on the above query, find the country with the user with most followers.
Your query should return a DataFrame that contains the following columns:
	country
	follower_count
	This DataFrame should have only one entry. */

SELECT country, MAX(follower_count) AS follower_count
FROM (SELECT g.country, p.poster_name, MAX(p.follower_count) AS follower_count
		FROM pin_df AS p
		JOIN geo_df AS g ON p.ind = g.ind
		JOIN user_df AS u ON p.ind = u.ind
		GROUP BY g.country)
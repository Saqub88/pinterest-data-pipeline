/*
Step 1: For each country find the user with the most followers.
Your query should return a DataFrame that contains the following columns:
	country
	poster_name
	follower_count
*/

SELECT g.country, p.poster_name, MAX(p.follower_count) AS follower_count
FROM pin_df AS p
JOIN geo_df AS g ON p.ind = g.ind
JOIN user_df AS u ON p.ind = u.ind
GROUP BY g.country
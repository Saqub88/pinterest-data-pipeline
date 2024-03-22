/*
Find the median follower count of users have joined between 2015 and 2020.
Your query should return a DataFrame that contains the following columns:
	post_year, a new column that contains only the year from the timestamp column
	median_follower_count, a new column containing the desired query output
*/

WITH base AS (SELECT strftime('%Y', u.date_joined) AS post_date, p.follower_count, row_number() OVER (ORDER BY strftime('%Y', u.date_joined), follower_count) AS row_id
	FROM user_df AS u
	JOIN pin_df AS p on u.ind = p.ind
	GROUP BY u.user_name
	ORDER BY post_date, follower_count)

SELECT post_date, (SELECT follower_count FROM base WHERE row_id = median_row)
FROM (SELECT *, (min(row_id)+max(row_id))/2 AS median_row
	FROM base
	GROUP BY post_date)
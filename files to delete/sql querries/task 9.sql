/*
Find how many users have joined between 2015 and 2020.
Your query should return a DataFrame that contains the following columns:
	post_year, a new column that contains only the year from the timestamp column
	number_users_joined, a new column containing the desired query output
*/

SELECT strftime('%Y', date_joined) AS post_year, count(*) AS number_users_joined
FROM (SELECT * FROM user_df GROUP BY user_name)
GROUP BY post_year
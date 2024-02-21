/*
What is the most popular category people post to based on the following age groups:
18-24
25-35
36-50
+50
Your query should return a DataFrame that contains the following columns:
	age_group, a new column based on the original age column
	category
	category_count, a new column containing the desired query output
*/

SELECT age_group, category,  MAX(tally) AS category_count
FROM (SELECT u.user_name, p.category,
		CASE
		WHEN u.age BETWEEN 18 AND 24 THEN '18-24'
		WHEN u.age BETWEEN 25 AND 35 THEN '25-35'
		WHEN u.age BETWEEN 36 AND 50 THEN '36-50'
		WHEN u.age > 50 THEN '50+'
		END AS age_group,
		count(*) AS tally
	FROM user_df AS u
	JOIN pin_df AS p ON u.ind = p.ind
	GROUP BY p.category, age_group)
GROUP BY age_group
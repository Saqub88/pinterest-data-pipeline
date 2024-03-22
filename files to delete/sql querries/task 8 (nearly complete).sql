/*
What is the median follower count for users in the following age groups:
	18-24, 25-35, 36-50, +50
Your query should return a DataFrame that contains the following columns:
	age_group, a new column based on the original age column
	median_follower_count, a new column containing the desired query output
*/

WITH ordered_table AS
(SELECT *, row_number() OVER (ORDER BY age_group, follower_count ASC) AS row_id
	FROM (SELECT p.follower_count, 
			CASE 
			WHEN u.age BETWEEN 18 AND 24 THEN '18-24'
			WHEN u.age BETWEEN 25 AND 35 THEN '25-35'
			WHEN u.age BETWEEN 36 AND 50 THEN '36-50'
			ELSE '50+'
				END AS age_group
		FROM user_df AS u
		JOIN pin_df AS p ON u.ind = p.ind
		group by user_name))

SELECT *, (SELECT follower_count FROM ordered_table WHERE row_id = low_pos), (SELECT follower_count FROM ordered_table WHERE row_id = high_pos)
FROM (SELECT *,
		CASE
		WHEN (max(row_id)-min(row_id))%2 = 1 THEN ((max(row_id)+min(row_id))/2)
		WHEN (max(row_id)-min(row_id))%2 = 0 THEN ((max(row_id)+min(row_id))/2)
		END AS low_pos,
		CASE
		WHEN (max(row_id)-min(row_id))%2 = 1 THEN ((max(row_id)+min(row_id))/2)
		WHEN (max(row_id)-min(row_id))%2 = 0 THEN ((max(row_id)+min(row_id))/2)+1
		END AS high_pos
	FROM ordered_table
	GROUP BY age_group)
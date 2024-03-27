# Databricks notebook source
pin_df = sqlContext.sql("SELECT * FROM 0aa58e5ad07d_pin_df_1")
geo_df = sqlContext.sql("SELECT * FROM 0aa58e5ad07d_geo_df")
user_df = sqlContext.sql("SELECT * FROM 0aa58e5ad07d_user_df")

# COMMAND ----------

'''
Find the most popular Pinterest category people post to based on their country.
Your query should return a DataFrame that contains the following columns:
	country
	category
	category_count, a new column containing the desired query output
'''

geo_pin_join = geo_df.join(pin_df, geo_df['ind'] == pin_df['ind'])
geo_pin_groupby_country_category = geo_pin_join.groupBy("country", "category").count()
most_popular_category_by_country = geo_pin_groupby_country_category.groupBy("country").agg({"count":"max"})
rename_to_avoid_duplicate = most_popular_category_by_country.withColumnRenamed("country", "country_2")			# further join required. name changed to mitiagate issue with duplicate columns
cleaned_table_4 = rename_to_avoid_duplicate.join(
    geo_pin_groupby_country_category, [rename_to_avoid_duplicate["country_2"] == geo_pin_groupby_country_category["country"],
	rename_to_avoid_duplicate["max(count)"] == geo_pin_groupby_country_category["count"]] , how='inner') \
    .select("country", "category", "max(count)") \
    .withColumnRenamed("max(count)", "category_count")
display(cleaned_table_4)

# COMMAND ----------

'''
Find how many posts each category had between 2018 and 2022.
Your query should return a DataFrame that contains the following columns:
	post_year, a new column that contains only the year from the timestamp column
	category
	category_count, a new column containing the desired query output
'''
from pyspark.sql.functions import year

geo_pin_join = geo_df.join(pin_df, geo_df['ind'] == pin_df['ind'])
timestamp_to_years = geo_pin_join.withColumn('post_year', year('timestamp'))
geo_pin_groupby_year_category = timestamp_to_years.groupBy('post_year', 'category').count()
year_list_2018_to_2022 = ['2018', '2019', '2020', '2021', '2022']
filter_year = geo_pin_groupby_year_category.filter(geo_pin_groupby_year_category['post_year'].isin(year_list_2018_to_2022))
cleaned_table_5 = filter_year.withColumnRenamed('count()', 'category_count')

display(cleaned_table_5)

# COMMAND ----------

'''
Step 1: For each country find the user with the most followers.
Your query should return a DataFrame that contains the following columns:
	country
	poster_name
	follower_count
'''

pin_geo_user_join = pin_df.join(geo_df, pin_df['ind'] == geo_df['ind']) \
    .join(user_df, pin_df['ind'] == user_df['ind'])
renamed_column = pin_geo_user_join.withColumnRenamed('country', 'country_2')
pingeouser_groupby_country = renamed_column.groupBy('country_2').agg({'follower_count':'max'})
rejoin_poster_column = pingeouser_groupby_country.join(pin_geo_user_join, pingeouser_groupby_country['max(follower_count)'] == pin_geo_user_join['follower_count'], how='inner')
cleaned_table_6_part1 = rejoin_poster_column.orderBy(rejoin_poster_column['country_2'], ascending=True) \
    .select('country_2', 'poster_name', 'follower_count').distinct() \
    .withColumnRenamed('country_2', 'country')

display(cleaned_table_6_part1)

# COMMAND ----------

'''
Step 2: Based on the above query, find the country with the user with most followers.
Your query should return a DataFrame that contains the following columns:
	country
	follower_count
	This DataFrame should have only one entry.
'''

cleaned_table_6_part2 = cleaned_table_6_part1.orderBy(cleaned_table_6_part1['follower_count'], ascending=False) \
    .limit(1) \
    .select('country', 'follower_count')

display(country_with_user_with_highest_followers)

# COMMAND ----------

'''
What is the most popular category people post to based on the following age groups:
18-24, 25-35, 36-50, +50
Your query should return a DataFrame that contains the following columns:
	age_group, a new column based on the original age column
	category
	category_count, a new column containing the desired query output
'''

from pyspark.sql.functions import when

pin_user_join = pin_df.join(user_df, pin_df['ind'] == user_df['ind'])
age_grouping = pin_user_join.withColumn('age_group', when((pin_user_join['age'] >= 18) & (pin_user_join['age'] < 25), '18-24') \
    .when((pin_user_join['age'] >= 25) & (pin_user_join['age'] < 36), '25-35') \
    .when((pin_user_join['age'] >= 36) & (pin_user_join['age'] < 51), '36-50') \
    .otherwise('50+'))
groupby_category_age = age_grouping.groupBy('category', 'age_group').count()
groupby_category = groupby_category_age.groupBy('age_group').agg({'count':'max'}) \
    .withColumnRenamed('age_group', 'age_group_renamed')
cleaned_table_7 = groupby_category.join(groupby_category_age, [groupby_category['age_group_renamed'] == groupby_category_age['age_group'],
    groupby_category['max(count)'] == groupby_category_age['count']], how='inner') \
    .select('age_group', 'category', 'count') \
    .withColumnRenamed('count', 'category_count') \
    .orderBy('age_group')

display(cleaned_table_7)

# COMMAND ----------

'''
What is the median follower count for users in the following age groups:
	18-24, 25-35, 36-50, +50
Your query should return a DataFrame that contains the following columns:
	age_group, a new column based on the original age column
	median_follower_count, a new column containing the desired query output
 '''

from pyspark.sql.functions import percentile_approx

pin_user_join = pin_df.join(user_df, pin_df['ind'] == user_df['ind'])
age_grouping = pin_user_join.withColumn('age_group', when((pin_user_join['age'] >= 18) & (pin_user_join['age'] < 25), '18-24') \
    .when((pin_user_join['age'] >= 25) & (pin_user_join['age'] < 36), '25-35') \
    .when((pin_user_join['age'] >= 36) & (pin_user_join['age'] < 51), '36-50') \
    .otherwise('50+'))
cleaned_table_8 = age_grouping.groupby('age_group').agg(percentile_approx('follower_count', 0.5).alias('median_follower_count'))

display(cleaned_table_8)

# COMMAND ----------

'''
Find how many users have joined between 2015 and 2020.
Your query should return a DataFrame that contains the following columns:
	post_year, a new column that contains only the year from the timestamp column
	number_users_joined, a new column containing the desired query output
'''

from pyspark.sql.functions import year

ammend_date_joined = user_df.withColumn('post_year', year('date_joined')) \
    .drop('ind') \
    .distinct()
groupby_year = ammend_date_joined.groupby('post_year').count() \
    .withColumnRenamed('count', 'number_users_joined')
year_list_2015_to_2020 = ['2015', '2016', '2017', '2018', '2019', '2020']
cleaned_table_9 = groupby_year.filter(groupby_year['post_year'].isin(year_list_2015_to_2020))

display(cleaned_table_9)

# COMMAND ----------

'''
Find the median follower count of users have joined between 2015 and 2020.
Your query should return a DataFrame that contains the following columns:
	post_year, a new column that contains only the year from the timestamp column
	median_follower_count, a new column containing the desired query output
'''

from pyspark.sql.functions import percentile_approx

pin_user_join = pin_df.join(user_df, pin_df['ind'] == user_df['ind'])
post_year_column = pin_user_join.withColumn('post_year', year('date_joined'))
drop_duplicates_name_year = post_year_column.dropDuplicates(['user_name', 'post_year'])
year_list_2015_to_2020 = ['2015', '2016', '2017', '2018', '2019', '2020']
groupby_table = drop_duplicates_name_year.groupby('post_year').agg(percentile_approx('follower_count', 0.5).alias('median_follower_count'))
cleaned_table_10 = groupby_table.filter(groupby_table['post_year'].isin(year_list_2015_to_2020))

display(cleaned_table_10)

# COMMAND ----------

'''
Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of.
Your query should return a DataFrame that contains the following columns:
	age_group, a new column based on the original age column
	post_year, a new column that contains only the year from the timestamp column
	median_follower_count, a new column containing the desired query output
 '''

pin_geo_user_join = pin_df.join(geo_df, pin_df['ind'] == geo_df['ind']) \
    .join(user_df, pin_df['ind'] == user_df['ind'])
add_age_group_column = pin_user_join.withColumn('age_group', when((pin_user_join['age'] >= 18) & (pin_user_join['age'] < 25), '18-24') \
    .when((pin_user_join['age'] >= 25) & (pin_user_join['age'] < 36), '25-35') \
    .when((pin_user_join['age'] >= 36) & (pin_user_join['age'] < 51), '36-50') \
    .otherwise('50+'))
post_year_column = add_age_group_column.withColumn('post_year', year('date_joined'))
drop_duplicate_users = post_year_column.dropDuplicates(['user_name'])
groupby_year_age = drop_duplicate_users.groupBy('post_year', 'age_group').agg(percentile_approx('follower_count', 0.5).alias('median_follower_count')) \
    .select('age_group', 'post_year', 'median_follower_count')
year_list_2015_to_2020 = ['2015', '2016', '2017', '2018', '2019', '2020']
filter_years = groupby_year_age.filter(groupby_year_age['post_year'].isin(year_list_2015_to_2020))
cleaned_table_11 = filter_years.orderBy('age_group', 'post_year')

display(cleaned_table_11)
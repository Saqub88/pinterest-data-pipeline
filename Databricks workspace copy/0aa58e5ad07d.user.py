# Databricks notebook source
# MAGIC %run "./Utility/Lib"

# COMMAND ----------

raw_user_df = fetch_dataframe("0aa58e5ad07d.user")
display(raw_user_df)

# COMMAND ----------

# Create a new column user_name that concatenates the information found in the first_name and last_name columns
# cleaned_df = cleaned_df.withColumn("Full Name", concat("First Name", lit(" "), "Last Name"))

from pyspark.sql.functions import concat, lit

user_df_concat_names = raw_user_df.withColumn("user_name", concat("first_name", lit(" "), "last_name"))
display(user_df_concat_names)

# COMMAND ----------

# Drop the first_name and last_name columns from the DataFrame
# df = df.drop("ColumnName")

user_df_drop_col = user_df_concat_names.drop("first_name", "last_name")
display(user_df_drop_col)

# COMMAND ----------

# Convert the date_joined column from a string to a timestamp data type
# df = df.withColumn("NewTimestampColumn", to_timestamp("ExistingTimestampColumn"))

from pyspark.sql.functions import to_timestamp

user_df_to_timestamp = user_df_drop_col.withColumn("date_joined", to_timestamp("date_joined"))
display(user_df_to_timestamp)

# COMMAND ----------

# Reorder the DataFrame columns to have the following column order: ind, user_name, age, date_joined
# df = df.select("Column1", "Column2", ...)

user_df = user_df_to_timestamp.select("ind", "user_name", "age", "date_joined")
display(user_df)

# COMMAND ----------

user_df.write.format("parquet").mode("overwrite").saveAsTable("0aa58e5ad07d_user_df")
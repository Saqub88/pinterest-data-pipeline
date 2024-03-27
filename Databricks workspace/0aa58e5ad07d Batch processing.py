# Databricks notebook source
# MAGIC %run "./Utility/Lib"

# COMMAND ----------

# MAGIC %run "./data_cleaning_functions"

# COMMAND ----------

# Fetch Dataframe with raw data
pin_df_raw = fetch_dataframe("0aa58e5ad07d.pin")

# Clean Dataframe
pin_df_clean = clean_pin_data(pin_df_raw)

# Write cleaned Dataframe to table
pin_df_clean.write.format("parquet").mode("overwrite").saveAsTable("0aa58e5ad07d_pin_df_1")

# COMMAND ----------

# Fetch Dataframe with raw data
geo_df_raw = fetch_dataframe("0aa58e5ad07d.geo")

# Clean Dataframe
geo_df_clean = clean_geo_data(geo_df_raw)

# Write cleaned Dataframe to table
geo_df_clean.write.format("parquet").mode("overwrite").saveAsTable("0aa58e5ad07d_geo_df")

# COMMAND ----------

# Fetch Dataframe with raw data
user_df_raw = fetch_dataframe("0aa58e5ad07d.user")

# Clean Dataframe
user_df_clean = clean_user_data(user_df_raw)

# Write cleaned Dataframe to table
user_df_clean.write.format("parquet").mode("overwrite").saveAsTable("0aa58e5ad07d_user_df")
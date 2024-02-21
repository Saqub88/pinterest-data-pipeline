# Databricks notebook source
# MAGIC %run "./Utility/Lib"

# COMMAND ----------

pin_df_raw = fetch_dataframe("0aa58e5ad07d.pin")

# COMMAND ----------

# df = df.replace({'User Info Error': None}, subset=['Status'])

pin_df_null_descriptions = pin_df_raw.replace(['No description available Story format', 'User Info Error', 'Image src error.', 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e', 'No Title Data Available', 'Untitled', 'multi-video(story page format)'], None)
pin_df_null_values = pin_df_null_descriptions.replace(0, None, subset=['downloaded'])

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# df = df.withColumn("ColumnName", regexp_replace("ColumnName", "pattern", "replacement")

pin_df_follower_count_k = pin_df_null_values.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
pin_df_follower_count_m = pin_df_follower_count_k.withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))

# COMMAND ----------

# df = df.withColumn("ColumnName", df["ColumnName"].cast("<desired_type>"))

pin_df_fc_datatype_int = pin_df_follower_count_m.withColumn("follower_count", pin_df_follower_count_m["follower_count"].cast("integer"))
pin_df_ind_datatype_int = pin_df_fc_datatype_int.withColumn("index", pin_df_follower_count_m["index"].cast("integer"))

# COMMAND ----------

pin_df_localsave_clean = pin_df_ind_datatype_int.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))

# COMMAND ----------

# df = df.withColumnRenamed("OldColumnName", "NewColumnName")

df_pin_rename_column = pin_df_localsave_clean.withColumnRenamed("index", "ind")

# COMMAND ----------

# df = df.select("Column1", "Column2", ...)

df_pin = df_pin_rename_column.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")
display(df_pin)

# COMMAND ----------

df_pin.write.format("parquet").mode("overwrite").saveAsTable("0aa58e5ad07d_pin_df_1")
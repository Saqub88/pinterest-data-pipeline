# Databricks notebook source
# MAGIC %run "./Utility/Lib"

# COMMAND ----------

geo_df_raw = fetch_dataframe("0aa58e5ad07d.geo")
display(geo_df_raw)

# COMMAND ----------

# df = df.withColumn("new_array_column", array("column1", "column2"))

from pyspark.sql.functions import array

geo_df_new_column = geo_df_raw.withColumn("coordinates", array("latitude", "longitude"))
display(geo_df_new_column)

# COMMAND ----------

# df = df.drop("ColumnName")
# Dropping multiple columns
# df = df.drop("ColumnName1", "ColumnName2", ...)

geo_df_drop_column = geo_df_new_column.drop("latitude", "longitude")
display(geo_df_drop_column)

# COMMAND ----------

# df = df.withColumn("NewTimestampColumn", to_timestamp("ExistingTimestampColumn"))

from pyspark.sql.functions import to_timestamp

geo_df_to_timestamp = geo_df_drop_column.withColumn("timestamp", to_timestamp("timestamp"))
display(geo_df_to_timestamp)

# COMMAND ----------

# df = df.select("Column1", "Column2", ...)

geo_df = geo_df_to_timestamp.select("ind", "country", "coordinates", "timestamp")
display(geo_df)

# COMMAND ----------

geo_df.write.format("parquet").mode("overwrite").saveAsTable("0aa58e5ad07d_geo_df")
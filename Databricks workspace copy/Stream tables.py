# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
import urllib

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disable format checks during the reading of Delta tables.
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# MAGIC %run "./data_cleaning_functions"

# COMMAND ----------

# Create schema for the pin, geo and user data.
from pyspark.sql.types import StructType,StructField, StringType
pin_schema = StructType([ 
    StructField("index",StringType(),True), 
    StructField("unique_id",StringType(),True), 
    StructField("title",StringType(),True), 
    StructField("description", StringType(), True),
    StructField("poster_name",StringType(),True),
    StructField("follower_count",StringType(),True),
    StructField("tag_list",StringType(),True),
    StructField("is_image_or_video",StringType(),True),
    StructField("image_src",StringType(),True),
    StructField("downloaded",StringType(),True),
    StructField("save_location",StringType(),True),
    StructField("category",StringType(),True)
  ])

geo_schema = StructType([ 
    StructField("ind",StringType(),True), 
    StructField("timestamp",StringType(),True),
    StructField("latitude",StringType(),True),
    StructField("longitude",StringType(),True),
    StructField("country",StringType(),True) 
  ])

user_schema = StructType([ 
    StructField("ind",StringType(),True), 
    StructField("first_name",StringType(),True),
    StructField("last_name",StringType(),True),
    StructField("age",StringType(),True),
    StructField("date_joined",StringType(),True) 
  ])

# COMMAND ----------

# Fetch data from kinesis stream
pin_df_kinesis_stream = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0aa58e5ad07d-pin') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load() \

# Deserialize the data into string and then, using the schema, create a table of the data
pin_df_raw = pin_df_kinesis_stream.selectExpr("CAST(data as STRING)") \
    .withColumn("pin_columns",from_json(col("data"),pin_schema)) \
    .select("pin_columns.*")

# Clean the data
pin_df_clean = clean_pin_data(pin_df_raw)

# Write data to Databricks Delta Table
pin_df_clean.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0aa58e5ad07d_pin_table")

# COMMAND ----------

# Fetch data from kinesis stream
geo_df_kinesis_stream = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0aa58e5ad07d-geo') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

# Deserialize the data into string and then, using the schema, create a table of the data
geo_df_raw = geo_df_kinesis_stream.selectExpr("CAST(data as STRING)") \
    .withColumn("geo_columns",from_json(col("data"),geo_schema)) \
    .select("geo_columns.*")

# Clean the data
geo_df_clean = clean_geo_data(geo_df_raw)

# Write data to Databricks Delta Table
geo_df_clean.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0aa58e5ad07d_geo_table")

# COMMAND ----------

# Fetch data from kinesis stream
user_df_kinesis_stream = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0aa58e5ad07d-user') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

# Deserialize the data into string and then, using the schema, create a table of the data
user_df_raw = user_df_kinesis_stream.selectExpr("CAST(data as STRING)") \
    .withColumn("user_columns",from_json(col("data"),user_schema)) \
    .select("user_columns.*")

# Clean the data
user_df_clean = clean_user_data(user_df_raw)

# Write data to Databricks Delta Table
user_df_clean.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0aa58e5ad07d_user_table")
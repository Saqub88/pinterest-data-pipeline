# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS 0aa58e5ad07d_db
# MAGIC LOCATION 'dbfs:/user/hive/0aa58e5ad07d_db.db'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disable format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

def fetch_dataframe(topic):
    # File location and type
    # Asterisk(*) indicates reading all the content of the specified file that have .json extension
    file_location = f"/mnt/0aa58e5ad07d-aws-s3-bucket/topics/{topic}/partition=0/*.json"
    file_type = "json"
    # Ask Spark to infer the schema
    infer_schema = "true"
    # Read in JSONs from mounted S3 bucket
    return spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .load(file_location)

# COMMAND ----------


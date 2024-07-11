# Databricks notebook source
# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# COMMAND ----------

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = (
    aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
)
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# AWS S3 bucket name
AWS_S3_BUCKET = "user-0aa58e5ad07d-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/0aa58e5ad07d-aws-s3-bucket"
# Source url
SOURCE_URL = (
    "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
)
# Mount the drive
# dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)
# Once this has run, no need to run again unless AWS_S3_Bucket is unmounted.
# To test whether AWS_S3_Bucket is mounted, run the code in 
# "/Workspace/Users/saqub_ali88@hotmail.co.uk/Utility/S3 storage mount checker"
# Databricks notebook source
# MAGIC %md
# MAGIC # Mount checker
# MAGIC This will assess whether the S3 storage has been correctly mounted. The code returns a table displaying the contents of the mounted S3 storage assuming that the mount was completed successfully. Otherwise the code would fail to return a table.

# COMMAND ----------

display(dbutils.fs.ls("/mnt/mount_name/../.."))
# Databricks notebook source
# MAGIC %run "./Utility/Lib"

# COMMAND ----------

geo_df = fetch_dataframe("0aa58e5ad07d.geo")
display(geo_df)
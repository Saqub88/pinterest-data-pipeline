# Databricks notebook source
# MAGIC %run "./Utility/Lib"

# COMMAND ----------

user_df = fetch_dataframe("0aa58e5ad07d.user")
display(user_df)
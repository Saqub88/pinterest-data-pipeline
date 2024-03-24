# Databricks notebook source
def clean_pin_data(pin_dataframe):
    from pyspark.sql.functions import regexp_replace

    pin_df_clean = pin_dataframe \
        .replace(['No description available Story format', 'User Info Error', 'Image src error.', 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e', 'No Title Data Available', 'Untitled', 'multi-video(story page format)'], None) \
        .replace(0, None, subset=['downloaded']) \
        .withColumn("follower_count", regexp_replace("follower_count", "k", "000")) \
        .withColumn("follower_count", regexp_replace("follower_count", "M", "000000")) \
        .withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))

    pin_df_clean = pin_df_clean \
        .withColumn("follower_count", pin_df_clean["follower_count"].cast("integer")) \

    pin_df_clean = pin_df_clean \
        .withColumn("index", pin_df_clean["index"].cast("integer")) \
        .withColumnRenamed("index", "ind") \
        .select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")
    
    return pin_df_clean

# COMMAND ----------

def clean_geo_data(geo_dataframe):
    from pyspark.sql.functions import array, to_timestamp

    geo_df_clean = geo_dataframe \
        .withColumn("coordinates", array("latitude", "longitude")) \
        .withColumn("timestamp", to_timestamp("timestamp")) \
        .drop("latitude", "longitude") \
        .select("ind", "country", "coordinates", "timestamp")

    return geo_df_clean

# COMMAND ----------

def clean_user_data(user_dataframe):
    from pyspark.sql.functions import concat, lit, to_timestamp

    user_df_clean = user_dataframe \
        .withColumn("user_name", concat("first_name", lit(" "), "last_name")) \
        .withColumn("date_joined", to_timestamp("date_joined")) \
        .drop("first_name", "last_name") \
        .select("ind", "user_name", "age", "date_joined")

    return user_df_clean
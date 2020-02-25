from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


#start spark session
spark = SparkSession.builder.appName("user_summary").getOrCreate()

#load JSON Data
user_df = spark.read.json("/opt/spark-data/user.json")

#aggregate user social engagement
user_final = user_df.select("user_id","name","average_stars",\
               F.size(F.split(F.col("elite"),",")).alias("total_elite"),\
              F.size(F.split(F.col("friends"),",")).alias("total_friends"),\
              (F.col("useful")+F.col("funny")+F.col("cool")).alias("total_votes"),
              (F.col("compliment_cool")+F.col("compliment_cute")+F.col("compliment_funny")+\
              F.col("compliment_hot")+F.col("compliment_list")+F.col("compliment_more")+\
               F.col("compliment_note")+F.col("compliment_photos")+F.col("compliment_plain")+\
               F.col("compliment_profile")+F.col("compliment_writer")).alias("total_compliments"))

#write user_summary table in Cassandra
user_final.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="user_summary", keyspace="yelp")\
    .save()

#stop spark session
spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import pygeohash as pgh

#start spark session
spark = SparkSession.builder.appName("business-rec-cassandra-write").getOrCreate()

#load JSON Data
business_df = spark.read.json('/opt/spark-data/business.json')
review_df = spark.read.json('/opt/spark-data/review.json')

#create user categories dataframe
user_cat_df = F.broadcast(business_df).join(review_df,['business_id'])\
    .groupBy('user_id')\
    .agg(F.collect_list("categories").alias('categories'))
#write user table in Cassandra
user_cat_df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="user", keyspace="yelp")\
    .save()

#Create UDF functions to transform hors to dict, lat e lon to geohash, review and star to popularity
udf_hours = F.udf(lambda hour: str(hour.asDict()) if hour is not None else {},  )
udf_geohash = F.udf(lambda lat,lon,prec: pgh.encode(lat,lon,precision=prec))
udf_popularity = F.udf(lambda review,star: review*star,FloatType())
#Create geo business DataFrame
geo_business = business_df.withColumn("geohash",udf_geohash(F.col('latitude'),F.col('longitude'),F.lit(9)))\
    .withColumn("geokey",udf_geohash(F.col('latitude'),F.col('longitude'),F.lit(5)))\
    .withColumn("popularity",udf_popularity(F.col('review_count'),F.col('stars')))\
    .withColumn("week_hours",udf_hours(F.col('hours')))\
    .select("business_id",\
            "name",\
            F.split(F.col("categories"),",").alias('categories'),\
            F.col("week_hours").alias('hours'),\
            "geohash",\
            "geokey",\
            "popularity")

#write geo_business table in Cassandra
geo_business.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="geo_business", keyspace="yelp")\
    .save()

#stop spark session
spark.stop()
#spark-submit --master local[*] --conf spark.cassandra.connection.host=cassandra /opt/spark-apps/business-rec-cassandra-write.py
#kafka-console-producer --broker-list localhost:9092 --topic
#kafkaStream = KafkaUtils.createDirectStream(ssc, ['user'], {"metadata.broker.list":'boker:9092'})

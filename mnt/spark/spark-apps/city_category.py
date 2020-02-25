from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


#start spark session
spark = SparkSession.builder.appName("city_category").getOrCreate()

#load JSON Data
business_df = spark.read.json('/opt/spark-data/business.json')

#select city and categories fields
city_cat = business_df\
.select(F.col("city"),F.explode(F.split(F.col('categories'),",")).alias("cat"))\
.withColumn('city2',F.trim(F.col("city")))\
.withColumn("cat2", F.trim(F.col("cat")))\
.select(F.col("city2").alias("city"),F.col("cat2").alias("cat"))

#create sql view
city_cat.createOrReplaceTempView("city_cat")
#group by cities and categories
city_cat_final =spark.sql("""
                          select city,
                          cat as category,
                          total,
                          rank() over (PARTITION  BY city order by total desc) as rn
                          from
                          (select city, cat, count(cat) as total from
                          city_cat group By city,cat)
                          """)

#write city_category table in Cassandra
city_cat_final.filter((F.col("city") != "" ) & (F.col("category") != "" )).write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="city_category", keyspace="yelp")\
    .save()

#stop spark session
spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


#start spark session
spark = SparkSession.builder.appName("review_summary").getOrCreate()

#load JSON Data
business_df = spark.read.json('/opt/spark-data/business.json')
review_df = spark.read.json('/opt/spark-data/review.json')

#join business and review
business_review = F.broadcast(business_df.select("business_id","name")).join(review_df,["business_id"])
#create sql view
business_review.createOrReplaceTempView("business_review")
#group votes by business_id and name
business_review_final = spark.sql("""
                                  select business_id,
                                  name,
                                  round(avg(stars),2) as stars_avg,
                                  round(avg(funny),2) as funny_avg,
                                  round(avg(cool),2) as cool_avg,
                                  round(avg(useful),2) as useful_avg,
                                  (count(cool) + count(funny) + count(useful)) as total_votes
                                  from business_review group by business_id,name
                                  """)

#write review_summary table in Cassandra
business_review_final.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="review_summary", keyspace="yelp")\
    .save()

#stop spark session
spark.stop()

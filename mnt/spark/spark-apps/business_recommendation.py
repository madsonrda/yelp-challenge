from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import pygeohash as pgh

#start spark session
spark = SparkSession.builder.appName("business_recommendation").getOrCreate()
business_df = spark.read.json('/opt/spark-data/business.json')

#load JSON Data
userlocation_df = spark.read.json("/opt/spark-data/userlocation.json")

#transform Row hours struct to MapType
def hours(hour):
    if hour is not None:
        return hour.asDict()
    else:
        return {"Monday":"","Tuesday":"","Wednesday":"","Thursday":"","Friday":"","Saturday":"","Sunday":""}
#Create UDF functions to transform hors to dict, lat e lon to geohash, review and star to popularity
udf_hours = F.udf(hours,MapType(StringType(),StringType()))
udf_geohash = F.udf(lambda lat,lon,prec: pgh.encode(lat,lon,precision=prec))
udf_popularity = F.udf(lambda review,star: review*star,FloatType())
#Create geo business DataFrame
geo_business = business_df.withColumn("geohash",udf_geohash(F.col('latitude'),F.col('longitude'),F.lit(9)))\
    .withColumn("geokey",udf_geohash(F.col('latitude'),F.col('longitude'),F.lit(5)))\
    .withColumn("popularity",udf_popularity(F.col('review_count'),F.col('stars')))\
    .withColumn("hourss",udf_hours(F.col("hours")))\
    .select("business_id",\
            F.split(F.col("categories"),",").alias('categories'),\
            "geohash",\
            "geokey",\
            F.col("hourss").alias("hours"),\
            "popularity")

#parser user location Rows and return user_id, geokey, date,category
def parser(line):
    from pyspark.sql import Row
    from collections import defaultdict
    from datetime import datetime
    import pygeohash as pgh

    user_id = line["user_id"]
    geokey = pgh.encode(line["latitude"],line["longitude"],precision=5)

    date = datetime.fromtimestamp(line['timestamp'])
    category = line["category"]

    row = defaultdict()

    row["user_id"] = user_id
    row['geokey'] = geokey
    row["date"] = date
    row["category"] = category

    return Row(**row)
#apply parser function
user_parsed = userlocation_df.rdd.map(parser).toDF()

#join user data with business data
businessRecommendation = F.broadcast(user_parsed).join(geo_business,["geokey"])

#UDF function to check if a business is open
def is_open(date,hours):
    import datetime
    weekday = date.strftime('%A')
    time = date.time()

    interval = hours[weekday]

    def time_in_range(start, end, x):
        if start <= end:
            return start <= x <= end
        else:
            return start <= x or x <= end

    if interval:
        start,end = interval.split("-")
        start_h,start_m = start.split(":")
        end_h,end_m = end.split(":")
        start = datetime.time(int(start_h),int(start_m))
        end = datetime.time(int(end_h),int(end_m))
        return time_in_range(start,end,time)
    else:
        return False

udf_open = F.udf(is_open)
#add open column
businessRecommendation_open = businessRecommendation.withColumn("open",udf_open(F.col("date"),F.col("hours")))
#filter if bussiness category list contains user category list and if the business is open
businessRecommendation_filter = businessRecommendation_open.filter(\
                                (F.arrays_overlap(F.col("categories"),F.col("category")))\
                                  & (F.col("open") == True ) )
#create view
businessRecommendation_filter.createOrReplaceTempView("BR")

#select top business
businessRecommendation_top = \
    spark.sql("""
              select *
              from
              (select user_id,
               business_id, popularity ,
                Row_number()
                over (partition by user_id order by popularity desc) as rn from BR)
                 where rn < 4
              """)

#aggregate top business by user in a list
businessRecommendation_final = businessRecommendation_top.groupBy("user_id")\
                                .agg(F.collect_list("business_id").alias("business"))

#write business_recommendation table in Cassandra
businessRecommendation_final.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="business_recommendation", keyspace="yelp")\
    .save()

#stop spark session
spark.stop()

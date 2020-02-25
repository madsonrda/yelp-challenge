#!/bin/bash

city_category()
{
  echo "start application city_category"
  spark-submit --master local[*] --conf spark.cassandra.connection.host=cassandra /opt/spark-apps/city_category.py
  echo "end application city_category"
}

review_summary()
{
  echo "start application review_summary"
  spark-submit --master local[*] --conf spark.cassandra.connection.host=cassandra /opt/spark-apps/review_summary.py
  echo "end application review_summary"
}

user_summary()
{
  echo "start application user_summary"
  spark-submit --master local[*] --conf spark.cassandra.connection.host=cassandra /opt/spark-apps/user_summary.py
  echo "end application user_summary"
}

business_recommendation()
{
  echo "start application business_recommendation"
  spark-submit --master local[*] --conf spark.cassandra.connection.host=cassandra /opt/spark-apps/business_recommendation.py
  echo "end application business_recommendation"
}


city_category && review_summary &&  user_summary &&  business_recommendation && echo "spark submit complete successfully " || echo "spark submit did not complete successfully"

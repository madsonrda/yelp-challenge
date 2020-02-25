#!/bin/bash

CQL="CREATE KEYSPACE yelp WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE yelp;
CREATE TABLE city_category(
city text,
category text,
total float,
rn int,
PRIMARY KEY (city, category)
);
CREATE TABLE review_summary(
business_id text,
name text,
stars_avg float,
funny_avg float,
cool_avg float,
useful_avg float,
total_votes int,
PRIMARY KEY (business_id)
);
CREATE TABLE user_summary(
user_id text,
name text,
average_stars float,
total_elite int,
total_friends int,
total_votes int,
total_compliments int,
PRIMARY KEY (user_id)
);
CREATE TABLE business_recommendation(
user_id text,
business list<text>,
PRIMARY KEY (user_id)
);
"
until echo $CQL | cqlsh; do
  echo "cqlsh: Cassandra is unavailable - retry later"
  sleep 2
done &

exec /docker-entrypoint.sh "$@"

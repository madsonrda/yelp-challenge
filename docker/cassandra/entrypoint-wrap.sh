#!/bin/bash

CQL="CREATE KEYSPACE yelp WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE yelp;
CREATE TABLE user(
user_id text PRIMARY KEY,
categories list<text>
);
CREATE TABLE geo_business (
  geokey text,
  geohash text,
  business_id text,
  name text,
  categories list<text>,
  hours text,
  popularity float,
  PRIMARY KEY (geokey, geohash)
);"
until echo $CQL | cqlsh; do
  echo "cqlsh: Cassandra is unavailable - retry later"
  sleep 2
done &

exec /docker-entrypoint.sh "$@"

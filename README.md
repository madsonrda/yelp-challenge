# yelp-challenge

## Setup

For the test I used a PC with Debian 10 OS, AMD Ryzen 3 2200G processor, 16 GB of RAM, 120 GB SSD, 1 TB SATA.

The requirements are docker and docker-compose

## Description
This project consists of 4 pyspark batch applications that read the yelp datasets in JSON format, execute some queries and save the results in tables in Cassandra.

### Business Recommendation app

This application aims to obtain for each user a list of the most popular businesses that are open, given the user_id, location, category list, timestamp. For this task I created the following JSON file ``mnt/spark/spark-data/userlocation.json:


```
{"user_id": "l6BmjZMeQD3rDxWUbiAiow", "latitude":36.1956146,"longitude":-115.2872775,"timestamp":1582564875,"category":["Food"]}
{"user_id": "4XChL029mKr5hydo79Ljxg", "latitude":36.0801679,"longitude":-115.1827558,"timestamp":1582192996,"category":["Shopping"]}
{"user_id": "bc8C_eETBWL0olvFSJJd0w", "latitude":43.79199,"longitude":-79.44453,"timestamp":1582840996,"category":["Bars"]}
{"user_id": "dD0gZpBctWGdWo9WlGuhlA", "latitude":36.2395570019,"longitude":-115.0796038143,"timestamp":1582120996,"category":["Pizza"]}
{"user_id": "MM4RJAeH6yuaN8oZDSt0RA", "latitude":36.1956146,"longitude":-115.0796038143,"timestamp":1582564875,"category":["Doctors"]}
{"user_id": "KGuqerdeNhxzXZEyBaqqSw", "latitude":36.2395570019,"longitude":-115.2872775,"timestamp":1582725796,"category":["Restaurants"]}
```

The application file is in ``mnt/spark/spark-apps/business_recommendation.py``.

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

### Cities and Categories

This application creates a dataset that selects the quantity of each category grouped by cities.
The application file is in ``mnt/spark/spark-apps/city_category.py``.

### Business review summary

This application aggregates reviews vote information by business.
The application file is in ``mnt/spark/spark-apps/review_summary.py``.

### Users social engagement summary

This application aggregates social engagement information by user.
The application file is in ``mnt/spark/spark-apps/user_summary.py``.

## Extract yelp data files

To extract yelp data files:

```
./extract-yelp-files.sh /your/path/yelp_dataset.tar.gz
```

## Running Docker containers

To start the Docker containers:

```
docker-compose up -d --no-deps --build
```

## Launch Spark applications

To running Spark applications:

```
docker-compose exec spark-master /bin/bash /opt/spark-apps/submit.sh
```

A few minutes later you should see the following message:

```
spark submit complete successfully
```

## Check the tables in Cassandra

Run Cassandra cqlsh:

```
docker-compose exec cassandra cqlsh
```

Run the following queries:

```
cqlsh> use yelp;
SELECT * from user_summary limit 10;

 user_id                | average_stars | name     | total_compliments | total_elite | total_friends | total_votes
------------------------+---------------+----------+-------------------+-------------+---------------+-------------
 qxAiQgE0aTC_bbGPu_YuWQ |           3.6 |     Mira |                 0 |           1 |             1 |           1
 UB8D8KzsKFaDVF8Vh4k28w |          4.12 |    Lenny |                 0 |           1 |            71 |          10
 a-P3SIgJ72pq1rThPKUSAA |             2 |    Susan |                 0 |           1 |             9 |           6
 JQ47IloLwOd1rJ0tQ3ev2Q |             5 |   Ashley |                 0 |           1 |             1 |           1
 cBig6mWo_zmjlgh1aBJHSQ |             3 |    Debra |                 0 |           1 |             1 |           4
 uq5Gmjzb3YtYliOhYCkxMQ |             5 |   THOMAS |                 0 |           1 |             1 |           6
 D23k2x1meyO13L1hrqq1iA |             5 | Corrinne |                 0 |           1 |             1 |           1
 bW8jWqYp0WiaFYvO20qlRg |             3 |     Mark |                 0 |           1 |             1 |           0
 -A5p-mU5pLQV0oNb4NGDfg |             3 |    Mirai |                 0 |           1 |             1 |           0
 zTAI78wS99D0J-iajCfLWw |          4.92 |    Casey |                 0 |           1 |             1 |           3

(10 rows)
cqlsh:yelp> SELECT * from review_summary  limit 10;

 business_id            | cool_avg | funny_avg | name                       | stars_avg | total_votes | useful_avg
------------------------+----------+-----------+----------------------------+-----------+-------------+------------
 uh9ianAy7l4ev6fQHEWusQ |     0.67 |      0.33 | Hot Yoga Wellness Brampton |      3.67 |           9 |       3.33
 i6x-Wf2uvx4YydJpPmaXmg |     1.02 |      0.48 |             Dozen Cupcakes |      3.33 |         138 |        2.3
 LU5zpQREdZUB1b5ZKgC2zQ |        0 |      0.67 |              Churro Burger |         1 |           9 |       1.67
 r8Jr-7YfkhJL2nxk9lxUNg |     0.49 |      0.26 |      Frank & Fina's Cocina |      4.09 |        1257 |       0.84
 745DO9ToR7mODW13t9m3tg |     0.37 |      0.27 |    Lowe's Home Improvement |      2.78 |         123 |       0.68
 fJ-2acaqvWOsujUTAJB-ew |     0.26 |      0.23 |               Raijin Ramen |      3.44 |        1032 |        0.6
 h84tU6REsM1VzyUCP0Ss7Q |     0.25 |      0.16 |                 Baja Fresh |      2.89 |         132 |       1.02
 dmQY_zODV6ymRxRSKs7FIw |     0.07 |      0.03 |  Goldwing Appliance Repair |      1.83 |          87 |          1
 yYgeUm-8qXhZiqTrVyQrKg |     0.88 |      0.28 |              Plaka Taverna |      4.07 |         270 |       1.39
 jc_wj2p3kWyGhjcUSsLo7Q |        0 |       0.6 |  Dragun's Landscape Supply |         4 |          15 |        0.2

(10 rows)
cqlsh:yelp> SELECT * from business_recommendation   limit 10;

 user_id                | business
------------------------+--------------------------------------------------------------------------------
 bc8C_eETBWL0olvFSJJd0w | ['yhDAzBBjFujZbHwBPfE2eQ', 'KvWtJUUT_nEGfWJ2u7wbdw', 'tU3zlAADEJREAaDas8oCOg']
 4XChL029mKr5hydo79Ljxg | ['ajWAVO3L3bFTpk3SVo9JcQ', 'lYFpDTiVMNrpkpFyIEcc_w', 'TKeiQuLVDLheXy9RSlKMYA']
 l6BmjZMeQD3rDxWUbiAiow | ['_8mBVtt6yhkS0YtT1FGBNg', 'sKhDrZFCJqfRNylkHrIDsQ', 'Pg4U6vKD42yswcGtnrBSSA']
 KGuqerdeNhxzXZEyBaqqSw | ['euksvHm653hJMJBqzcvMzQ', 'gRdBkmXdRqUzDMkcMtt7rQ', 'yYkvonvBGZB6UMnpzr6Ryw']
 dD0gZpBctWGdWo9WlGuhlA |                                                     ['e9gaoUQEws5tmQROZodZMg']

(5 rows)
cqlsh:yelp> SELECT * from city_category  limit 10;

 city        | category               | rn | total
-------------+------------------------+----+-------
 Finleyville |            Active Life | 10 |     2
 Finleyville |         American (New) | 15 |     1
 Finleyville | American (Traditional) |  5 |     3
 Finleyville |             Appliances | 15 |     1
 Finleyville |    Appliances & Repair | 15 |     1
 Finleyville |          Arts & Crafts | 15 |     1
 Finleyville |   Arts & Entertainment | 15 |     1
 Finleyville |               Bakeries | 15 |     1
 Finleyville |               Barbeque | 15 |     1
 Finleyville |                Beaches | 15 |     1

(10 rows)

```

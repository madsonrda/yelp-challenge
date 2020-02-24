FROM bde2020/spark-master:2.4.4-hadoop2.7
ENV SPARK_HOME=/spark
ENV PATH=$SPARK_HOME/bin:$PATH
COPY jars/ /spark/jars/
RUN pip install pygeohash

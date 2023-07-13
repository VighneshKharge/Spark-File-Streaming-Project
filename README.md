# Spark-File-Streaming-Project

## Project Overview

step 1:

Create 3 directories in hdfs 

For raw data -> hadoop fs -mkdir /hdfs_path/json_data

For Hive data -> hadoop fs -mkdir /hdfs_path/to_hive 

For Cassandradata -> hadoop fs -mkdir /hdfs_path/to_cassandra

step 2:

put JSON data in the raw directory ->
hadoop fs -put file_name /hdfs_path/json_data

step 3:

Monitoring raw directory using the spark s & converting nested json data into simple per product invoice josn data & writing it to  CSV format then  sending to 'to_hive' and 'to_cassandra' directories on hdfs 

from the 'to_hive' directory this data can be used for creating a table & then querying the data

from the 'to_cassandra' directory data will be sent to Cassandra NoSQL db and the s3 bucket further from there using AWS Glue & Athena data can be queried. AWS Quicksight is used to visualize the query results.

## Architecture Diagram
![This is architecture](https://github.com/VighneshKharge/Spark-File-Streaming-Project/blob/main/file_streaming_arch.png)

## 



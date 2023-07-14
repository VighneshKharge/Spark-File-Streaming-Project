# Spark-File-Streaming-Project

## Project Overview

**Step 1:**

Create 3 directories in hdfs 

For raw data -> hadoop fs -mkdir /hdfs_path/json_data

For Hive data -> hadoop fs -mkdir /hdfs_path/to_hive 

For Cassandradata -> hadoop fs -mkdir /hdfs_path/to_cassandra

**Step 2:**

put JSON data in the raw directory ->
hadoop fs -put file_name /hdfs_path/json_data

**Step 3:**

Monitoring raw directory using the spark s & converting nested json data into simple per product invoice josn data & writing it to  CSV format then  sending to 'to_hive' and 'to_cassandra' directories on hdfs 

from the 'to_hive' directory this data can be used for creating a table & then querying the data

from the 'to_cassandra' directory data will be sent to Cassandra NoSQL db and the s3 bucket further from there using AWS Glue & Athena data can be queried. AWS Quicksight is used to visualize the query results.

## Problem Statement 

Received e-commerce invoice data is having a nested structure so, while creating dataframe from that data each product associated with the invoice number needs to be separated. This data needs to be sent in CSV format 'to_hive' & to_cassandra' folders. The following image shows the problem statement;

![This is Nested JOSN](https://github.com/VighneshKharge/Spark-File-Streaming-Project/blob/main/Nested%20JSON.png)

## Architecture Diagram
![This is architecture](https://github.com/VighneshKharge/Spark-File-Streaming-Project/blob/main/file_streaming_arch.png)

## Flow Of Execution

**Phase 1** 

- Breaking nested JSON data present in 'json_data' on HDFS & converting it to simple per-product invoice josn data & writing it to  CSV format. Please refer HDFS_Monitor.py

- As in the problem statement reference image shows how the data frame is created using raw json data i.e. Invoice1.json file.  

- Now 1st select the required columns & break the nested json column using explode function & separate each item per invoice number column as 'LineItem'

![This is explode DF](https://github.com/VighneshKharge/Spark-File-Streaming-Project/blob/main/ExplodeDF.png)

- From the 'LineItem' column extract the keys as column

![This is Extract_LineItem](https://github.com/VighneshKharge/Spark-File-Streaming-Project/blob/main/Extract_LineItem..png)

- Then this DataFrame is converted to CSV file & sent to 'to_hive' & 'to_cassandra' directories.

**Phase 2**

- From the 'to_cassandra' directory sending CSV data to the Cassandra table and to the s3 bucket on AWS creating a DAG in Apache Airflow.

- Apache Airflow is a task scheduler tool. Where we can deploy a DAG (Directed Acyclic Graph) which when triggered will execute tasks. 

- 1st create cassandra keyspace  & table. Please refer to 'Cassandra commands.txt'.

 





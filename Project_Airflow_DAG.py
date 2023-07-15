from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import os
import boto3
import pyarrow
import pyarrow.hdfs as hdfs


default_args = {'owner':'airflow', 'start_date':datetime(2023,7,5)}

file_dag = DAG(dag_id='file_transfer',default_args=default_args,schedule_interval=None, catchup=False)


delete_command = 'hadoop fs -rm -r /user/ubh01/to_cassandra/_spark_metadata'

delete_unnecessary=BashOperator(
    task_id='delete_unnecessary',
    bash_command = delete_command,
    dag=file_dag
)



spark_command = "spark-submit --jars /home/ubh01/spark-3.0.3-bin-hadoop2.7/jars/spark-cassandra-connector-assembly_2.12-3.1.0.jar /home/ubh01/spark-cassandra.py"

spark_to_cassandra = BashOperator(
    task_id='to_cassandra',
    bash_command=spark_command,
    dag=file_dag
)

send_to_s3=BashOperator(
    task_id='send_to_s3',
    bash_command='python /home/ubh01/hdfs-s3.py',  
    dag=file_dag
)


delete_command = "hadoop fs -rm -r /user/ubh01/to_cassandra/*"

delete_file = BashOperator(
    task_id='delete_file',
    bash_command=delete_command,
    dag=file_dag
)

print_success = BashOperator(
    task_id='print_sucsess',
    bash_command='echo "all tasks are successful."',
    dag=file_dag
)


# Defining flow task in DAG
delete_unnecessary >> spark_to_cassandra >> send_to_s3 >> delete_file >> print_success


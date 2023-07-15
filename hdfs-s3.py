from pydoop import hdfs
import boto3

#Path where CSV file on hdfs is stored i.e. 'to_cassandra' directory
hdfs_path = 'hdfs://localhost:9000/user/ubh01/to_cassandra/part-00000-7a3e8ab2-511a-449a-86e5-139f003ad19a-c000.csv' 

bucket = 'give-your-s3-bucket-name'
s3_client = boto3.client(service_name='s3', \
                         aws_access_key_id= 'give-your-aws-user-access-key', \
                         aws_secret_access_key ='give-your-aws-user--secret-access-key')


s3_folder = 'give-folder-path-where-csv-will-be-stored'
with hdfs.open(hdfs_path,'r') as f:
    content=f.read()
s3_client.put_object(Body=content,Bucket=bucket,Key=s3_folder)
print("The file is uploaded to the S3 bucket successfully.")



# from pyspark.sql import SparkSession
# from delta.tables import *
import boto3
import os
import json
import pandas as pd

# os.environ['AWS_PROFILE'] = "default"
# os.environ['AWS_DEFAULT_REGION'] = "us-east-2"

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.2.0  --driver-memory 2g pyspark-shell'

path = f'{os.path.realpath(os.path.dirname(__file__))}'

with open(f"{path}/access_keys.json", 'r') as f:
        file = json.load(f)
        access_key = file["accessKey"]
        secret_key = file["secretAccessKey"]


# spark = SparkSession.builder.appName("citibikes") \
#         .config("spark.pyspark.python", "python") \
#         .config("spark.hadoop.fs.s3a.access.key", access_key) \
#         .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
#         .config("spark.hadoop.fs.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#         .config("spark.debug.maxToStringFields", "500") \
#         .config("spark.sql.execution.arrow.pyspark.enabled", True) \
#         .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
#         .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
#         .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
#         .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
#         .getOrCreate()


s3 = boto3.client('s3')

bucket_name = 'citi-delta-lake'
transient = 'transient/csv_files/'
bronze = 'bronze/'

# get s3 object list
# response = s3.list_objects(Bucket=bucket_name, Prefix=transient)

response = s3.list_objects_v2(Bucket=bucket_name, Prefix=transient)

for content in response.get('Contents', []):
    # print(content['Key'])
    csv_file = content['Key']
    try:
        if csv_file.endswith(".csv"):
             
             df = pd.read_csv(f"s3a://{bucket_name}/{csv_file}", low_memory=False)
             file = csv_file.replace("transient/csv_files/", "")
             file = file.replace(".csv", "")   
             bronze_layer = f"s3a://{bucket_name}/{bronze}{file}"
             df.to_parquet(bronze_layer, engine="fastparquet")
             # remove file from source folder
             s3.delete_object(Bucket=bucket_name, Key=csv_file)
        
             print(f"converting {file}...")

        else:
            raise ValueError(f"Not a file or invalid extension: {csv_file}")
        
    except ValueError as e:
        print(f"Error: {str(e)}")


print(".csv to PARQUET step done.")


from pyspark.sql import SparkSession
import boto3
import os

os.environ['AWS_PROFILE'] = "default"
os.environ['AWS_DEFAULT_REGION'] = "us-east-2"

s3 = boto3.client('s3')

bucket_name = 'citi-delta-lake'
transient = 'transient/csv_files/'
delta_bronze = 'bronze/'

spark = SparkSession.builder \
    .appName("delta") \
    .getOrCreate()

# get s3 object list
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=transient)

for file in response['Contents']:
    csv_file = file['Key']

    df = spark.read.csv(f"s3a://{bucket_name}/{csv_file}", header=True, inferSchema=True)

    bronze_layer = f"s3a://{bucket_name}/{delta_bronze}/{csv_file.replace('.csv', '.delta')}"

    df.write.format("delta").save(bronze_layer)

    # remove file from source folder
    s3.delete_object(Bucket=bucket_name, Key=csv_file)

print(".csv to delta step done.")

spark.stop()

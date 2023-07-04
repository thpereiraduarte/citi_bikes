import boto3
import os

os.environ['AWS_PROFILE'] = "default"
os.environ['AWS_DEFAULT_REGION'] = "us-east-2"

s3 = boto3.client('s3')

bucket_name = 'citi-delta-lake'
s3_folder = 'transient/csv_files/'

local_folder = 'csv_files'

csv_files = [f for f in os.listdir(local_folder) if f.endswith(".csv")]

for csv in csv_files:
    my_path = os.path.join(local_folder, csv)
    s3_key = os.path.join(s3_folder, csv)

    s3.upload_file(my_path, bucket_name, s3_key)
    
    # remove local files after the upload
    os.remove(my_path)

print("Files upload done! Local files removed.")

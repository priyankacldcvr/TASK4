import os
from google.cloud import storage
from datetime import datetime

def move_files():
  bucket_name = 'walmart_pc'
  folder_name = 'rawfolder'
  file_name = 'Walmart.csv'
  destination_bucket_name = "walmart_pc"
  destination_folder_path = 'staging'
  storage_client = storage.Client()
  source_bucket = storage_client.bucket(bucket_name)
  source_blob = source_bucket.blob(f"{folder_name}/{file_name}")
  # created_time = source_blob.time_created
  date_str = datetime.strftime(datetime.today(), '%Y-%m-%d')
  destination_bucket = storage_client.bucket(destination_bucket_name)
  new_blob = source_bucket.copy_blob(source_blob, destination_bucket, f"{destination_folder_path}/{date_str}/{file_name}")
  source_blob.delete()
  
  
move_files()

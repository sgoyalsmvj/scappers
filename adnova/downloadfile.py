import boto3
import requests
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# AWS S3 Configuration
AWS_REGION = 'ap-south-1'
AWS_ACCESS_KEY_ID = 'your_access_key'
AWS_SECRET_ACCESS_KEY = 'your_secret_key'
BUCKET_NAME = 'quickads-db'
BUCKET_PREFIX = 'graphics/'

# Initialize S3 client
s3_client = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

total_files = 0  # Total number of files to download and upload
downloaded_files = 0  # Number of successfully processed files

def download_and_upload(line):
    global downloaded_files
    try:
        url = line.strip()  # Remove any leading/trailing whitespace or newlines
        s3_filename = url.split('/')[-1]  # Derive filename from the URL
        
        # Download the file
        print(f'Downloading {url}')
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Upload to S3
        s3_key = f'{BUCKET_PREFIX}{s3_filename}'
        print(f'Uploading {s3_filename} to S3 bucket {BUCKET_NAME}')
        s3_client.upload_fileobj(response.raw, BUCKET_NAME, s3_key)
        print(f'Successfully uploaded {s3_filename} to {s3_key}')

        # Update progress
        downloaded_files += 1
        percentage_complete = (downloaded_files / total_files) * 100
        print(f'Progress: {downloaded_files}/{total_files} files ({percentage_complete:.2f}%) completed.')

    except requests.exceptions.RequestException as e:
        print(f'Error downloading {url}: {e}')


if __name__ == "__main__":
    try:
        script_dir = Path(__file__).parent  # Get the directory of the script
        input_file = script_dir / 'downloadfinal.txt' # Your input file containing the list of URLs and S3 filenames

        # Count total files for progress tracking
        with open(input_file, 'r') as file:
            lines = file.readlines()
            total_files = len(lines)
            # print(total_files)

        if total_files == 0:
            print("The input file is empty.")
            exit()

        # Download and upload files concurrently
        with ThreadPoolExecutor(max_workers=48) as executor:
            executor.map(download_and_upload, lines)

    except Exception as e:
        print(f"An error occurred: {e}")

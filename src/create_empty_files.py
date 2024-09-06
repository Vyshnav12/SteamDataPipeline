import boto3
import subprocess

# Define the bucket name and file names
bucket_name = 'steamscraperbucket'
file_names = [
    'games.json',
    'discarded.json',
    'notreleased.json',
    'applist.json'
]

# Delete all files in the bucket
try:
    subprocess.run(['aws', 's3', 'rm', f's3://{bucket_name}', '--recursive'], check=True)
    print('All files removed successfully')
except subprocess.CalledProcessError as e:
    print(f'Error removing files: {e}')

# Initialize S3 client
s3 = boto3.client('s3')

# Create and upload empty files
for file_name in file_names:
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=b'')
    print(f'Created empty file: {file_name}')

print('All files created successfully')

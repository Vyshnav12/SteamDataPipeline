import json
import boto3
from botocore.exceptions import NoCredentialsError
import logging
import re
import config
import datetime as dt

# Initialize logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

logging.basicConfig(
    level=logging.INFO,  # Adjust to the desired level
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# AWS S3 client
s3_client = boto3.client('s3')

def save_to_s3(bucket_name, key, data):
    '''
    Save data to S3 bucket under the given key.

    :param bucket_name: The name of the S3 bucket.
    :param key: The key under which to save the data.
    :param data: The data to save.
    '''

    try:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(data, indent=4))
        logger.info(f'Successfully saved {key} to S3.')
    except NoCredentialsError:
        logger.error('Credentials not available for S3.')
    except Exception as e:
        logger.error(f'Error saving to S3: {e}')

def load_from_s3(bucket_name, key):
    '''
    Load data from S3 bucket under the given key.

    :param bucket_name: The name of the S3 bucket.
    :param key: The key under which to load the data.
    :return: The loaded data, or None if the key is not present.
    '''
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        data = response['Body'].read().decode('utf-8')
        return json.loads(data)
    except s3_client.exceptions.NoSuchKey:
        return None
    except Exception as e:
        logger.error(f'Error loading from S3: {e}')
        return None

def save_chunk_to_s3(bucket_name, chunk, manifest):
    '''
    Save a chunk of scraped data to S3 and update the manifest accordingly.

    :param bucket_name: The name of the S3 bucket.
    :param chunk: The chunk of scraped data to save.
    :param manifest: The current manifest of chunks.
    :return: The updated manifest.
    '''
    chunk_index = len(manifest['chunks']) + 1
    chunk_key = f'chunk_{chunk_index}.json'
    save_to_s3(bucket_name, chunk_key, chunk)
    manifest['chunks'].append(chunk_key)
    logger.info(f'Successfully saved chunk to {chunk_key}.')
    return manifest

def merge_chunks(bucket_name, output_file):
    '''
    Merge all chunks of scraped data stored in S3 into a single file.

    :param bucket_name: The name of the S3 bucket.
    :param output_file: The key under which to save the merged data.
    '''
    manifest = load_from_s3(bucket_name, 'manifest.json')
    if manifest:
        all_data = {}
        for chunk_key in manifest['chunks']:
            chunk_data = load_from_s3(bucket_name, chunk_key)
            if chunk_data:
                all_data.update(chunk_data)
        save_to_s3(bucket_name, output_file, all_data)
        logger.info(f'Merged all chunks into {output_file}.')
    else:
        logger.error('Manifest file is missing or invalid.')

def SanitizeText(text):
    '''
    Remove HTML tags and excessive whitespace from a given string.
    
    :param text: The string to sanitize.
    :return: The sanitized string.
    '''
    if text:
        text = re.sub('<[^<]+?>', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
    return text

def Log(level, message):
    '''
    Format and log a message.
    
    :param level: Log level (INFO, WARNING, ERROR, EXCEPTION).
    :param message: The message to log.
    '''
    log_levels = {config.INFO: logging.INFO, config.WARNING: logging.WARNING, config.ERROR: logging.ERROR, config.EXCEPTION: logging.ERROR}
    logger = logging.getLogger()

    log_level = log_levels.get(level, logging.INFO)
    logger.log(log_level, f"{config.LOG_ICON[level]} {message}")

def ProgressLog(title, count, total, start_time):
    '''
    Logs progress information.
    
    :param title: Title to display.
    :param count: Current count.
    :param total: Total count.
    :param start_time: Start time for the progress log.
    '''
    elapsed_time = dt.datetime.now() - start_time
    elapsed_str = str(elapsed_time).split('.')[0]  # Exclude milliseconds for simplicity
    percents = round(100.0 * count / float(total), 2)
    
    Log(config.INFO, f"{title} - {percents}% completed ({count}/{total}) - Elapsed time: {elapsed_str}")


def PriceToFloat(price_str):
    '''
    Converts a Steam price string into a float.
    
    :param price_str: The Steam price string to convert.
    :return: The price as a float, or 0.0 if the conversion fails.
    '''
    try:
        return float(re.sub(r'[^\d.]', '', price_str))
    except ValueError:
        return 0.0

# New functions for metadata index management

def load_metadata_index(bucket_name):
    '''
    Loads the metadata index from S3 as a set of appIDs.
    
    :param bucket_name: The name of the S3 bucket to load the metadata index from.
    :return: The set of appIDs in the metadata index, or an empty set if the index is not present.
    '''
    try:
        metadata = load_from_s3(bucket_name, config.METADATA_FILE)
        return set(metadata) if metadata else set()
    except Exception as e:
        logger.error(f'Error loading metadata index: {e}')
        return set()

def save_metadata_index(bucket_name, metadata):
    '''
    Saves the metadata index to S3 as a JSON list of appIDs.
    
    :param bucket_name: The name of the S3 bucket to save the metadata index to.
    :param metadata: The metadata index to save, as a set of appIDs.
    '''
    try:
        save_to_s3(bucket_name, config.METADATA_FILE, list(metadata))
    except Exception as e:
        logger.error(f'Error saving metadata index: {e}')

def update_metadata_index(metadata, chunk):
    '''
    Updates the metadata index with the appIDs from the given chunk.

    :param metadata: The metadata index to update, as a set of appIDs.
    :param chunk: The chunk to update the metadata index with, as a dict.
    :return: The updated metadata index, as a set of appIDs.
    '''
    metadata.update(chunk.keys())
    return metadata

def list_chunk_filenames(bucket_name, prefix='chunks/'):
    '''
    List all chunk filenames in the specified S3 bucket and prefix.
    
    :param bucket_name: The name of the S3 bucket.
    :param prefix: The prefix used for chunk filenames (e.g., 'chunks/').
    :return: List of chunk filenames.
    '''
    s3_client = boto3.client('s3')
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            chunk_files = [obj['Key'] for obj in response['Contents']]
            return chunk_files
        else:
            return []
    except Exception as e:
        Log(config.ERROR, f'Error listing chunk files from S3: {str(e)}')
        return []
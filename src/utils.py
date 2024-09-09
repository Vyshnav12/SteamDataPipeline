import json
import boto3
from botocore.exceptions import NoCredentialsError
import logging
import re
import config
import datetime as dt
import io

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# AWS S3 client
s3_client = boto3.client('s3')

def save_to_s3(bucket_name, key, data):
    try:
        with io.BytesIO(json.dumps(data, indent=4).encode('utf-8')) as file_obj:
            s3_client.upload_fileobj(file_obj, bucket_name, key)
        logger.info(f'Successfully saved {key} to S3.')
    except NoCredentialsError:
        logger.error('Credentials not available for S3.')
    except Exception as e:
        logger.error(f'Error saving to S3: {e}')

def load_from_s3(bucket_name, key):
    try:
        with io.BytesIO() as file_obj:
            s3_client.download_fileobj(bucket_name, key, file_obj)
            return json.loads(file_obj.getvalue().decode('utf-8'))
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
    if manifest and manifest['chunks']:
        all_data = {}
        for chunk_key in manifest['chunks']:
            chunk_data = load_from_s3(bucket_name, chunk_key)
            if chunk_data:
                all_data.update(chunk_data)
        
        if all_data:  # Only save if there's data to save
            save_to_s3(bucket_name, output_file, all_data)
            logger.info(f'Merged {len(manifest["chunks"])} chunk(s) into {output_file}.')
        else:
            logger.warning('No data found in chunks. No merged file created.')
    else:
        logger.warning('No chunks found in manifest. No merged file created.')

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
    log_levels = {config.INFO: logging.INFO, config.WARNING: logging.WARNING, config.ERROR: logging.ERROR, config.EXCEPTION: logging.ERROR}
    logger.log(log_levels.get(level, logging.INFO), f"{config.LOG_ICON[level]} {message}")

def ProgressLog(title, count, total, start_time):
    elapsed_time = dt.datetime.now() - start_time
    elapsed_str = str(elapsed_time).split('.')[0]
    percents = round(100.0 * count / float(total), 2)
    
    Log(config.INFO, f"{title} - {percents:.2f}% completed ({count}/{total}) - Elapsed time: {elapsed_str}")

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

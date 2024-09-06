import datetime as dt
import json
import logging
import boto3
import re
import config

# Set up the logging configuration for AWS EC2
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def load_from_s3(bucket_name, file_key):
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        body = response['Body'].read().decode('utf-8')
        if body.strip():  # Check if the body is not empty
            return json.loads(body)
        else:
            return []  # Return an empty list or dict if the file is empty
    except Exception as e:
        logging.error(f"Error loading from S3: {str(e)}")
        return []

def save_to_s3(bucket_name, file_key, data):
    # Convert the data to JSON and upload to S3
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket_name, Key=file_key, Body=json.dumps(data, indent=4))

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

def SanitizeText(text):
    '''
    Removes HTML codes, escape codes and URLs.
    '''
    if text is None:
        return ''  # Return an empty string if text is None

    text = text.replace('\n\r', ' ')
    text = text.replace('\r\n', ' ')
    text = text.replace('\r \n', ' ')
    text = text.replace('\r', ' ')
    text = text.replace('\n', ' ')
    text = text.replace('\t', ' ')
    text = text.replace('&quot;', "'")
    text = re.sub(r'(https|http)?:\/\/(\w|\.|\/|\?|\=|\&|\%)*\b', '', text, flags=re.MULTILINE)
    text = re.sub('<[^<]+?>', ' ', text)
    text = re.sub(' +', ' ', text)
    text = text.lstrip(' ')

    return text

def PriceToFloat(price, decimals=2):
  '''
  Price in text to float. Use locate?
  '''
  price = price.replace(',', '.')

  return round(float(re.findall('([0-9]+[,.]+[0-9]+)', price)[0]), decimals)
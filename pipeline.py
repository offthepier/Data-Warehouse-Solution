import requests
import pandas as pd
import sqlite3
import getpass
import time
from dotenv import load_dotenv
import os
import logging
import sys
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from cryptography.fernet import Fernet

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to get environment variables with error handling
def get_env_variable(var_name):
    value = os.getenv(var_name)
    if not value:
        logging.error(f"Environment variable {var_name} is not set.")
        sys.exit(1)
    return value

# Function to validate URLs
def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

# Load environment variables from .env file
load_dotenv('pipeline.env')

# Prompt user to enter file URLs
urls = input("Enter the file URLs (separated by commas): ").strip().split(',')
csv_urls = [url.strip() for url in urls if url.strip()]

# Validate CSV URLs
validated_urls = [url for url in csv_urls if is_valid_url(url)]
if len(validated_urls) != len(csv_urls):
    logging.error("One or more provided URLs are invalid.")
    sys.exit(1)

# Get environment variables with error handling
dremio_url = get_env_variable('DREMIO_URL')
username = get_env_variable('DREMIO_USERNAME')
password = get_env_variable('DREMIO_PASSWORD')
source = get_env_variable('DREMIO_SOURCE')

chunk_size = 50 * 1024 * 1024  # 50MB chunk size (change as needed)

# Authenticate and get token
try:
    auth_response = requests.post(f'{dremio_url}/apiv2/login', json={'userName': username, 'password': password})
    auth_response.raise_for_status()
    auth_token = auth_response.json().get('token')
except requests.exceptions.RequestException as e:
    logging.error(f"Error occurred while making a request: {str(e)}")
    sys.exit(1)

# Headers for authenticated requests
headers = {
    'Authorization': f'_dremio{auth_token}',
    'Content-Type': 'application/json'
}

# Function to generate encryption key
encryption_key = Fernet.generate_key()
cipher_suite = Fernet(encryption_key)

# Function to encrypt sensitive data
def encrypt_data(df, sensitive_columns):
    for column in sensitive_columns:
        if column in df.columns:
            logging.info(f"Encrypting column: {column}")
            df[column] = df[column].apply(lambda x: cipher_suite.encrypt(str(x).encode()).decode())
        else:
            logging.warning(f"Column {column} not found in data.")
    return df

# Retry mechanism for network requests
def download_file_with_retry(url, retries=3, delay=2):
    attempt = 0
    while attempt < retries:
        try:
            logging.info(f"Attempting to download file: {url} (Attempt {attempt + 1}/{retries})")
            response = requests.get(url)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            attempt += 1
            logging.error(f"Attempt {attempt} failed for {url}: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
    logging.error(f"Failed to download the file {url} after {retries} attempts.")
    sys.exit(1)

# Function to sanitize table names
def sanitize_table_name(file_name):
    sanitized = re.sub(r'[^A-Za-z0-9]+', '_', file_name).lstrip('_')
    return sanitized

# Function to filter out unsupported SQL commands
def filter_sql_commands(commands):
    supported_commands = [
        "CREATE", "INSERT", "DELETE", "UPDATE", "DROP", "ALTER", "TRUNCATE", "SELECT", "VALUES"
    ]
    return [cmd for cmd in commands if any(cmd.strip().upper().startswith(sc) for sc in supported_commands)]

# Mapping of SQLite types to Dremio types
type_mapping = {
    "INTEGER": "INT",
    "TEXT": "VARCHAR",
    "REAL": "FLOAT",
    "BLOB": "VARBINARY",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "FLOAT": "FLOAT",
    "DECIMAL": "DECIMAL",
    "DOUBLE": "DOUBLE",
    "INTERVAL": "INTERVAL",
    "BIGINT": "BIGINT",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP"
}

def convert_sqlite_to_dremio(sql_commands):
    converted_commands = []
    for command in sql_commands:
        for sqlite_type, dremio_type in type_mapping.items():
            command = command.replace(sqlite_type, dremio_type)
        converted_commands.append(command)
    return converted_commands

# Function to send SQL command to Dremio
def send_sql_command(command):
    try:
        sql_response = requests.post(f'{dremio_url}/api/v3/sql', headers=headers, json={'sql': command})
        sql_response.raise_for_status()
        logging.info(f'Executed SQL command: {sql_response.status_code}')
    except requests.exceptions.RequestException as e:
        logging.error(f'Failed to execute SQL command: {str(e)}')
        logging.error(f'Response content: {sql_response.content}')
        sys.exit(1)
    time.sleep(5)

# Function to combine INSERT commands into a single statement
def combine_insert_commands(insert_commands):
    if not insert_commands:
        return None
    
    base_insert = insert_commands[0].split(' VALUES', 1)[0]  # Get the INSERT part
    combined_values = [cmd.split(' VALUES', 1)[1].strip().rstrip(';') for cmd in insert_commands]
    return f'{base_insert} VALUES {", ".join(combined_values)};'

# Function to send accumulated SQL commands in chunks
def send_sql_in_chunks(filtered_commands, max_chunk_size):
    chunk = []
    chunk_size = 0
    for command in filtered_commands:
        command_size = len(command.encode('utf-8'))
        if chunk_size + command_size > max_chunk_size:
            combined_insert_command = combine_insert_commands(chunk)
            if combined_insert_command:
                send_sql_command(combined_insert_command)
            chunk = []
            chunk_size = 0
        
        chunk.append(command)
        chunk_size += command_size

    if chunk:
        combined_insert_command = combine_insert_commands(chunk)
        if combined_insert_command:
            send_sql_command(combined_insert_command)

# Parallel processing example logging
def process_file(url, sensitive_columns):
    logging.info(f"Starting to process file: {url}")
    try:
        response = download_file_with_retry(url)
        file_name = url.split('/')[-1]
        table_name = sanitize_table_name(file_name.split('.')[0])

        # Read CSV in chunks
        for chunk in pd.read_csv(url, chunksize=1000):  # Adjust the chunksize as needed
            # Encrypt sensitive data
            chunk = encrypt_data(chunk, sensitive_columns)

            conn = sqlite3.connect(':memory:')
            chunk.to_sql(table_name, conn, if_exists='replace', index=False)

            sql_commands = [line for line in conn.iterdump()]
            conn.close()

            sql_commands = convert_sqlite_to_dremio(sql_commands)
            filtered_commands = filter_sql_commands(sql_commands)

            create_table_command = None
            insert_commands = []
            for command in filtered_commands:
                if command.strip().upper().startswith("CREATE TABLE"):
                    create_table_command = command
                else:
                    insert_commands.append(command)

            full_table_path = f'"{source}"."{table_name}"'
            if create_table_command:
                create_table_command = create_table_command.replace(f'"{table_name}"', full_table_path)
                send_sql_command(create_table_command)

            insert_commands = [cmd.replace(f'"{table_name}"', full_table_path) for cmd in insert_commands]
            send_sql_in_chunks(insert_commands, chunk_size)

        logging.info(f"Completed processing file: {url}")
    except Exception as e:
        logging.error(f"Error processing file {url}: {str(e)}")
        sys.exit(1)

# Prompt user to enter sensitive columns
sensitive_columns = input("Enter sensitive columns to encrypt (comma separated): ").strip().split(',')

# ThreadPoolExecutor for parallel processing
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(process_file, url, sensitive_columns) for url in validated_urls]

    for future in as_completed(futures):
        future.result()  # Raises any exception that occurred during file processing

logging.info("Script completed")
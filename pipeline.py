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

# Set up logging for the script. This helps track the progress and errors.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to fetch environment variables safely, with error handling in case they are missing.
def get_env_variable(var_name):
    value = os.getenv(var_name)
    if not value:
        logging.error(f"Environment variable {var_name} is not set.")
        sys.exit(1)
    return value

# Function to validate the given URLs by ensuring the scheme is http/https and netloc is present.
def is_valid_url(url):
    try:
        result = urlparse(url)
        # Ensure that the scheme is either http or https
        return result.scheme in ['http', 'https'] and all([result.netloc])
    except ValueError:
        return False

# Load environment variables from a .env file. This file contains sensitive information like database credentials.
load_dotenv('pipeline.env')

# Prompt the user to enter the file URLs they wish to process.
urls = input("Enter the file URLs (separated by commas): ").strip().split(',')
csv_urls = [url.strip() for url in urls if url.strip()]  # Clean and prepare a list of URLs.

# Validate each provided URL to ensure it's well-formed.
validated_urls = [url for url in csv_urls if is_valid_url(url)]
if len(validated_urls) != len(csv_urls):
    logging.error("One or more provided URLs are invalid.")
    sys.exit(1)

# Retrieve necessary environment variables for connecting to the Dremio server.
dremio_url = get_env_variable('DREMIO_URL')
username = get_env_variable('DREMIO_USERNAME')
password = get_env_variable('DREMIO_PASSWORD')
source = get_env_variable('DREMIO_SOURCE')

chunk_size = 50 * 1024 * 1024  # Set the size of data chunks to 50MB for processing large files.

# Authenticate with Dremio to get a session token. This token is used for making API requests.
try:
    auth_response = requests.post(f'{dremio_url}/apiv2/login', json={'userName': username, 'password': password})
    auth_response.raise_for_status()
    auth_token = auth_response.json().get('token')
except requests.exceptions.RequestException as e:
    logging.error(f"Error occurred while making a request: {str(e)}")
    sys.exit(1)

# Prepare the headers for authenticated API requests using the token we just received.
headers = {
    'Authorization': f'_dremio{auth_token}',
    'Content-Type': 'application/json'
}

# Generate a key for encrypting sensitive data and set up the encryption tool.
encryption_key = Fernet.generate_key()
cipher_suite = Fernet(encryption_key)

# Function to encrypt sensitive columns in the data frame.
def encrypt_data(df, sensitive_columns):
    # For each specified sensitive column, encrypt the data if the column exists in the dataframe.
    for column in sensitive_columns:
        if column in df.columns:
            logging.info(f"Encrypting column: {column}")
            df[column] = df[column].apply(lambda x: cipher_suite.encrypt(str(x).encode()).decode())  # Encrypt each cell
        else:
            logging.warning(f"Column {column} not found in data.")  # Log a warning if the column is missing.
    return df

# Retry mechanism for downloading files in case of network errors. This helps prevent failures due to transient issues.
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

# Function to sanitize table names, removing any special characters to ensure compatibility with SQL.
def sanitize_table_name(file_name):
    # Remove the file extension first, then sanitize the name
    file_name_without_extension = re.sub(r'\.[^.]+$', '', file_name)
    sanitized = re.sub(r'[^A-Za-z0-9]+', '_', file_name_without_extension).lstrip('_')
    return sanitized

# Filter SQL commands to include only the ones that are supported by the Dremio SQL engine.
def filter_sql_commands(commands):
    supported_commands = [
        "CREATE", "INSERT", "DELETE", "UPDATE", "DROP", "ALTER", "TRUNCATE", "SELECT", "VALUES"
    ]
    return [cmd for cmd in commands if any(cmd.strip().upper().startswith(sc) for sc in supported_commands)]

# Mapping SQLite types to Dremio SQL types for compatibility during SQL command conversion.
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

# Convert SQL commands from SQLite syntax to Dremio-compatible syntax by replacing types.
def convert_sqlite_to_dremio(sql_commands):
    converted_commands = []
    for command in sql_commands:
        for sqlite_type, dremio_type in type_mapping.items():
            command = command.replace(sqlite_type, dremio_type)
        converted_commands.append(command)
    return converted_commands

# Function to send an SQL command to the Dremio API.
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

# Combine multiple INSERT commands into one to optimize performance when sending large amounts of data to Dremio.
def combine_insert_commands(insert_commands):
    if not insert_commands:
        return None
    
    # Combine the values of all insert commands into a single SQL statement.
    base_insert = insert_commands[0].split(' VALUES', 1)[0]  # Get the common INSERT part
    combined_values = [cmd.split(' VALUES', 1)[1].strip().rstrip(';') for cmd in insert_commands]  # Combine the values
    return f'{base_insert} VALUES {", ".join(combined_values)};'

# Function to send the accumulated SQL commands in chunks, ensuring the request doesn't exceed the max_chunk_size.
def send_sql_in_chunks(filtered_commands, max_chunk_size):
    chunk = []
    chunk_size = 0
    for command in filtered_commands:
        command_size = len(command.encode('utf-8'))  # Calculate the size of the current command
        if chunk_size + command_size > max_chunk_size:
            combined_insert_command = combine_insert_commands(chunk)
            if combined_insert_command:
                send_sql_command(combined_insert_command)
            chunk = []
            chunk_size = 0
        
        chunk.append(command)  # Add command to the current chunk
        chunk_size += command_size  # Update the chunk size

    if chunk:  # Send any remaining commands in the last chunk.
        combined_insert_command = combine_insert_commands(chunk)
        if combined_insert_command:
            send_sql_command(combined_insert_command)

# Function to process user input for each file.
def get_user_input_for_files(url):
    # Prompt the user to decide whether or not to anonymize each file.
    print(f"Do you want to anonymize the data for {url.split('/')[-1]}? (yes/no): ", end="")
    anonymize = input().strip().lower() == "yes"
    sensitive_columns = []
    if anonymize:
        sensitive_columns = input(f"Enter sensitive columns to encrypt for {url.split('/')[-1]} (comma separated): ").strip().split(',')
    return anonymize, sensitive_columns

# Function to process the file: download, encrypt (if needed), and upload to Dremio.
def process_file(url, sensitive_columns):
    logging.info(f"Starting to process file: {url}")
    try:
        # Download the file with retry mechanism.
        response = download_file_with_retry(url)
        file_name = url.split('/')[-1]
        table_name = sanitize_table_name(file_name.split('.')[0])  # Sanitize the filename for table name.

        # Read the CSV file in chunks to handle large files efficiently.
        for chunk in pd.read_csv(url, chunksize=1000):
           
            # If sensitive columns were specified, encrypt them.
            if sensitive_columns:
                chunk = encrypt_data(chunk, sensitive_columns)

            # Create an in-memory SQLite database to store the chunk of data.
            conn = sqlite3.connect(':memory:')
            chunk.to_sql(table_name, conn, if_exists='replace', index=False)

            # Convert the SQLite dump to SQL commands.
            sql_commands = [line for line in conn.iterdump()]
            conn.close()

            # Convert SQLite SQL commands to Dremio-compatible SQL commands.
            sql_commands = convert_sqlite_to_dremio(sql_commands)
            
            # Filter the SQL commands to ensure only valid ones are sent.
            filtered_commands = filter_sql_commands(sql_commands)

            # Separate CREATE TABLE and INSERT commands.
            create_table_command = None
            insert_commands = []
            for command in filtered_commands:
                if command.strip().upper().startswith("CREATE TABLE"):
                    create_table_command = command
                else:
                    insert_commands.append(command)

            # Full table path including the source for Dremio.
            full_table_path = f'"{source}"."{table_name}"'

            # Send CREATE TABLE command to Dremio.
            if create_table_command:
                create_table_command = create_table_command.replace(f'"{table_name}"', full_table_path)
                send_sql_command(create_table_command)

            # Send INSERT commands to Dremio in chunks to optimize performance.
            insert_commands = [cmd.replace(f'"{table_name}"', full_table_path) for cmd in insert_commands]
            send_sql_in_chunks(insert_commands, chunk_size)

        logging.info(f"Completed processing file: {url}")
    except Exception as e:
        logging.error(f"Error processing file {url}: {str(e)}")
        sys.exit(1)

# Main function to handle the processing of multiple files concurrently.
def main():
    # ThreadPoolExecutor allows us to download and process multiple files concurrently.
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []

        # For each URL, get user input for anonymization and sensitive columns.
        for url in validated_urls:
            anonymize, sensitive_columns = get_user_input_for_files(url)
            futures.append(executor.submit(process_file, url, sensitive_columns))

        # Process all submitted tasks and raise exceptions if any errors occur.
        for future in as_completed(futures):
            future.result()  # This will raise any exception that occurred during file processing.

    logging.info("Script completed")

if __name__ == '__main__':
    main()

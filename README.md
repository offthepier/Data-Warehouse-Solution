Redback-Operations Data Pipeline

Overview
This project implements a data pipeline for processing and uploading files to Dremio. The pipeline is designed to handle multiple file formats, anonymize sensitive data, and improve efficiency with parallel processing and retry mechanisms. This tool is ideal for securely handling and transforming large datasets while ensuring data integrity during uploads.

Features
Data Anonymization: Encrypt sensitive columns in your datasets using a user-specified encryption algorithm (Fernet).
Retry Mechanism: Automatically retries downloading a file up to three times in case of network failures.
Parallel Processing: Utilizes ThreadPoolExecutor to process multiple files simultaneously, improving performance.
User Input for File URLs: Users can provide multiple file URLs for processing at runtime.
User Input for Sensitive Columns: Users can specify which columns to anonymize during execution.

Installation

Clone the repository:

git clone https://github.com/offthepier/Redback-Operations.git
cd Redback-Operations

Set up a virtual environment (optional but recommended):

python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

Install the required dependencies:

pip install -r requirements.txt

reate a .env file: Copy the .env.example file and fill in your Dremio credentials:

cp .env.example .env

Then, update the .env file with your actual credentials:

plaintext
DREMIO_URL=http://your-dremio-url:9047
DREMIO_USERNAME=your_dremio_username
DREMIO_PASSWORD=your_dremio_password
DREMIO_SOURCE=your_dremio_source

Usage

To run the pipeline, use the following command:

python pipeline.py

You will be prompted to:

Enter the file URLs (comma-separated) you wish to process.
Specify whether you want to anonymize data for each file.
If anonymization is required, you'll be prompted to specify which columns to encrypt.
Example:
Enter the file URLs (separated by commas): https://example.com/file1.csv, https://example.com/file2.xlsx
Do you want to anonymize data for file1.csv? (yes/no): yes
Enter sensitive columns to encrypt (comma separated): SSN, CreditCardNumber
Do you want to anonymize data for file2.xlsx? (yes/no): no

Configuration

All the necessary environment variables should be stored in the .env file.

DREMIO_URL: The URL of your Dremio instance (e.g., http://your-dremio-url:9047)
DREMIO_USERNAME: Your Dremio username.
DREMIO_PASSWORD: Your Dremio password.
DREMIO_SOURCE: The source location in Dremio (e.g., project name).

File Formats Supported
CSV: Files with .csv extension are fully supported.
XLSX: Excel files are supported, and the pipeline will automatically detect and process them.
RAW GitHub Links: The pipeline handles GitHub raw file links directly.
Note: If you provide a GitHub link (non-raw), the pipeline automatically converts it into a raw link for processing.

Examples
Here are some example URLs to process:

CSV file:
https://github.com/username/repo/blob/main/data/sample.csv
XLSX file:
https://github.com/username/repo/blob/main/data/sample.xlsx

Error Handling
If a download fails due to a network issue, the pipeline will retry three times before failing.
If a column specified for encryption is not found, the pipeline will log a warning and continue processing.

# connector & and upload_to_blob.py
from azure.storage.blob import BlobServiceClient
# from src.configuration.configuration import connect_str, container_name_blob
import pandas as pd
from io import StringIO
from dotenv import load_dotenv
import os
from src.configuration.configuration import STATUS_FILE
from datetime import datetime
from azure.core.exceptions import ResourceNotFoundError
import logging

load_dotenv()

connect_str = os.getenv('connect_str')
container_name_blob = os.getenv('container_name_blob')

def upload_to_blob(csv_data, actual_date_str):
    date_object = datetime.strptime(actual_date_str, '%Y-%m-%d')
    year = date_object.year
    month = date_object.month

    # We want all the csv_data corresponding to a month to be on one file in the name format year-month.csv. eg: 2024-10.csv
    file_name = str(year) + '-' + str(month) + 'page2.csv'
    print(f"File name to upload: {file_name}")

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    print("Connected to Azure Blob Storage")

    # Create a container client
    container_client = blob_service_client.get_container_client(container= container_name_blob) 
    print(f"Created container client for container: {container_name_blob}")

    # Download the current csv data if it exists
    try:
        existing_csv_data = container_client.download_blob(file_name, encoding='UTF-8').readall()
    except ResourceNotFoundError:
        existing_csv_data = ''

    if existing_csv_data != '':
            existing_csv_data = existing_csv_data.split('\n', maxsplit=1)[1] # remove header row from existing csv data
    # Append existing csv data to the new csv data
    csv_data += existing_csv_data

    # Upload the new CSV data to the blob
    blob_client = blob_service_client.get_blob_client(container=container_name_blob, blob=file_name)
    blob_client.upload_blob(csv_data.encode('utf-8'), overwrite=True)
    print(f"Uploaded {file_name} to Azure Blob Storage")


def download_processed_pdfs():
    """Downloads processed pdf link tracker from Azure blob and return it as a string 
    """
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container= container_name_blob) 
    status_file_string = container_client.download_blob(STATUS_FILE, encoding='UTF-8').readall()
    return status_file_string

def upload_processed_pdfs(file_as_string):
    """Overwrites given string to processed pdf link tracker in Azure blob. 
    """
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container= container_name_blob) 
    blob_client = container_client.upload_blob(name=STATUS_FILE, data=file_as_string, overwrite=True)



# Old logic commented out

# from azure.storage.blob import BlobServiceClient
# from src.configuration.configuration import connect_str, container_name_blob

# def upload_to_blob(csv_data, actual_date_str):
    
#         # Create a filename with the actual date
#         file_name = f"Harti_data_page_2_{actual_date_str}.csv"
#         print(f"File name to upload: {file_name}")

#         blob_service_client = BlobServiceClient.from_connection_string(connect_str)
#         print("Connected to Azure Blob Storage")

#         # Create a blob client
#         blob_client = blob_service_client.get_blob_client(container=container_name_blob, blob=file_name)
#         print(f"Created blob client for container: {container_name_blob}")

#         # Upload the new CSV data to the blob
#         blob_client.upload_blob(csv_data.encode('utf-8'), overwrite=True)
#         print(f"Uploaded {file_name} to Azure Blob Storage")

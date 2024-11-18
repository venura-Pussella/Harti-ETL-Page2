# main.py
import logging.handlers
import os
import asyncio
import platform
import itertools
import warnings
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import logging # for use in Azure functions environment (replace all calls to logger object with python logging class)
from src import logHandling
from src.logHandling import log_messages
import pdfminer.pdfparser
# from src.localLogging import logger
import pdfminer
from src.utils.log_utils import send_log
from src.connector.cosmos_db import write_harti_data_to_cosmosdb
from src.configuration.configuration import pdf_source_url, metadata_line1
from src.pipeline2.meta_data_checker import find_line_with_metadata
from src.pipeline2.get_pdfdata import (
    get_latest_pdf_link,
    download_pdf_as_bytes,
    extract_text_from_page1,
    extract_text_from_page2
)
from src.pipeline2.extract_table_from_pdf_to_df import (
    extract_tables_from_pdf_to_df_page_1,
    extract_tables_from_pdf_to_df_page_2
)
from src.pipeline2.cleaning_column_values import clean_dataframe
from src.pipeline2.data_transformation import (
    rename_columns_before_dot,
    transform_food_df,
    rename_first_column,
    update_item_names,
    split_and_convert_value_column,
    insert_database_write_date,
    drop_rows_with_missing_values_in_value_column,preprocess_dataframe,convert_dates,add_page_number_column
)
from src.pipeline2.data_format_converter import (
    dataframe_to_csv_string,convert_dataframe_to_cosmos_format)

from src.connector.blob import upload_to_blob, upload_processed_pdfs, download_processed_pdfs, update_logs

warnings.filterwarnings('ignore')

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv()) # read local .env file
from src.configuration.configuration import WEB_SOURCE

container_name = os.getenv('container_name_blob')
az_blob_conn_str = os.getenv('connect_str')



def load_processed_pdfs(status_file_string: str):
    """Expects a string consisting of pdf_link lines, returns it as a set
    """
    mySet = set()
    status_file_lines = status_file_string.rsplit('\n')
    for line in status_file_lines:
        mySet.add(line.strip())
    return mySet

def load_procssed_pdfs_as_list(status_file_string: str) -> list[str]:
    """Expects a string consisting of pdf_link lines, returns it as a reversed list (so firt index is typically the oldest link from the Harti website)
    """
    list = []
    status_file_lines = status_file_string.rsplit('\n')
    for line in status_file_lines:
        list.append(line.strip())
    list.reverse()
    return list

def get_all_pdf_links(pdf_source):
    response = requests.get(pdf_source)
    soup = BeautifulSoup(response.content, 'html.parser')
    pdf_links = soup.find_all('a', href=True)
    
    all_pdf_links: list[str] = []
    for link in pdf_links:
        if '.pdf' in link['href']:
            full_link = urljoin(pdf_source, link['href'])
            all_pdf_links.append(full_link.strip())
    
    return all_pdf_links

async def process_pdf(pdf_link):
    try:
        logging.info(">>>> Starting the data extraction process <<<<")

        # Download the latest PDF
        # latest_pdf_link = get_latest_pdf_link(pdf_source_url)

        pdf_bytes = download_pdf_as_bytes(pdf_link)
        
        # Extract text from both pages
        extracted_text_1 = extract_text_from_page1(pdf_bytes)
        extracted_text_2 = extract_text_from_page2(pdf_bytes)
        
        # Split text into lines
        extracted_lines_1 = [text.split('\n') for text in extracted_text_1]
        extracted_lines_2 = [text.split('\n') for text in extracted_text_2]

        # Flatten the list of lists into a single list of lines
        flattened_extracted_lines_1 = list(itertools.chain.from_iterable(extracted_lines_1))
        flattened_extracted_lines_2 = list(itertools.chain.from_iterable(extracted_lines_2))

        # Check metadata in Page 1
        if find_line_with_metadata(flattened_extracted_lines_1, metadata_line1):
            
            logging.info(">>>> Metadata line found on Page 1, proceeding with data extraction from Page 1... <<<<")
            food_df = extract_tables_from_pdf_to_df_page_1(pdf_bytes)
        
        # Check metadata in Page 2 if not found in Page 1
        elif find_line_with_metadata(flattened_extracted_lines_2, metadata_line1):

            logging.info(">>>> Metadata line is not found on Page 2, proceeding with data extraction from Page 2... <<<<")
            food_df = extract_tables_from_pdf_to_df_page_2(pdf_bytes)
        
        else:
            logging.error(">>>> Metadata line not found on either page. Aborting data extraction. <<<<")
            return
        
        # Data transformation
        logging.info(">>>> Staring data transformation <<<<")

        food_df = clean_dataframe(food_df)
        food_df = rename_columns_before_dot(food_df)
        food_df = rename_first_column(food_df)
        food_df = preprocess_dataframe(food_df)
        food_df = convert_dates(food_df)
        food_df = transform_food_df(food_df)
        food_df = update_item_names(food_df)
        food_df = split_and_convert_value_column(food_df)
        food_df = insert_database_write_date(food_df)
        food_df = drop_rows_with_missing_values_in_value_column(food_df)
        food_df = add_page_number_column(food_df)
        # food_df.to_csv('food.csv', index=False)
        logging.info(">>>> Data transformation finished <<<<")

        # Save the DataFrame to a CSV in blob storage
        csv_data,actual_date_str = dataframe_to_csv_string(food_df)
        upload_to_blob(csv_data,actual_date_str)
        logging.info(">>>> Uploaded CSV to blob storage <<<<")

        # Save the Data to cosmos db
        logging.info(">>>> Saving the Data to cosmos db format <<<<")
        cosmos_data = convert_dataframe_to_cosmos_format(food_df)
        await write_harti_data_to_cosmosdb(cosmos_data)
        logging.info(">>>> Data Ingested to CosmosDB <<<<")

        # # Send success log
        # send_log(
        #     service_type="Azure Functions",
        #     application_name="Harti Food Price Collector Page 2",
        #     project_name="Harti Food Price Prediction",
        #     project_sub_name="Food Price History",
        #     azure_hosting_name="AI Services",
        #     developmental_language="Python",
        #     description="Sri Lanka Food Prices - Azure Functions",
        #     created_by="BrownsAIsevice",
        #     log_print="Successfully completed data ingestion to Cosmos DB.",
        #     running_within_minutes=1440,
        #     error_id=0
        #     )
        # logging.info("Sent success log to function monitoring service.")

        # logging.info(f">>>> {pdf_link} <<<<")

    except Exception as e:
        logging.error(f"Error processing PDF {pdf_link}: {e}")

        # # Send error log
        # send_log(
        #     service_type="Azure Functions",
        #     application_name="Harti Food Price Collector Page 2",
        #     project_name="Harti Food Price Prediction",
        #     project_sub_name="Food Price History",
        #     azure_hosting_name="AI Services",
        #     developmental_language="Python",
        #     description="Sri Lanka Food Prices - Azure Functions",
        #     created_by="BrownsAIsevice",
        #     log_print="An error occurred: " + str(e),
        #     running_within_minutes=1440,
        #     error_id=1,
        #     )
        
        # logging.error("Sent error log to function monitoring service.")

        # # raise     

async def main():
    try:

        # Get all links from Harti website
        pdf_links = get_all_pdf_links(pdf_source_url)
        if not pdf_links:
            logging.warning("No PDF links found.")
            return

        # Load already processed PDFs
        status_file_string = download_processed_pdfs()
        processed_pdfs = load_processed_pdfs(status_file_string) # set
        processed_pdfs_list = load_procssed_pdfs_as_list(status_file_string) # list

        # Loop through each PDF link and process it
        for pdf_link in reversed(pdf_links): # loop thru list of pdf links, starting from oldest first
            if pdf_link not in processed_pdfs: # this operation is fast cuz processed_pdfs is a set
                logging.info(f"New PDF link: {pdf_link}")
                try:
                    await process_pdf(pdf_link)   
                except pdfminer.pdfparser.PDFSyntaxError:
                    logging.error(f"PDF Syntax Error{pdf_link}")
                processed_pdfs_list.append(pdf_link)  
            else:
                logging.info(f"Skipping already processed PDF link: {pdf_link}")

        logging.info(">>>> Data extraction process completed <<<<")

        # update processed pdf tracker file in blob (which has now processed all pdf links)
        processed_pdfs_string = ''
        for link in reversed(processed_pdfs_list): # using the list here is computationally efficient, and makes the processed pdf tracker sorted in the Harti website order, making it easier for the user to review and manipulate
            processed_pdfs_string += link
            processed_pdfs_string += '\n'
        upload_processed_pdfs(processed_pdfs_string)
        logging.info(">>>> Processed PDF Tracker uploaded to blob <<<<")

    except Exception as e:
        logging.error(f"Error in main execution: {e}")
        raise
    
def run_main():

    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    # asyncio.run(main())
    try:
        loop = asyncio.get_event_loop()
        
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    try: update_logs(log_messages)
    except Exception as e: logging.ERROR(f'Exception when updating logs: {e}')


run_main()

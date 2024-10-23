import io
import requests
import pdfplumber
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def get_latest_pdf_link(pdf_source): 
    try:
        response = requests.get(pdf_source)
        response.raise_for_status()  # Raise an error for bad responses
        soup = BeautifulSoup(response.content, 'html.parser')
        pdf_links = soup.find_all('a', href=True)
        
        for link in pdf_links:
            if '.pdf' in link['href']:
                return urljoin(pdf_source, link['href'])  # Construct the full URL
        
        return None  # Return None if no PDF link is found

    except requests.RequestException as e:
        print(f"An error occurred while fetching the PDF source: {e}")
        return None

def download_pdf_as_bytes(pdf_url):
    try:
        response = requests.get(pdf_url)
        response.raise_for_status()  # Raise an error for bad responses
        return io.BytesIO(response.content)
        
    except requests.RequestException as e:
        print(f"An error occurred while downloading the PDF: {e}")
        return None

def extract_text_from_page1(pdf_data):

    extracted_texts = []
    
    try:
        with pdfplumber.open(pdf_data) as pdf:
            page_text = pdf.pages[0].extract_text()
            extracted_texts.append(page_text)
    
    except Exception as e:
        print(f"An error occurred while extracting text from PDF: {e}")
        return None

    return extracted_texts

def extract_text_from_page2(pdf_data):

    extracted_texts = []

    try:
        with pdfplumber.open(pdf_data) as pdf:
            page_text = pdf.pages[1].extract_text()
            extracted_texts.append(page_text)

    except Exception as e:
        print(f"An error occurred while extracting text from PDF: {e}")
        return None

    return extracted_texts

if __name__ == "__main__":

    pdf_source = 'https://www.harti.gov.lk/index.php/en/market-information/data-food-commodities-bulletin' 

    link = get_latest_pdf_link(pdf_source)

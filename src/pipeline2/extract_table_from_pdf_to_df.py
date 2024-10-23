import tabula
import pandas as pd

def extract_tables_from_pdf_to_df_page_1(pdf_bytes_io):
        
        # Pass the BytesIO object to tabula.read_pdf
        tables = tabula.read_pdf(pdf_bytes_io,pages='1',lattice=True)
        
        # Combine all tables into a single DataFrame if there are multiple tables
        if isinstance(tables, list):
            combined_df = pd.concat(tables, ignore_index=True)
        else:
            combined_df = tables

        # print(tabulate(combined_df, headers='keys', tablefmt='psql'))
        return combined_df

def extract_tables_from_pdf_to_df_page_2(pdf_bytes_io):
        
        # Pass the BytesIO object to tabula.read_pdf
        tables = tabula.read_pdf(pdf_bytes_io,pages='2',lattice=True)
        
        # Combine all tables into a single DataFrame if there are multiple tables
        if isinstance(tables, list):
            combined_df = pd.concat(tables, ignore_index=True)
        else:
            combined_df = tables

        # print(tabulate(combined_df, headers='keys', tablefmt='psql'))
        return combined_df
    

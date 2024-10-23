# csv_data.py
import uuid
import pandas as pd
from io import StringIO

def dataframe_to_csv_string(df):
    # Convert the DataFrame to a StringIO object
    output = StringIO()
    df.to_csv(output, index=False)
    output.seek(0)  # Rewind the StringIO object to start reading from the beginning

    # Extract the date from the first row of the Date column and convert it to the required format
    first_row_date = pd.to_datetime(df['Date'].iloc[0], dayfirst=True)
    formatted_date = first_row_date.strftime('%Y-%m-%d')

    # Return the StringIO object and the formatted date
    stringio_data = output.getvalue()
    
    return stringio_data, formatted_date
    
def convert_dataframe_to_cosmos_format(df):

    cosmos_db_hartidata = []
    
    required_columns = ['Database Write Date', 'Date', 'Location','Page', 'Item_Names', 'Value', 'Min_Value', 'Max_Value']
    
    # Check if all required columns are present
    if not all(col in df.columns for col in required_columns):
        raise KeyError(f"One or more required columns are missing: {required_columns}")
    
    for _, row in df.iterrows():
  
        rate_document = {
             
            "id": str(uuid.uuid4()),
            "date": row['Date'].isoformat() if isinstance(row['Date'], pd.Timestamp) else row['Date'],
            "database_write_date": row['Database Write Date'].isoformat() if isinstance(row['Database Write Date'], pd.Timestamp) else row['Database Write Date'],
            "location": row['Location'],
            "page": row['Page'],
            "item_names": row['Item_Names'],
            "value": row['Value'],
            "min_value": row['Min_Value'],
            "max_value": row['Max_Value'],
        }

        cosmos_db_hartidata.append(rate_document)
        
    return cosmos_db_hartidata
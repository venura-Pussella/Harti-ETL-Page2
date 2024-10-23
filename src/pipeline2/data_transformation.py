import re
import pandas as pd
from datetime import datetime
import numpy as np

def rename_columns_before_dot(df):
    """
    Renames columns in the DataFrame by extracting the part before the first dot.
    
    Parameters:
    - df: The DataFrame whose columns need renaming.
    
    Returns:
    - The DataFrame with renamed columns.
    """
    # Identify columns with "."
    columns_with_dot = [col for col in df.columns if '.' in col]

    # Extract column names before "." and rename columns
    new_column_names = {}
    for col in columns_with_dot:
        new_col_name = col.split('.')[0]  # Get part before "."
        new_column_names[col] = new_col_name

    # Rename columns
    df.rename(columns=new_column_names, inplace=True)
    
    return df

def rename_first_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames the first column of the DataFrame to 'Location'.
    
    Parameters:
        df (pd.DataFrame): The DataFrame to be modified.
    
    Returns:
        pd.DataFrame: The DataFrame with the first column renamed.
    """
    # Rename the first column
    df.iloc[0, 0] = 'Location'
    
    return df
    
def preprocess_dataframe(df):
    # Transpose the DataFrame
    df_transposed = df.T
    
    # Set the first row as column headers
    df_transposed.columns = df_transposed.iloc[0]
    
    # Drop the first row (which is now redundant as column headers) and reset index
    df_transposed = df_transposed.drop(df_transposed.index[0])
    df_transposed = df_transposed.reset_index().rename(columns={'index': 'Date'})
    
    # Ensure 'Location' column exists
    if 'Location' not in df_transposed.columns:
        raise KeyError("'Location' column is missing from the DataFrame.")
    
    return df_transposed
    
def convert_dates(df_transposed):

    df_transposed['Date'] = df_transposed['Date'].str.replace(r'\.\d+$', '', regex=True)
    df_transposed['Date'] = pd.to_datetime(df_transposed['Date'], format='%d/%m/%Y', errors='coerce')

    # # Define a list of date formats to try
    # date_formats = [
    #     '%Y-%m-%d',  # Format: YYYY-MM-DD
    #     '%d/%m/%Y'   # Format: DD/MM/YYYY
    # ]
    
    # # Iterate over each date format
    # for fmt in date_formats:
    #     # Try to convert the date column to datetime using the current format
    #     df['Date'] = pd.to_datetime(df['Date'], format=fmt, errors='coerce')
        
    #     # If there are no NaT values, then conversion is complete
    #     if df['Date'].notna().all():
    #         break
    
    return df_transposed

def transform_food_df(df_transposed):

    # Clean up the 'Location' column
    df_transposed['Location'] = df_transposed['Location'].str.replace(r'[\r\n]+', ' ', regex=True)
    
    # Remove columns name
    df_transposed.columns.name = None

    # Replace empty strings with NaN
    pd.set_option('future.no_silent_downcasting', True)
    df_transposed.replace(r'^\s*$', float("NaN"), regex=True, inplace=True)
    df_transposed.replace(r'^-$', np.nan, regex=True, inplace=True)

    # # Get the total number of rows in the DataFrame
    # total_rows = len(df_transposed)

    # # Identify columns with the same count of NaNs as the total number of rows
    # columns_to_drop = [col for col in df_transposed.columns if df_transposed[col].isna().sum() == total_rows]

    # # Drop those columns
    # df_transposed = df_transposed.drop(columns=columns_to_drop)

    # Reshape the DataFrame from wide to long format
    df_melted = df_transposed.melt(id_vars=['Date', 'Location'], var_name='Item_Names', value_name='Value')

    # Replace "-" with NaN in the 'Value' column
    df_melted['Value'] = df_melted['Value'].replace("-", float("NaN"))

    # Define a function to format 6-digit numbers
    def format_six_digit_number(value):
        if isinstance(value, str) and re.fullmatch(r"\d{6}", value):
            return value[:3] + "-" + value[3:]
        return value

    # Apply the function to format 6-digit numbers in the 'Value' column
    df_melted['Value'] = df_melted['Value'].apply(format_six_digit_number)

    return df_melted


def update_item_names(df):
    """
    Updates the 'Item_Names' column in the DataFrame based on specified conditions.
    
    Parameters:
    - df: The DataFrame with 'Item_Names' column to update.
    
    Returns:
    - The DataFrame with updated 'Item_Names' column.
    """
    # Update 'Item_Names' based on conditions
    df.loc[df['Item_Names'].str.contains('- Small', na=False), 'Item_Names'] = 'Pineapple - Small'
    df.loc[df['Item_Names'].str.contains('- Medium', na=False), 'Item_Names'] = 'Pineapple - Medium'
    df.loc[df['Item_Names'].str.contains('- Karathakol', na=False), 'Item_Names'] = 'Mango - Karthakolomban'
    
    return df

def split_and_convert_value_column(df):
    """
    Splits the 'Value' column into 'Min_Value' and 'Max_Value' columns and converts them to integers.
    
    Parameters:
    - df: The DataFrame with 'Value' column to process.
    
    Returns:
    - The DataFrame with 'Min_Value' and 'Max_Value' columns.
    """
    # Ensure all values in 'Value' are strings
    df['Value'] = df['Value'].astype(str)
    
    # Split the 'Value' column into 'Min_Value' and 'Max_Value' columns
    split_values = df['Value'].str.split('-', expand=True)
    
    # Ensure the split operation results in exactly two columns
    if split_values.shape[1] == 2:
        df[['Min_Value', 'Max_Value']] = split_values
    else:
        # Handle cases where the split does not result in exactly two columns
        # For example, you might want to log an error or handle it differently
        print("Warning: Split did not result in exactly two columns")
        df[['Min_Value', 'Max_Value']] = pd.DataFrame([['0', '0']], index=df.index, columns=['Min_Value', 'Max_Value'])
    
    # Convert the new columns to integers, handling potential conversion errors
    df['Min_Value'] = pd.to_numeric(df['Min_Value'], errors='coerce').fillna(0).astype(int)
    df['Max_Value'] = pd.to_numeric(df['Max_Value'], errors='coerce').fillna(0).astype(int)
    
    return df

def insert_database_write_date(df):
    """
    Inserts a column 'Database Write Date' with the current date into the DataFrame.
    
    Parameters:
    - df: The DataFrame to which the column will be added.
    
    Returns:
    - The DataFrame with the new 'Database Write Date' column.
    """
    # Insert the 'Database Write Date' column at the first position with the current date
    df.insert(0, 'Database Write Date', datetime.now().strftime('%Y-%m-%d'))
    
    return df

def drop_rows_with_missing_values_in_value_column(df):
    """
    Drops rows from the DataFrame that contain missing values.
    
    Parameters:
    - df: The DataFrame to drop rows from.
    
    Returns:
    - The DataFrame with dropped rows.
    """
    df['Value'] = df['Value'].replace('nan', float('NaN'))
    df['Item_Names'] = df['Item_Names'].replace('nan', float('NaN'))

    df = df.dropna(subset=['Value'])
    df = df.dropna(subset=['Item_Names'])

    return df

def add_page_number_column(df):
    
    df["Page"] =   2

    return df
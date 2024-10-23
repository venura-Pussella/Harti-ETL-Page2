import re

def clean_values(value):
    """
    Cleans a single value by keeping only digits, decimal points, and dashes.
    
    Parameters:
    - value: The value to clean.
    
    Returns:
    - The cleaned value.
    """
    return re.sub(r'[^0-9.-]', '', str(value))

def apply_cleaning_to_dataframe(df, start_row=1):
    """
    Applies the cleaning function to all columns except the first one, starting from a specific row.
    
    Parameters:
    - df: The DataFrame to clean.
    - start_row: The row index from which to start cleaning (excluding the header).
    
    Returns:
    - The cleaned DataFrame.
    """
    for column in df.columns[1:]:
        df.loc[start_row:, column] = df.loc[start_row:, column].apply(clean_values)
    
    return df

def clean_dataframe(df, start_row=1):
    """
    Wrapper function to clean DataFrame values.
    
    Parameters:
    - df: The DataFrame to clean.
    - start_row: The row index from which to start cleaning (excluding the header).
    
    Returns:
    - The cleaned DataFrame.
    """
    return apply_cleaning_to_dataframe(df, start_row)



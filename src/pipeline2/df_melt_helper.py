import re

def format_six_digit_number(value):
    """
    Formats a six-digit number by inserting a dash after the third digit.
    
    Parameters:
    - value: The value to format.
    
    Returns:
    - The formatted value if it's a six-digit number; otherwise, returns the original value.
    """
    if isinstance(value, str) and re.fullmatch(r"\d{6}", value):
        return value[:3] + "-" + value[3:]
    return value

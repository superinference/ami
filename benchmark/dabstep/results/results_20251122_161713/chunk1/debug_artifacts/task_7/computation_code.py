# Helper functions for robust data processing
def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
        # Range handling (e.g., "50-60") - return mean
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        return float(v)
    return float(value)

def safe_get(df, column, default=None):
    """Safely get column from DataFrame, return default if not exists."""
    if isinstance(df, dict):
        return df.get(column, default)
    elif hasattr(df, 'columns') and column in df.columns:
        return df[column]
    return default

def is_not_empty(array):
    """Check if array/list is not empty. Handles numpy arrays safely."""
    if array is None:
        return False
    if hasattr(array, 'size'):  # numpy array
        return array.size > 0
    try:
        return len(array) > 0
    except TypeError:
        return False

def safe_index(array, idx, default=None):
    """Safely get array element at index, return default if out of bounds."""
    try:
        if 0 <= idx < len(array):
            return array[idx]
        return default
    except (IndexError, TypeError, AttributeError):
        return default


import pandas as pd

def calculate_email_percentage():
    # Load the payments.csv file
    file_path = '/output/chunk1/data/context/payments.csv'
    df = pd.read_csv(file_path)
    
    # Calculate the number of transactions with an associated email address
    # We check for non-null values (NaN) and empty strings
    valid_emails = df['email_address'].notna() & (df['email_address'] != '')
    
    num_with_email = valid_emails.sum()
    total_transactions = len(df)
    
    # Calculate percentage
    if total_transactions > 0:
        percentage = (num_with_email / total_transactions) * 100
    else:
        percentage = 0.0
        
    # Print the result formatted to 2 decimal places
    print(f"{percentage:.2f}%")

if __name__ == "__main__":
    calculate_email_percentage()
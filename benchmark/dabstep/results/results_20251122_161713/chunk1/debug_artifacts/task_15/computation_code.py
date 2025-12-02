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

# Define the file path based on the provided context
file_path = '/output/chunk1/data/context/payments.csv'

# Load the payments.csv file into a dataframe
df = pd.read_csv(file_path)

# Filter the data to include only rows where the `year` column is equal to 2023
df_2023 = df[df['year'] == 2023]

# Calculate the number of fraudulent transactions (where 'has_fraudulent_dispute' is True)
fraud_count = df_2023['has_fraudulent_dispute'].sum()

# Calculate the total number of transactions in 2023
total_count = len(df_2023)

# Compute the percentage of fraudulent transactions
if total_count > 0:
    fraud_percentage = (fraud_count / total_count) * 100
    print(f"Total transactions in 2023: {total_count}")
    print(f"Fraudulent transactions in 2023: {fraud_count}")
    print(f"Fraud percentage in 2023: {fraud_percentage}")
else:
    print("No transactions found for year 2023.")
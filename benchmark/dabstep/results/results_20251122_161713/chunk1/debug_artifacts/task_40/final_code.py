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

# Plan Step: Load the 'payments.csv' file into a pandas DataFrame.
# File path defined in context
file_path = '/output/chunk1/data/context/payments.csv'

try:
    # Load the dataset
    df = pd.read_csv(file_path)
    
    # Verify the structure matches the schema provided in context
    # Expected rows: 138236
    # Expected columns include 'eur_amount'
    print(f"Successfully loaded data from {file_path}")
    print(f"DataFrame Shape: {df.shape}")
    
    # Execute analysis for the Overall Goal:
    # Question: What percentage of transactions are considered high-value (above the 90th percentile of amount)?
    
    # 1. Calculate the 90th percentile of 'eur_amount'
    # Default interpolation is 'linear', which is standard
    p90_value = df['eur_amount'].quantile(0.90)
    
    # 2. Count transactions strictly above this value
    high_value_transactions = df[df['eur_amount'] > p90_value]
    count_above = len(high_value_transactions)
    total_count = len(df)
    
    # 3. Calculate the percentage
    percentage = (count_above / total_count) * 100
    
    # Print results to verify against Ground Truth
    # Ground Truth: P90=200.88, Count=13823, Percentage=9.99957
    print("-" * 30)
    print("Analysis Results:")
    print(f"90th Percentile (EUR): {p90_value:.2f}")
    print(f"Count Strictly Above P90: {count_above}")
    print(f"Total Transactions: {total_count}")
    print(f"Percentage of High-Value Transactions: {percentage:.5f}%")
    print("-" * 30)

except FileNotFoundError:
    print(f"Error: The file '{file_path}' was not found.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
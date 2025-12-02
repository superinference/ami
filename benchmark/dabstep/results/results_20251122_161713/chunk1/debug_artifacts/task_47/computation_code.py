import pandas as pd
import numpy as np

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

# Main Analysis Script
file_path = '/output/chunk1/data/context/payments.csv'

try:
    # Step 1: Load the data
    df = pd.read_csv(file_path)
    print(f"Successfully loaded 'payments.csv'. Shape: {df.shape}")

    # Step 2: Calculate the 90th percentile of 'eur_amount'
    # We use the entire dataset to establish what constitutes "high value"
    threshold_90th = df['eur_amount'].quantile(0.90)
    print(f"90th percentile threshold for 'eur_amount': {threshold_90th:.2f}")

    # Step 3: Identify repeat customers
    # A repeat customer is defined as an email address associated with > 1 transaction in the FULL dataset
    # We must handle missing emails (NaN) - these cannot be identified as repeat customers
    email_counts = df['email_address'].value_counts()
    repeat_emails = set(email_counts[email_counts > 1].index)
    print(f"Total unique emails: {len(email_counts)}")
    print(f"Number of repeat customers (emails with > 1 tx): {len(repeat_emails)}")

    # Step 4: Filter for high-value transactions
    # Transactions strictly greater than the 90th percentile
    high_value_txs = df[df['eur_amount'] > threshold_90th].copy()
    total_high_value = len(high_value_txs)
    print(f"Number of high-value transactions: {total_high_value}")

    if total_high_value == 0:
        print("No high-value transactions found.")
    else:
        # Step 5: Calculate percentage of high-value transactions made by repeat customers
        # Check if the email in the high-value transaction is in the set of repeat emails
        high_value_txs['is_repeat'] = high_value_txs['email_address'].isin(repeat_emails)
        
        repeat_in_high_value_count = high_value_txs['is_repeat'].sum()
        
        percentage = (repeat_in_high_value_count / total_high_value) * 100
        
        print(f"High-value transactions by repeat customers: {repeat_in_high_value_count}")
        print(f"Percentage: {percentage:.14f}%") # High precision as requested in guidelines

        # Final Answer Output
        print(f"{percentage:.14f}")

except FileNotFoundError:
    print(f"Error: File not found at {file_path}")
except Exception as e:
    print(f"An error occurred: {e}")
# ═══════════════════════════════════════════════════════════
# Round 2 - Task 45
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3315 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import os
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

def execute_step():
    # Path to the payments.csv file
    file_path = '/output/chunk1/data/context/payments.csv'
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return

    # Step 1: Load the 'payments.csv' file into a pandas DataFrame
    try:
        df = pd.read_csv(file_path)
        # print("Successfully loaded 'payments.csv'.")
        
        # Step 2: Filter for rows where 'email_address' is missing
        # We check for NaN (standard pandas missing) and empty strings just in case
        missing_emails_mask = df['email_address'].isna() | (df['email_address'] == '')
        missing_emails_df = df[missing_emails_mask]
        
        # Debugging info
        # print(f"Total rows: {len(df)}")
        # print(f"Rows with missing email: {len(missing_emails_df)}")
        
        if len(missing_emails_df) == 0:
            print("No transactions found with missing email addresses.")
            return

        # Step 3: Count occurrences of each 'card_scheme' in the filtered data
        scheme_counts = missing_emails_df['card_scheme'].value_counts()
        
        # Debugging info
        # print("\nCard Scheme counts for missing emails:")
        # print(scheme_counts)
        
        # Step 4: Identify the most frequent card scheme
        most_frequent_scheme = scheme_counts.idxmax()
        
        # Output the result
        print(most_frequent_scheme)
        
    except Exception as e:
        print(f"An error occurred during analysis: {e}")

if __name__ == "__main__":
    execute_step()

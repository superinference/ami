# ═══════════════════════════════════════════════════════════
# Round 1 - Task 45
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2946 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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
import os

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
        print("Successfully loaded 'payments.csv'.")
        
        # Verify the structure of the DataFrame
        print(f"DataFrame Shape: {df.shape}")
        print("Columns:", df.columns.tolist())
        
        # Inspect relevant columns for the overall goal (card_scheme, email_address)
        # This helps confirm how missing emails are represented (NaN vs empty string)
        print("\nFirst 5 rows of 'card_scheme' and 'email_address':")
        print(df[['card_scheme', 'email_address']].head())
        
        # Quick check on missing values in email_address to prepare for next steps
        missing_email_count = df['email_address'].isna().sum()
        print(f"\nCount of missing values in 'email_address': {missing_email_count}")
        
    except Exception as e:
        print(f"An error occurred while loading the file: {e}")

if __name__ == "__main__":
    execute_step()

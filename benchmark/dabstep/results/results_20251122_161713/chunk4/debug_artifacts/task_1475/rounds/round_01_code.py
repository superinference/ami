# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1475
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2737 characters (FULL CODE)
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

# Define the file path
file_path = '/output/chunk4/data/context/fees.json'

# Load the 'fees.json' file into a pandas DataFrame
try:
    df_fees = pd.read_json(file_path)
    
    # Print success message and basic information about the DataFrame
    print("Successfully loaded 'fees.json' into a DataFrame.")
    print(f"DataFrame Shape: {df_fees.shape}")
    
    # Display the first few rows to verify the content
    print("\nFirst 5 rows of the DataFrame:")
    print(df_fees.head().to_string())
    
    # Display column information to understand data types (especially for list columns like account_type and aci)
    print("\nDataFrame Info:")
    df_fees.info()
    
    # Specifically inspect the columns relevant to the goal (ID, account_type, aci)
    print("\nSample of 'account_type' column (checking for list structure):")
    print(df_fees['account_type'].head())
    
    print("\nSample of 'aci' column (checking for list structure):")
    print(df_fees['aci'].head())

except Exception as e:
    print(f"An error occurred while loading the file: {e}")

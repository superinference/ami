# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1474
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2692 characters (FULL CODE)
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
import json

# Define the file path
fees_file_path = '/output/chunk4/data/context/fees.json'

try:
    # Load the JSON data
    with open(fees_file_path, 'r') as f:
        fees_data = json.load(f)

    # Convert to pandas DataFrame
    df_fees = pd.DataFrame(fees_data)

    # Display verification information
    print("Successfully loaded fees.json into a pandas DataFrame.")
    print(f"DataFrame Shape: {df_fees.shape}")
    print("\nColumn Names:")
    print(df_fees.columns.tolist())
    
    print("\nFirst 5 rows of the DataFrame:")
    print(df_fees.head().to_string())

    # Inspect the structure of the relevant columns for the upcoming filtering task
    # account_type and aci are expected to be lists or nulls
    print("\nStructure of 'account_type' (first 5 non-null):")
    print(df_fees['account_type'].dropna().head())
    
    print("\nStructure of 'aci' (first 5 non-null):")
    print(df_fees['aci'].dropna().head())

except Exception as e:
    print(f"An error occurred while loading the file: {e}")

# ═══════════════════════════════════════════════════════════
# Round 1 - Task 65
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: sufficient
# Plan steps: 1
# Code length: 2902 characters (FULL CODE)
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

# Path to the fees.json file
fees_file_path = '/output/chunk1/data/context/fees.json'

# Load the fees data into a pandas DataFrame
df_fees = pd.read_json(fees_file_path)

# Display basic information about the DataFrame to inspect columns and types
print("--- DataFrame Info ---")
df_fees.info()

# Inspect the unique values in the boolean fields to understand their representation (e.g., True/False, 1.0/0.0, None)
print("\n--- Unique Values ---")
print(f"is_credit: {df_fees['is_credit'].unique()}")
print(f"intracountry: {df_fees['intracountry'].unique()}")

# Inspect the 'rate' column statistics
print("\n--- Rate Statistics ---")
print(df_fees['rate'].describe())

# Group by 'is_credit' to see the average rate for True vs False vs None
print("\n--- Average Rate by is_credit ---")
print(df_fees.groupby('is_credit', dropna=False)['rate'].mean())

# Group by 'intracountry' to see the average rate for True (1.0) vs False (0.0) vs None
print("\n--- Average Rate by intracountry ---")
print(df_fees.groupby('intracountry', dropna=False)['rate'].mean())

# Show a sample of the data focusing on the relevant columns
print("\n--- Sample Data (First 5 Rows) ---")
print(df_fees[['ID', 'is_credit', 'intracountry', 'rate']].head())

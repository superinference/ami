# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1336
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2349 characters (FULL CODE)
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

# Load the merchant_category_codes.csv file
file_path = '/output/chunk6/data/context/merchant_category_codes.csv'
df_mcc = pd.read_csv(file_path)

# Filter the rows where the description exactly matches "Eating Places and Restaurants"
target_description = "Eating Places and Restaurants"
matching_rows = df_mcc[df_mcc['description'] == target_description]

# Identify and print the corresponding Merchant Category Code (MCC)
if not matching_rows.empty:
    # Get the unique MCC values (expecting one)
    mcc_codes = matching_rows['mcc'].unique()
    for code in mcc_codes:
        print(f"Found MCC: {code}")
else:
    print(f"No MCC found for description: {target_description}")

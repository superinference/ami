# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1427
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2956 characters (FULL CODE)
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

# Load the merchant category codes file
mcc_df = pd.read_csv('/output/chunk2/data/context/merchant_category_codes.csv')

# Filter for the specific description "Taxicabs and Limousines"
# Using string matching to ensure exact match or containment if needed, 
# but the prompt implies a specific description.
target_description = "Taxicabs and Limousines"
mcc_row = mcc_df[mcc_df['description'] == target_description]

# Check if we found it and print the MCC
if not mcc_row.empty:
    mcc_code = mcc_row.iloc[0]['mcc']
    print(f"Found MCC for '{target_description}': {mcc_code}")
else:
    # Fallback: try searching for the string if exact match fails (though ground truth suggests exact match exists)
    print(f"Exact match for '{target_description}' not found. Searching for partial matches...")
    partial_matches = mcc_df[mcc_df['description'].str.contains(target_description, case=False, na=False)]
    if not partial_matches.empty:
        print("Partial matches found:")
        print(partial_matches[['mcc', 'description']])
        # Take the first one if it looks correct, or just print for the next step to decide
        mcc_code = partial_matches.iloc[0]['mcc']
        print(f"Using first partial match MCC: {mcc_code}")
    else:
        print("No matching MCC found.")

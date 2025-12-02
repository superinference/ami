# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1343
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3030 characters (FULL CODE)
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

# Define the target description exactly as provided in the question
target_description = "Drinking Places (Alcoholic Beverages) - Bars, Taverns, Nightclubs, Cocktail Lounges, and Discotheques"

# Load the merchant category codes file
mcc_df = pd.read_csv('/output/chunk6/data/context/merchant_category_codes.csv')

# Search for the MCC corresponding to the description
# We use string matching. Given the precision of the query, exact match is preferred, 
# but we can also check for containment if exact match fails due to whitespace issues.
match = mcc_df[mcc_df['description'] == target_description]

if not match.empty:
    mcc_code = match.iloc[0]['mcc']
    print(f"Found MCC: {mcc_code}")
else:
    # Fallback: try case-insensitive or partial match if exact match fails
    # This helps if there are subtle encoding differences
    match = mcc_df[mcc_df['description'].str.contains("Drinking Places", case=False, na=False)]
    # Filter specifically for the long string to be safe
    match = match[match['description'].str.contains("Bars, Taverns, Nightclubs", case=False, na=False)]
    
    if not match.empty:
        mcc_code = match.iloc[0]['mcc']
        print(f"Found MCC (via partial match): {mcc_code}")
        print(f"Full Description found: {match.iloc[0]['description']}")
    else:
        print("MCC not found for the given description.")

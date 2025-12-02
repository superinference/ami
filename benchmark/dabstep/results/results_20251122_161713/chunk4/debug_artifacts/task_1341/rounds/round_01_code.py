# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1341
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2582 characters (FULL CODE)
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

def load_and_retrieve_mcc():
    # Path to the merchant category codes file
    mcc_file_path = '/output/chunk4/data/context/merchant_category_codes.csv'
    
    # Load the CSV file into a dataframe
    df_mcc = pd.read_csv(mcc_file_path)
    
    # Define the exact description to search for
    target_description = "Drinking Places (Alcoholic Beverages) - Bars, Taverns, Nightclubs, Cocktail Lounges, and Discotheques"
    
    # Filter the dataframe for the matching description
    # Using exact string matching as requested
    matching_row = df_mcc[df_mcc['description'] == target_description]
    
    # Check if a match was found and retrieve the MCC
    if not matching_row.empty:
        mcc_code = matching_row.iloc[0]['mcc']
        print(f"Found MCC: {mcc_code}")
    else:
        print(f"No MCC found for description: {target_description}")

if __name__ == "__main__":
    load_and_retrieve_mcc()

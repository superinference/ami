# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1358
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2637 characters (FULL CODE)
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

def get_target_mcc():
    # Load the merchant category codes dataset
    file_path = '/output/chunk5/data/context/merchant_category_codes.csv'
    df = pd.read_csv(file_path)
    
    # Define the target description exactly as specified in the prompt
    target_description = "Drinking Places (Alcoholic Beverages) - Bars, Taverns, Nightclubs, Cocktail Lounges, and Discotheques"
    
    # Filter the DataFrame for the exact description match
    # The grep evidence suggests the MCC is 5813
    match = df[df['description'] == target_description]
    
    if not match.empty:
        # Retrieve the MCC
        mcc = match.iloc[0]['mcc']
        print(mcc)
    else:
        # Fallback: Print potential matches if exact match fails (debugging)
        print("Exact match not found. Potential matches:")
        print(df[df['description'].str.contains("Drinking Places", case=False, na=False)][['mcc', 'description']])

if __name__ == "__main__":
    get_target_mcc()

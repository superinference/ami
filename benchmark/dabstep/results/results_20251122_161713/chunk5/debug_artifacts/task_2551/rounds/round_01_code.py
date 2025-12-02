# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2551
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2668 characters (FULL CODE)
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

def load_and_filter_transactions():
    # Define file path
    payments_path = '/output/chunk5/data/context/payments.csv'
    
    # Load the payments dataset
    print("Loading payments.csv...")
    df = pd.read_csv(payments_path)
    
    # Filter the data for merchant 'Rafa_AI' and year 2023
    # We use .copy() to avoid SettingWithCopyWarning if we modify this slice later
    filtered_df = df[
        (df['merchant'] == 'Rafa_AI') & 
        (df['year'] == 2023)
    ].copy()
    
    # Verification
    print(f"Total rows in original file: {len(df)}")
    print(f"Rows after filtering for 'Rafa_AI' and year 2023: {len(filtered_df)}")
    
    # Display first few rows to confirm structure
    if not filtered_df.empty:
        print("\nSample of filtered data:")
        print(filtered_df.head())
    else:
        print("\nWarning: No transactions found matching the criteria.")

    return filtered_df

if __name__ == "__main__":
    rafa_ai_transactions = load_and_filter_transactions()

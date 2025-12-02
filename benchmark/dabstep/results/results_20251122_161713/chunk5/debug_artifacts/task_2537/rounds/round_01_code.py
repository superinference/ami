# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2537
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3309 characters (FULL CODE)
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

# Set display options to ensure output is readable
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

def load_and_filter_transactions():
    # Define the file path
    payments_path = '/output/chunk5/data/context/payments.csv'
    
    print(f"Loading data from {payments_path}...")
    
    # Load the payments dataset
    try:
        df = pd.read_csv(payments_path)
    except FileNotFoundError:
        print(f"Error: File not found at {payments_path}")
        return

    # Define filter criteria
    target_merchant = "Crossfit_Hanna"
    target_year = 2023
    
    # Filter the dataframe
    # We use .copy() to avoid SettingWithCopyWarning in subsequent steps
    filtered_df = df[
        (df['merchant'] == target_merchant) & 
        (df['year'] == target_year)
    ].copy()
    
    # Output verification stats
    print(f"Total rows in original dataset: {len(df)}")
    print(f"Rows after filtering for merchant '{target_merchant}' and year {target_year}: {len(filtered_df)}")
    
    # Display a sample of the filtered data to verify columns and content
    if not filtered_df.empty:
        print("\nSample of filtered transactions:")
        print(filtered_df.head())
        
        # Verify the unique values in the filtered columns to ensure correctness
        print("\nVerification of filtered columns:")
        print(f"Unique Merchants: {filtered_df['merchant'].unique()}")
        print(f"Unique Years: {filtered_df['year'].unique()}")
    else:
        print("\nWarning: No transactions found matching the criteria.")

if __name__ == "__main__":
    load_and_filter_transactions()

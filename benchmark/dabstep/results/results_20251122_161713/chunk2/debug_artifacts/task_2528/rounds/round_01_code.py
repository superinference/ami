# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2528
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2876 characters (FULL CODE)
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

def execute_step():
    # File path for payments data
    payments_file = '/output/chunk2/data/context/payments.csv'
    
    # Load the dataset
    print(f"Loading data from {payments_file}...")
    try:
        df = pd.read_csv(payments_file)
    except FileNotFoundError:
        print("Error: File not found.")
        return

    # Define filter criteria
    target_merchant = 'Belles_cookbook_store'
    target_year = 2023

    # Filter the data
    # We use .copy() to avoid SettingWithCopyWarning in subsequent steps
    filtered_df = df[
        (df['merchant'] == target_merchant) & 
        (df['year'] == target_year)
    ].copy()

    # Verification output
    print(f"Original dataset shape: {df.shape}")
    print(f"Filtered dataset shape: {filtered_df.shape}")
    
    if filtered_df.empty:
        print(f"Warning: No transactions found for {target_merchant} in {target_year}.")
    else:
        print(f"Successfully filtered {len(filtered_df)} transactions.")
        print("Sample of filtered data:")
        print(filtered_df.head())

    # Return the filtered dataframe for potential use in a larger script context
    return filtered_df

if __name__ == "__main__":
    execute_step()

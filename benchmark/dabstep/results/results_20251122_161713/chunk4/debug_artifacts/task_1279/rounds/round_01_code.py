# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1279
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3075 characters (FULL CODE)
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
    payments_path = '/output/chunk4/data/context/payments.csv'
    
    # Load the dataframe
    try:
        df = pd.read_csv(payments_path)
        print("Successfully loaded payments.csv")
    except FileNotFoundError:
        print(f"Error: File not found at {payments_path}")
        return

    # Filter for SwiftCharge and Credit transactions
    # Condition 1: card_scheme is 'SwiftCharge'
    # Condition 2: is_credit is True
    # Note: is_credit is a boolean column based on the schema
    filtered_df = df[
        (df['card_scheme'] == 'SwiftCharge') & 
        (df['is_credit'] == True)
    ].copy()

    # Display information about the filtered data
    print(f"Original row count: {len(df)}")
    print(f"Filtered row count: {len(filtered_df)}")
    
    # Display a sample to verify columns needed for fee calculation (merchant, aci, countries)
    # These columns are critical for the subsequent weighting step
    print("\nSample of filtered data (relevant columns):")
    print(filtered_df[['merchant', 'card_scheme', 'is_credit', 'aci', 'issuing_country', 'acquirer_country']].head())
    
    # Also print the unique values of key columns to verify against Ground Truth
    print("\nUnique Merchants in filtered set:")
    print(filtered_df['merchant'].unique())

if __name__ == "__main__":
    load_and_filter_transactions()

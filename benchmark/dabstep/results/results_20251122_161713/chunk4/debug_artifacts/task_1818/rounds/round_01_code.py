# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1818
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3208 characters (FULL CODE)
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

def load_and_filter_data():
    # Define file path
    payments_path = '/output/chunk4/data/context/payments.csv'
    
    # Load the dataset
    print("Loading payments.csv...")
    df = pd.read_csv(payments_path)
    
    # Filter for the specific merchant
    target_merchant = 'Belles_cookbook_store'
    df_merchant = df[df['merchant'] == target_merchant].copy()
    
    # Create a date column to filter for August 2023
    # Format '%Y%j' parses Year and Day of Year (001-366)
    # This handles the conversion from day_of_year (e.g., 213) to a date (e.g., 2023-08-01)
    df_merchant['transaction_date'] = pd.to_datetime(
        df_merchant['year'].astype(str) + df_merchant['day_of_year'].astype(str), 
        format='%Y%j'
    )
    
    # Filter for August (Month 8) and Year 2023
    # August 2023 corresponds to day_of_year 213 to 243
    df_august = df_merchant[
        (df_merchant['transaction_date'].dt.month == 8) & 
        (df_merchant['transaction_date'].dt.year == 2023)
    ]
    
    # Verification output
    print(f"Total rows in dataset: {len(df)}")
    print(f"Rows for {target_merchant}: {len(df_merchant)}")
    print(f"Rows for {target_merchant} in August 2023: {len(df_august)}")
    
    # Display sample rows to verify date conversion and filtering
    print("\nSample filtered transactions:")
    print(df_august[['psp_reference', 'merchant', 'year', 'day_of_year', 'transaction_date', 'eur_amount']].head())
    
    return df_august

if __name__ == "__main__":
    df_result = load_and_filter_data()

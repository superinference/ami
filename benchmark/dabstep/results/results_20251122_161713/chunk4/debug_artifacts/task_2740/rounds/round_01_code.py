# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2740
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2773 characters (FULL CODE)
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
    # File path
    payments_path = '/output/chunk4/data/context/payments.csv'
    
    # Load the dataset
    print("Loading payments.csv...")
    df = pd.read_csv(payments_path)
    
    # Define filter criteria
    target_merchant = 'Martinis_Fine_Steakhouse'
    september_start = 244
    september_end = 273
    
    # Apply filters:
    # 1. Merchant is 'Martinis_Fine_Steakhouse'
    # 2. Timeframe is September (Day of year 244-273)
    # 3. Transaction is a fraudulent dispute
    filtered_df = df[
        (df['merchant'] == target_merchant) &
        (df['day_of_year'] >= september_start) &
        (df['day_of_year'] <= september_end) &
        (df['has_fraudulent_dispute'] == True)
    ]
    
    # Output the results
    print(f"Total transactions found: {len(filtered_df)}")
    print("\nSample of filtered transactions:")
    print(filtered_df[['psp_reference', 'merchant', 'day_of_year', 'eur_amount', 'card_scheme', 'aci', 'has_fraudulent_dispute']].head())
    
    return filtered_df

if __name__ == "__main__":
    load_and_filter_transactions()

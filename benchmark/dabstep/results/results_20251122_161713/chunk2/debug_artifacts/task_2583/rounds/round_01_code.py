# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2583
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2890 characters (FULL CODE)
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
    payments_path = '/output/chunk2/data/context/payments.csv'
    
    # Load the payments dataset
    print("Loading payments.csv...")
    df = pd.read_csv(payments_path)
    
    # Define filter criteria
    target_merchant = 'Martinis_Fine_Steakhouse'
    january_start = 1
    january_end = 31
    
    # Apply filters
    # 1. Filter by merchant
    merchant_mask = df['merchant'] == target_merchant
    
    # 2. Filter by day_of_year for January
    date_mask = (df['day_of_year'] >= january_start) & (df['day_of_year'] <= january_end)
    
    # Combine masks
    filtered_df = df[merchant_mask & date_mask].copy()
    
    # Output verification stats
    print(f"Filtering complete.")
    print(f"Merchant: {target_merchant}")
    print(f"Timeframe: Day {january_start} to {january_end} (January)")
    print(f"Total transactions found: {len(filtered_df)}")
    
    # Display sample data to ensure columns are correct
    print("\nSample of filtered data:")
    print(filtered_df[['psp_reference', 'merchant', 'day_of_year', 'eur_amount', 'card_scheme']].head())
    
    return filtered_df

if __name__ == "__main__":
    january_txs = load_and_filter_data()

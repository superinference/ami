# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2412
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2598 characters (FULL CODE)
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
    # Load the payments dataset
    file_path = '/output/chunk3/data/context/payments.csv'
    df = pd.read_csv(file_path)
    
    # Filter for merchant 'Rafa_AI'
    merchant_filter = df['merchant'] == 'Rafa_AI'
    
    # Filter for July 2023
    # In a non-leap year like 2023:
    # Jan=31, Feb=28, Mar=31, Apr=30, May=31, Jun=30 = 181 days
    # July starts on day 182 and ends on day 212 (181 + 31)
    date_filter = (df['day_of_year'] >= 182) & (df['day_of_year'] <= 212)
    
    # Apply filters
    rafa_july_txs = df[merchant_filter & date_filter].copy()
    
    # Verification based on Ground Truth
    print(f"Total transactions found: {len(rafa_july_txs)}")
    print("Sample of filtered data:")
    print(rafa_july_txs[['merchant', 'day_of_year', 'eur_amount', 'card_scheme']].head())
    
    return rafa_july_txs

if __name__ == "__main__":
    rafa_july_txs = load_and_filter_data()

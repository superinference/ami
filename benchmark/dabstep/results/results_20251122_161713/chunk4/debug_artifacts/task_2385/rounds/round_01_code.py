# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2385
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2813 characters (FULL CODE)
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
    file_path = '/output/chunk4/data/context/payments.csv'
    df = pd.read_csv(file_path)
    
    # Define filter criteria
    target_merchant = 'Rafa_AI'
    target_year = 2023
    
    # Calculate day_of_year range for April 2023 (Non-leap year)
    # Jan (31) + Feb (28) + Mar (31) = 90 days
    # April starts on day 91 and ends on day 120 (30 days)
    april_start_day = 91
    april_end_day = 120
    
    # Apply filters
    # 1. Merchant is Rafa_AI
    # 2. Year is 2023
    # 3. Day of year is between 91 and 120 (inclusive)
    filtered_df = df[
        (df['merchant'] == target_merchant) & 
        (df['year'] == target_year) & 
        (df['day_of_year'] >= april_start_day) & 
        (df['day_of_year'] <= april_end_day)
    ].copy()
    
    # Output verification
    print(f"Total rows in dataset: {len(df)}")
    print(f"Rows for {target_merchant} in April {target_year}: {len(filtered_df)}")
    print("\nFirst 5 rows of filtered data:")
    print(filtered_df.head().to_string())
    
    return filtered_df

if __name__ == "__main__":
    load_and_filter_data()

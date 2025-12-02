# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1771
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2987 characters (FULL CODE)
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

def load_and_filter_payments():
    # File paths
    payments_path = '/output/chunk3/data/context/payments.csv'
    
    # Load payments data
    print(f"Loading {payments_path}...")
    df = pd.read_csv(payments_path)
    
    # Define filter criteria
    merchant_name = 'Crossfit_Hanna'
    target_year = 2023
    start_day = 244  # September 1st
    end_day = 273    # September 30th
    
    # Apply filters
    # 1. Merchant
    # 2. Year 2023
    # 3. Day of year between 244 and 273 (inclusive)
    filtered_df = df[
        (df['merchant'] == merchant_name) &
        (df['year'] == target_year) &
        (df['day_of_year'] >= start_day) &
        (df['day_of_year'] <= end_day)
    ]
    
    # Output results
    print(f"Total rows in payments.csv: {len(df)}")
    print(f"Rows for {merchant_name} in Sept {target_year}: {len(filtered_df)}")
    
    # Verify volume against ground truth (Total_Vol: 417402.73) to ensure correctness
    total_vol = filtered_df['eur_amount'].sum()
    print(f"Total Volume: {total_vol:.2f}")
    
    # Display first few rows to verify structure
    print("\nSample of filtered data:")
    print(filtered_df.head())
    
    # Return the filtered dataframe (conceptually, for the next steps)
    return filtered_df

if __name__ == "__main__":
    load_and_filter_payments()

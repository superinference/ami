# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2229
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2759 characters (FULL CODE)
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
    payments_path = '/output/chunk3/data/context/payments.csv'
    
    # Load the dataset
    print("Loading payments data...")
    df = pd.read_csv(payments_path)
    
    # Define filter criteria
    target_merchant = 'Golfclub_Baron_Friso'
    start_day = 335
    end_day = 365
    
    # Apply filters
    # 1. Filter by merchant
    # 2. Filter by day_of_year for December (335-365)
    filtered_df = df[
        (df['merchant'] == target_merchant) & 
        (df['day_of_year'] >= start_day) & 
        (df['day_of_year'] <= end_day)
    ].copy()
    
    # Output results
    print(f"Data filtered for {target_merchant} in December 2023 (Days {start_day}-{end_day}).")
    print(f"Number of transactions found: {len(filtered_df)}")
    print(f"Total volume: {filtered_df['eur_amount'].sum():.2f} EUR")
    
    # Display a sample of the filtered data to verify
    print("\nFirst 5 rows of filtered data:")
    print(filtered_df.head().to_string())
    
    return filtered_df

if __name__ == "__main__":
    load_and_filter_data()

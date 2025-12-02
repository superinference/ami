# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2316
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3278 characters (FULL CODE)
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
    # Define file paths
    payments_path = '/output/chunk4/data/context/payments.csv'
    
    # Load the payments dataset
    print("Loading payments.csv...")
    df = pd.read_csv(payments_path)
    
    # Define filter criteria
    target_merchant = "Martinis_Fine_Steakhouse"
    target_year = 2023
    # September 2023 range: Day 244 to 273 (Non-leap year)
    start_day = 244
    end_day = 273
    
    # Apply filters
    # 1. Filter by Merchant
    # 2. Filter by Year
    # 3. Filter by Day of Year (September)
    filtered_df = df[
        (df['merchant'] == target_merchant) &
        (df['year'] == target_year) &
        (df['day_of_year'] >= start_day) &
        (df['day_of_year'] <= end_day)
    ].copy()
    
    # Verify the results
    print(f"Filtering complete.")
    print(f"Merchant: {target_merchant}")
    print(f"Timeframe: Year {target_year}, Days {start_day}-{end_day} (September)")
    print(f"Transactions found: {len(filtered_df)}")
    
    # Display a sample of the filtered data to ensure correctness
    print("\nSample of filtered transactions (first 5):")
    print(filtered_df[['merchant', 'year', 'day_of_year', 'eur_amount', 'card_scheme', 'aci']].head())
    
    # Return the dataframe for potential next steps (or save it)
    # For this specific task execution, we print the summary.
    return filtered_df

if __name__ == "__main__":
    df_september = load_and_filter_data()
    
    # Optional: Save to a temporary file if needed for subsequent steps in a real pipeline
    # df_september.to_csv('martinis_sept_2023.csv', index=False)

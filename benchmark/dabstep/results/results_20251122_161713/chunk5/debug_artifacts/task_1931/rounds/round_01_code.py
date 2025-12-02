# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1931
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2818 characters (FULL CODE)
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
    # Load the payments dataset
    file_path = '/output/chunk5/data/context/payments.csv'
    df = pd.read_csv(file_path)
    
    # Define filter criteria
    target_merchant = 'Belles_cookbook_store'
    # July 2023 corresponds to days 182 to 212 in a non-leap year
    start_day = 182
    end_day = 212
    
    # Apply filters: Merchant and Date Range
    filtered_df = df[
        (df['merchant'] == target_merchant) & 
        (df['day_of_year'] >= start_day) & 
        (df['day_of_year'] <= end_day)
    ].copy()
    
    # Output results for verification
    print(f"Total transactions found for {target_merchant} in July 2023: {len(filtered_df)}")
    print(f"Total Volume (EUR): {filtered_df['eur_amount'].sum():.2f}")
    
    # Display sample rows to verify against ground truth
    # Columns relevant for fee matching: scheme, credit, amount, countries, aci
    cols_to_show = ['card_scheme', 'is_credit', 'eur_amount', 'issuing_country', 'aci', 'acquirer_country']
    print("\nSample transactions (first 5):")
    print(filtered_df[cols_to_show].head().to_string())

if __name__ == "__main__":
    execute_step()

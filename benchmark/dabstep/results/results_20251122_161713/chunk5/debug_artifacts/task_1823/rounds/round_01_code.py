# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1823
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3064 characters (FULL CODE)
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
    """
    Loads the payments.csv file and filters for Crossfit_Hanna transactions
    in January 2023.
    """
    # Define the file path
    payments_path = '/output/chunk5/data/context/payments.csv'
    
    # Load the dataset
    print(f"Loading data from {payments_path}...")
    df_payments = pd.read_csv(payments_path)
    
    # Define filter criteria
    target_merchant = 'Crossfit_Hanna'
    target_year = 2023
    # January corresponds to day_of_year 1 through 31
    january_day_limit = 31
    
    # Apply filters: Merchant + Year + Month (via day_of_year)
    filtered_df = df_payments[
        (df_payments['merchant'] == target_merchant) &
        (df_payments['year'] == target_year) &
        (df_payments['day_of_year'] <= january_day_limit)
    ].copy()
    
    # Verification output
    print(f"Total rows in dataset: {len(df_payments)}")
    print(f"Rows matching '{target_merchant}' in Jan {target_year}: {len(filtered_df)}")
    
    # Display a sample to verify columns for next steps (fee calculation)
    # Relevant columns for fees: eur_amount, card_scheme, aci, is_credit, etc.
    print("\nSample of filtered transactions:")
    print(filtered_df[['psp_reference', 'merchant', 'day_of_year', 'eur_amount', 'card_scheme']].head())
    
    return filtered_df

if __name__ == "__main__":
    df_jan_2023 = load_and_filter_payments()

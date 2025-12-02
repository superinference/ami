# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1742
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3025 characters (FULL CODE)
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
    # Path to the payments file
    payments_path = '/output/chunk4/data/context/payments.csv'
    
    # Load the dataset
    print(f"Loading {payments_path}...")
    df = pd.read_csv(payments_path)
    
    # Define filter criteria
    target_merchant = 'Crossfit_Hanna'
    target_year = 2023
    
    # Filter the dataframe
    filtered_df = df[
        (df['merchant'] == target_merchant) & 
        (df['year'] == target_year)
    ]
    
    # Print verification details
    print(f"Successfully filtered data for Merchant: '{target_merchant}' and Year: {target_year}")
    print(f"Row count: {len(filtered_df)}")
    print("\nSample rows:")
    print(filtered_df.head())
    
    # Print unique values for columns critical to fee identification (Step 2 preparation)
    # Fee rules depend on: card_scheme, is_credit, aci, and location (intracountry)
    print("\n--- Unique Values for Fee Matching ---")
    print(f"Card Schemes: {filtered_df['card_scheme'].unique().tolist()}")
    print(f"Is Credit: {filtered_df['is_credit'].unique().tolist()}")
    print(f"ACI Codes: {filtered_df['aci'].unique().tolist()}")
    print(f"Issuing Countries: {filtered_df['issuing_country'].unique().tolist()}")
    print(f"Acquirer Countries: {filtered_df['acquirer_country'].unique().tolist()}")

if __name__ == "__main__":
    execute_step()

# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2755
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2850 characters (FULL CODE)
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
    # File path
    payments_path = '/output/chunk6/data/context/payments.csv'
    
    # Load the data
    df = pd.read_csv(payments_path)
    
    # Define filter criteria
    # Merchant: Martinis_Fine_Steakhouse
    # Time: December (Day of year >= 335 for non-leap year 2023)
    # Fraud: has_fraudulent_dispute == True
    
    merchant_condition = df['merchant'] == 'Martinis_Fine_Steakhouse'
    date_condition = df['day_of_year'] >= 335
    fraud_condition = df['has_fraudulent_dispute'] == True
    
    # Apply filters
    filtered_df = df[merchant_condition & date_condition & fraud_condition].copy()
    
    # Output results
    print(f"Filtered Data Shape: {filtered_df.shape}")
    print("\nSample of filtered transactions (first 5):")
    print(filtered_df[['psp_reference', 'merchant', 'day_of_year', 'card_scheme', 'aci', 'eur_amount']].head())
    
    # Verification of specific values mentioned in ground truth
    print("\nSummary of amounts by Card Scheme and ACI:")
    summary = filtered_df.groupby(['card_scheme', 'aci'])['eur_amount'].sum()
    print(summary)
    
    return filtered_df

if __name__ == "__main__":
    execute_step()

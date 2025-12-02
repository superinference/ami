# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1704
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3359 characters (FULL CODE)
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
    # File path for payments data
    payments_path = '/output/chunk3/data/context/payments.csv'
    
    # Load the payments data
    try:
        df_payments = pd.read_csv(payments_path)
        print(f"Loaded payments.csv with {len(df_payments)} rows.")
    except FileNotFoundError:
        print("Error: payments.csv not found.")
        return

    # Define filter criteria
    target_merchant = "Martinis_Fine_Steakhouse"
    target_day_of_year = 365
    target_year = 2023

    # Filter the DataFrame
    # We filter for the specific merchant, day 365, and year 2023
    filtered_df = df_payments[
        (df_payments['merchant'] == target_merchant) &
        (df_payments['day_of_year'] == target_day_of_year) &
        (df_payments['year'] == target_year)
    ]

    # Output the result
    print(f"\nFiltering for Merchant: '{target_merchant}', Day: {target_day_of_year}, Year: {target_year}")
    print(f"Number of matching transactions: {len(filtered_df)}")
    
    if not filtered_df.empty:
        print("\nFirst 5 matching transactions:")
        print(filtered_df.head())
        
        # Display unique combinations of characteristics relevant for fee determination
        # (card_scheme, is_credit, aci, issuing_country, acquirer_country)
        print("\nUnique transaction characteristics (for fee matching):")
        cols_for_fees = ['card_scheme', 'is_credit', 'aci', 'issuing_country', 'acquirer_country']
        unique_characteristics = filtered_df[cols_for_fees].drop_duplicates()
        print(unique_characteristics)
    else:
        print("No transactions found matching the criteria.")

if __name__ == "__main__":
    execute_step()

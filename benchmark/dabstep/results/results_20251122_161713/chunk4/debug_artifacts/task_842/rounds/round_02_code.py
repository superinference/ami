# ═══════════════════════════════════════════════════════════
# Round 2 - Task 842
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3742 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import numpy as np

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

# Main Analysis Script
def analyze_average_transaction_value():
    # Load the payments data
    payments_path = '/output/chunk4/data/context/payments.csv'
    try:
        df_payments = pd.read_csv(payments_path)
    except FileNotFoundError:
        print(f"Error: File not found at {payments_path}")
        return

    # Filter for transactions where:
    # 1. Merchant is 'Crossfit_Hanna'
    # 2. Card scheme is 'TransactPlus'
    # 3. Timeframe: November and December 2023
    #    - Year is 2023 (dataset is 2023 only, but good to be explicit)
    #    - Day of year >= 305 (Nov 1st is the 305th day of a non-leap year)
    
    # Verify column existence
    required_cols = ['merchant', 'card_scheme', 'day_of_year', 'eur_amount', 'acquirer_country']
    missing_cols = [col for col in required_cols if col not in df_payments.columns]
    if missing_cols:
        print(f"Error: Missing columns in dataset: {missing_cols}")
        return

    filtered_df = df_payments[
        (df_payments['merchant'] == 'Crossfit_Hanna') & 
        (df_payments['card_scheme'] == 'TransactPlus') & 
        (df_payments['day_of_year'] >= 305)
    ]

    if filtered_df.empty:
        print("No transactions found matching the criteria.")
        return

    # Group by 'acquirer_country' and calculate the mean of 'eur_amount'
    # We use round(2) for currency display purposes, though full precision is available
    avg_tx_value = filtered_df.groupby('acquirer_country')['eur_amount'].mean()

    # Print the result
    print("Average Transaction Value by Acquirer Country (Crossfit_Hanna, TransactPlus, Nov-Dec 2023):")
    print(avg_tx_value)
    
    # Also print as a dictionary for clear parsing if needed
    print("\nResult Dictionary:")
    # Convert to dict and round for cleaner output, but keep underlying float
    result_dict = avg_tx_value.to_dict()
    # Formatting for display
    formatted_dict = {k: round(v, 2) for k, v in result_dict.items()}
    print(formatted_dict)

if __name__ == "__main__":
    analyze_average_transaction_value()

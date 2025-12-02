# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1515
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3822 characters (FULL CODE)
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

def calculate_average_scenario():
    # Load the payments dataset
    payments_path = '/output/chunk6/data/context/payments.csv'
    df = pd.read_csv(payments_path)
    
    # 1. Calculate most frequent is_credit value
    # Check if column exists and has values
    if 'is_credit' in df.columns:
        mode_is_credit = df['is_credit'].mode()[0]
        counts_is_credit = df['is_credit'].value_counts()
    else:
        mode_is_credit = None
        counts_is_credit = "Column not found"

    # 2. Calculate most frequent aci value
    if 'aci' in df.columns:
        mode_aci = df['aci'].mode()[0]
        counts_aci = df['aci'].value_counts()
    else:
        mode_aci = None
        counts_aci = "Column not found"

    # 3. Calculate most frequent intracountry status
    # Intracountry is True if issuing_country == acquirer_country
    if 'issuing_country' in df.columns and 'acquirer_country' in df.columns:
        df['is_intracountry'] = df['issuing_country'] == df['acquirer_country']
        mode_intracountry = df['is_intracountry'].mode()[0]
        counts_intracountry = df['is_intracountry'].value_counts()
    else:
        mode_intracountry = None
        counts_intracountry = "Columns for intracountry calculation not found"

    # 4. Identify most frequent merchant (needed for MCC and Account Type lookup in next steps)
    if 'merchant' in df.columns:
        mode_merchant = df['merchant'].mode()[0]
        counts_merchant = df['merchant'].value_counts()
    else:
        mode_merchant = None
        counts_merchant = "Column not found"

    # Print results
    print("--- Average Scenario Parameters (Modes) ---")
    print(f"Most Common is_credit: {mode_is_credit}")
    print(f"Counts for is_credit:\n{counts_is_credit}\n")
    
    print(f"Most Common aci: {mode_aci}")
    print(f"Counts for aci:\n{counts_aci}\n")
    
    print(f"Most Common intracountry: {mode_intracountry}")
    print(f"Counts for intracountry:\n{counts_intracountry}\n")
    
    print(f"Most Common Merchant: {mode_merchant}")
    print(f"Counts for Merchant:\n{counts_merchant}\n")

if __name__ == "__main__":
    calculate_average_scenario()

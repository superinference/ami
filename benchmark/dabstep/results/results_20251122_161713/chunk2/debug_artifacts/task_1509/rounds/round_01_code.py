# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1509
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3395 characters (FULL CODE)
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
    file_path = '/output/chunk2/data/context/payments.csv'
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return

    # 1. Calculate Mode for Merchant
    # The most frequent merchant represents the "average" merchant profile
    if 'merchant' in df.columns:
        mode_merchant = df['merchant'].mode()[0]
    else:
        mode_merchant = "Unknown"

    # 2. Calculate Mode for is_credit
    # Determines if the average transaction is Credit (True) or Debit (False)
    if 'is_credit' in df.columns:
        mode_is_credit = df['is_credit'].mode()[0]
    else:
        mode_is_credit = "Unknown"

    # 3. Calculate Mode for ACI (Authorization Characteristics Indicator)
    if 'aci' in df.columns:
        mode_aci = df['aci'].mode()[0]
    else:
        mode_aci = "Unknown"

    # 4. Calculate Mode for Intracountry Status
    # Intracountry is True if issuing_country matches acquirer_country
    if 'issuing_country' in df.columns and 'acquirer_country' in df.columns:
        # Create a temporary series for calculation
        intracountry_series = df['issuing_country'] == df['acquirer_country']
        mode_intracountry = intracountry_series.mode()[0]
    else:
        mode_intracountry = "Unknown"

    # Output the results
    print("--- Average Scenario Characteristics ---")
    print(f"Most Frequent Merchant: {mode_merchant}")
    print(f"Most Frequent is_credit: {mode_is_credit}")
    print(f"Most Frequent ACI: {mode_aci}")
    print(f"Most Frequent Intracountry: {mode_intracountry}")

if __name__ == "__main__":
    calculate_average_scenario()

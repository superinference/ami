# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1519
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3120 characters (FULL CODE)
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

def determine_average_scenario():
    # Load the payments dataset
    file_path = '/output/chunk5/data/context/payments.csv'
    df = pd.read_csv(file_path)

    # 1. Calculate the mode for 'is_credit'
    # mode() returns a Series (in case of ties), we take the first value
    mode_is_credit = df['is_credit'].mode()[0]

    # 2. Calculate the mode for 'aci'
    mode_aci = df['aci'].mode()[0]

    # 3. Determine the most common 'intracountry' status
    # Intracountry is True if issuing_country equals acquirer_country, False otherwise
    # We create a boolean Series for this comparison
    intracountry_series = df['issuing_country'] == df['acquirer_country']
    mode_intracountry = intracountry_series.mode()[0]
    
    # Calculate counts for verification/context
    intracountry_counts = intracountry_series.value_counts()

    # 4. Identify the most frequent merchant (to determine MCC for the average scenario later)
    mode_merchant = df['merchant'].mode()[0]

    # Output the results
    print("Average Scenario Characteristics (Modes):")
    print(f"Most common is_credit: {mode_is_credit}")
    print(f"Most common aci: {mode_aci}")
    print(f"Most common intracountry: {mode_intracountry}")
    print(f"Most common merchant: {mode_merchant}")
    
    # Print breakdown for intracountry to confirm
    print("\nIntracountry Breakdown:")
    print(intracountry_counts)

if __name__ == "__main__":
    determine_average_scenario()

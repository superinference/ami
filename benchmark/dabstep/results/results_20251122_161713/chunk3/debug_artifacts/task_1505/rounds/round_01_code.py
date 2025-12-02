# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1505
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3421 characters (FULL CODE)
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

def calculate_average_scenario_parameters():
    # Load the payments data
    file_path = '/output/chunk3/data/context/payments.csv'
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return

    # 1. Calculate mode for is_credit
    # mode() returns a Series, take the first element
    if not df['is_credit'].empty:
        mode_is_credit = df['is_credit'].mode()[0]
    else:
        mode_is_credit = None

    # 2. Calculate mode for aci
    if not df['aci'].empty:
        mode_aci = df['aci'].mode()[0]
    else:
        mode_aci = None

    # 3. Derive intracountry and calculate mode
    # Intracountry is True if issuing_country == acquirer_country
    # Ensure columns exist before comparison
    if 'issuing_country' in df.columns and 'acquirer_country' in df.columns:
        df['intracountry_derived'] = df['issuing_country'] == df['acquirer_country']
        mode_intracountry = df['intracountry_derived'].mode()[0]
    else:
        mode_intracountry = None
        print("Warning: 'issuing_country' or 'acquirer_country' columns missing.")

    # Output the results
    print("Average Scenario Parameters (Modes):")
    print(f"is_credit: {mode_is_credit}")
    print(f"aci: {mode_aci}")
    print(f"intracountry: {mode_intracountry}")
    
    # Print detailed counts for verification (as per Ground Truth in prompt)
    print("\nDetailed Counts:")
    print(f"is_credit:\n{df['is_credit'].value_counts()}")
    print(f"aci:\n{df['aci'].value_counts()}")
    if 'intracountry_derived' in df.columns:
        print(f"intracountry:\n{df['intracountry_derived'].value_counts()}")

if __name__ == "__main__":
    calculate_average_scenario_parameters()

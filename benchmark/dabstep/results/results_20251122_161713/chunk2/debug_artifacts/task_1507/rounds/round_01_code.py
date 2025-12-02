# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1507
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3512 characters (FULL CODE)
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
    # Load the payments dataset
    file_path = '/output/chunk2/data/context/payments.csv'
    df = pd.read_csv(file_path)
    
    # 1. Calculate Mode for Merchant
    # The most frequent merchant in the dataset
    mode_merchant = df['merchant'].mode()[0]
    count_merchant = df['merchant'].value_counts()[mode_merchant]
    
    # 2. Calculate Mode for is_credit
    # The most frequent credit status (True/False)
    mode_is_credit = df['is_credit'].mode()[0]
    count_is_credit = df['is_credit'].value_counts()[mode_is_credit]
    
    # 3. Calculate Mode for ACI
    # The most frequent Authorization Characteristics Indicator
    mode_aci = df['aci'].mode()[0]
    count_aci = df['aci'].value_counts()[mode_aci]
    
    # 4. Calculate Mode for Intracountry
    # Derived by comparing issuing_country and acquirer_country
    # True if they match (Domestic), False otherwise (Cross-border)
    df['intracountry_derived'] = df['issuing_country'] == df['acquirer_country']
    mode_intracountry = df['intracountry_derived'].mode()[0]
    count_intracountry = df['intracountry_derived'].value_counts()[mode_intracountry]
    
    # Output the results
    print("Average Scenario Parameters (Modes):")
    print(f"Merchant: {mode_merchant} (Count: {count_merchant})")
    print(f"Is Credit: {mode_is_credit} (Count: {count_is_credit})")
    print(f"ACI: {mode_aci} (Count: {count_aci})")
    print(f"Intracountry: {mode_intracountry} (Count: {count_intracountry})")
    
    # Return parameters as a dictionary for potential future use in the pipeline
    return {
        "merchant": mode_merchant,
        "is_credit": bool(mode_is_credit),
        "aci": mode_aci,
        "intracountry": bool(mode_intracountry)
    }

if __name__ == "__main__":
    calculate_average_scenario_parameters()

# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1517
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3165 characters (FULL CODE)
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
import json

def get_average_scenario():
    # Load payments data
    payments_path = '/output/chunk6/data/context/payments.csv'
    df = pd.read_csv(payments_path)
    
    # 1. Determine most frequent is_credit
    mode_is_credit = df['is_credit'].mode()[0]
    
    # 2. Determine most frequent aci
    mode_aci = df['aci'].mode()[0]
    
    # 3. Determine most frequent merchant
    mode_merchant = df['merchant'].mode()[0]
    
    # 4. Determine most frequent intracountry status
    # Intracountry is True if issuing_country == acquirer_country
    df['is_intracountry'] = df['issuing_country'] == df['acquirer_country']
    mode_intracountry = df['is_intracountry'].mode()[0]
    
    # 5. Get Merchant Category Code (MCC) for the most frequent merchant
    # This is needed for fee calculations in the next step
    merchant_data_path = '/output/chunk6/data/context/merchant_data.json'
    with open(merchant_data_path, 'r') as f:
        merchant_data = json.load(f)
        
    mode_mcc = None
    for m in merchant_data:
        if m['merchant'] == mode_merchant:
            mode_mcc = m['merchant_category_code']
            break
            
    # Print results to define the average scenario
    print("Average Scenario Parameters:")
    print(f"is_credit: {mode_is_credit}")
    print(f"aci: {mode_aci}")
    print(f"merchant: {mode_merchant}")
    print(f"mcc: {mode_mcc}")
    print(f"intracountry: {mode_intracountry}")

if __name__ == "__main__":
    get_average_scenario()

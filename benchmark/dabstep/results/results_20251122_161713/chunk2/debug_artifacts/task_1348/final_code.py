import pandas as pd
import json
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

# Step 1: Get the MCC for the specific description
df_mcc = pd.read_csv('/output/chunk2/data/context/merchant_category_codes.csv')
target_description = "Drinking Places (Alcoholic Beverages) - Bars, Taverns, Nightclubs, Cocktail Lounges, and Discotheques"

# Filter for the specific description
matching_mcc_row = df_mcc[df_mcc['description'] == target_description]

if matching_mcc_row.empty:
    # Fallback if exact string match fails (though previous step confirmed it exists)
    # Try partial match or exit
    print(f"Error: MCC description '{target_description}' not found.")
    mcc_code = None
else:
    mcc_code = int(matching_mcc_row.iloc[0]['mcc'])

if mcc_code is not None:
    # Step 2: Load fees and filter
    with open('/output/chunk2/data/context/fees.json', 'r') as f:
        fees_data = json.load(f)

    target_scheme = 'TransactPlus'
    target_account_type = 'H'
    transaction_value = 100.0
    
    calculated_fees = []

    for rule in fees_data:
        # Filter 1: Card Scheme
        if rule.get('card_scheme') != target_scheme:
            continue
            
        # Filter 2: Account Type
        # Logic: If list is empty or None, it applies to ALL (Wildcard). 
        # If list has values, 'H' must be in it.
        rule_account_types = rule.get('account_type')
        if is_not_empty(rule_account_types):
            if target_account_type not in rule_account_types:
                continue
        
        # Filter 3: Merchant Category Code
        # Logic: If list is empty or None, it applies to ALL (Wildcard).
        # If list has values, mcc_code must be in it.
        rule_mccs = rule.get('merchant_category_code')
        if is_not_empty(rule_mccs):
            if mcc_code not in rule_mccs:
                continue
        
        # Calculate Fee
        # Formula: fee = fixed_amount + rate * transaction_value / 10000
        fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
        rate = coerce_to_float(rule.get('rate', 0))
        
        fee = fixed_amount + (rate * transaction_value / 10000.0)
        calculated_fees.append(fee)

    # Step 3: Calculate Average
    if calculated_fees:
        average_fee = sum(calculated_fees) / len(calculated_fees)
        # Step 4: Output formatted result
        print(f"{average_fee:.6f}")
    else:
        print("No matching fee rules found.")
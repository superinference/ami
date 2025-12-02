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

def calculate_fee(amount, fee_rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = coerce_to_float(fee_rule.get('fixed_amount', 0))
    rate = coerce_to_float(fee_rule.get('rate', 0))
    # Formula from manual: fee = fixed_amount + rate * transaction_value / 10000
    return fixed + (rate * amount / 10000)

# --- Step 1: Find the MCC for "Eating Places and Restaurants" ---
mcc_df = pd.read_csv('/output/chunk4/data/context/merchant_category_codes.csv')

target_description = "Eating Places and Restaurants"
# Filter exactly
filtered_mcc = mcc_df[mcc_df['description'] == target_description]

if filtered_mcc.empty:
    # Fallback: try case insensitive or partial match if exact fails (though prompt implies exact)
    filtered_mcc = mcc_df[mcc_df['description'].str.contains(target_description, case=False, na=False)]

if filtered_mcc.empty:
    print(f"Error: Could not find MCC for description '{target_description}'")
    exit()

# Get the MCC code (assuming integer)
target_mcc = int(filtered_mcc.iloc[0]['mcc'])
print(f"Found MCC for '{target_description}': {target_mcc}")

# --- Step 2: Load Fees and Filter ---
with open('/output/chunk4/data/context/fees.json', 'r') as f:
    fees_data = json.load(f)

target_scheme = 'TransactPlus'
target_account_type = 'H'
transaction_amount = 50.0

matching_fees = []

for rule in fees_data:
    # 1. Check Card Scheme
    if rule.get('card_scheme') != target_scheme:
        continue
    
    # 2. Check Account Type (Wildcard [] matches ALL, otherwise must contain 'H')
    rule_account_types = rule.get('account_type')
    if is_not_empty(rule_account_types):
        if target_account_type not in rule_account_types:
            continue
            
    # 3. Check MCC (Wildcard [] matches ALL, otherwise must contain target_mcc)
    rule_mccs = rule.get('merchant_category_code')
    if is_not_empty(rule_mccs):
        if target_mcc not in rule_mccs:
            continue
            
    # If we passed all checks, this rule applies
    fee = calculate_fee(transaction_amount, rule)
    matching_fees.append(fee)

# --- Step 3: Calculate Average and Output ---
if not matching_fees:
    print("No matching fee rules found.")
else:
    average_fee = sum(matching_fees) / len(matching_fees)
    print(f"Number of matching rules: {len(matching_fees)}")
    print(f"{average_fee:.6f}")
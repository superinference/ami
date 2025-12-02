import pandas as pd
import json

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

# Step 1: Identify the MCC for "Taxicabs and Limousines"
mcc_df = pd.read_csv('/output/chunk2/data/context/merchant_category_codes.csv')
target_description = "Taxicabs and Limousines"

# Try exact match
mcc_row = mcc_df[mcc_df['description'] == target_description]

# If not found, try case-insensitive match
if mcc_row.empty:
    mcc_row = mcc_df[mcc_df['description'].str.lower() == target_description.lower()]

if mcc_row.empty:
    print(f"Error: Could not find MCC for description '{target_description}'")
    # Fallback logic if needed, but based on context, it should exist (likely 4121)
    # Let's try to find it via partial match just in case
    mcc_row = mcc_df[mcc_df['description'].str.contains(target_description, case=False, na=False)]

if mcc_row.empty:
    print("Critical Error: MCC not found even with partial match.")
    exit()

target_mcc = int(mcc_row.iloc[0]['mcc'])
# print(f"Identified MCC: {target_mcc}")

# Step 2: Load Fee Rules
with open('/output/chunk2/data/context/fees.json', 'r') as f:
    fees = json.load(f)

# Step 3: Filter Rules and Calculate Fees
# Criteria:
# - card_scheme: SwiftCharge
# - account_type: H (or wildcard)
# - merchant_category_code: target_mcc (or wildcard)

matching_fees = []
transaction_value = 1234.0
target_scheme = 'SwiftCharge'
target_account_type = 'H'

for rule in fees:
    # 1. Check Card Scheme
    if rule.get('card_scheme') != target_scheme:
        continue
    
    # 2. Check Account Type
    # If list is empty/None, it applies to ALL (Wildcard). 
    # If list is present, 'H' must be in it.
    rule_account_types = rule.get('account_type')
    if is_not_empty(rule_account_types):
        if target_account_type not in rule_account_types:
            continue
            
    # 3. Check Merchant Category Code
    # If list is empty/None, it applies to ALL (Wildcard).
    # If list is present, target_mcc must be in it.
    rule_mccs = rule.get('merchant_category_code')
    if is_not_empty(rule_mccs):
        if target_mcc not in rule_mccs:
            continue
    
    # If we passed all checks, calculate the fee for this rule
    fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
    rate = coerce_to_float(rule.get('rate', 0))
    
    # Fee formula: fixed + (rate * value / 10000)
    fee = fixed_amount + (rate * transaction_value / 10000.0)
    matching_fees.append(fee)

# Step 4: Calculate Average and Output
if not matching_fees:
    print("No matching fee rules found.")
else:
    average_fee = sum(matching_fees) / len(matching_fees)
    print(f"{average_fee:.6f}")
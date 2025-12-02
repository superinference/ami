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

# 1. Get the MCC code for the specific description
df_mcc = pd.read_csv('/output/chunk6/data/context/merchant_category_codes.csv')
target_description = "Drinking Places (Alcoholic Beverages) - Bars, Taverns, Nightclubs, Cocktail Lounges, and Discotheques"

# Filter for the description
matching_mcc_row = df_mcc[df_mcc['description'] == target_description]

if matching_mcc_row.empty:
    print(f"Error: Could not find MCC for description: {target_description}")
    # Fallback or exit - for this specific task, we expect a match.
    # Based on exploration, it is likely 5813.
    mcc_code = 5813 
else:
    mcc_code = int(matching_mcc_row.iloc[0]['mcc'])

# 2. Load Fee Rules
with open('/output/chunk6/data/context/fees.json', 'r') as f:
    fees_data = json.load(f)

# 3. Filter Rules and Calculate Fees
# Criteria:
# - Card Scheme: TransactPlus
# - Account Type: H (or wildcard)
# - MCC: mcc_code (or wildcard)
# - Transaction Value: 50 EUR

matching_fees = []
transaction_value = 50.0

for rule in fees_data:
    # Filter by Card Scheme
    if rule.get('card_scheme') != 'TransactPlus':
        continue
        
    # Filter by Account Type
    # Rule applies if account_type list is empty (wildcard) OR contains 'H'
    rule_account_types = rule.get('account_type')
    if is_not_empty(rule_account_types) and 'H' not in rule_account_types:
        continue
        
    # Filter by Merchant Category Code
    # Rule applies if mcc list is empty (wildcard) OR contains our mcc_code
    rule_mccs = rule.get('merchant_category_code')
    if is_not_empty(rule_mccs) and mcc_code not in rule_mccs:
        continue
        
    # If we reach here, the rule is applicable
    # Calculate Fee: fixed_amount + (rate * amount / 10000)
    fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
    rate = coerce_to_float(rule.get('rate', 0))
    
    fee = fixed_amount + (rate * transaction_value / 10000)
    matching_fees.append(fee)

# 4. Calculate Average and Output
if not matching_fees:
    print("No matching fee rules found.")
else:
    average_fee = sum(matching_fees) / len(matching_fees)
    print(f"{average_fee:.6f}")
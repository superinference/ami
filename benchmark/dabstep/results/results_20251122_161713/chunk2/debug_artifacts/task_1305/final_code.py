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

# 1. Get MCC for "Eating Places and Restaurants"
mcc_path = '/output/chunk2/data/context/merchant_category_codes.csv'
mcc_df = pd.read_csv(mcc_path)

target_description = "Eating Places and Restaurants"
matching_mcc_row = mcc_df[mcc_df['description'] == target_description]

if matching_mcc_row.empty:
    # Fallback if not found (though it should be there based on context)
    print(f"Warning: Description '{target_description}' not found in MCC file.")
    mcc = 5812 
else:
    mcc = int(matching_mcc_row.iloc[0]['mcc'])

# 2. Load Fees
fees_path = '/output/chunk2/data/context/fees.json'
with open(fees_path, 'r') as f:
    fees = json.load(f)

# 3. Filter Rules and Calculate Fees
matching_fees = []
target_scheme = 'GlobalCard'
target_account_type = 'H'
transaction_value = 10.0

for rule in fees:
    # Filter by Card Scheme
    if rule.get('card_scheme') != target_scheme:
        continue
    
    # Filter by Account Type (Wildcard check)
    # Rule applies if account_type is None/Empty OR contains 'H'
    rule_acct = rule.get('account_type')
    if is_not_empty(rule_acct):
        if target_account_type not in rule_acct:
            continue
            
    # Filter by MCC (Wildcard check)
    # Rule applies if merchant_category_code is None/Empty OR contains the MCC
    rule_mcc_list = rule.get('merchant_category_code')
    if is_not_empty(rule_mcc_list):
        if mcc not in rule_mcc_list:
            continue
    
    # Calculate Fee
    # Formula from manual: fee = fixed_amount + rate * transaction_value / 10000
    fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
    rate = coerce_to_float(rule.get('rate', 0))
    
    fee = fixed_amount + (rate * transaction_value / 10000)
    matching_fees.append(fee)

# 4. Compute Average and Print
if matching_fees:
    average_fee = sum(matching_fees) / len(matching_fees)
    print(f"{average_fee:.6f}")
else:
    print("No matching rules found")
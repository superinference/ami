import pandas as pd
import json

# --- Helper Functions ---
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

# --- Step 1: Identify MCC ---
# Load the merchant category codes dataset
df_mcc = pd.read_csv('/output/chunk5/data/context/merchant_category_codes.csv')

# Filter for the specific description 'Eating Places and Restaurants'
target_description = 'Eating Places and Restaurants'
mcc_row = df_mcc[df_mcc['description'] == target_description]

if not mcc_row.empty:
    target_mcc = int(mcc_row.iloc[0]['mcc'])
else:
    # Fallback if exact string match fails (though it should work based on previous context)
    # Based on standard MCC codes, Eating Places and Restaurants is 5812
    target_mcc = 5812

# --- Step 2: Load Fee Rules ---
with open('/output/chunk5/data/context/fees.json', 'r') as f:
    fees_data = json.load(f)

# --- Step 3 & 4: Filter Rules and Calculate Fees ---
target_scheme = 'TransactPlus'
target_account_type = 'H'
transaction_amount = 10.0
calculated_fees = []

for rule in fees_data:
    # 1. Check Card Scheme
    if rule.get('card_scheme') != target_scheme:
        continue

    # 2. Check Account Type
    # If list is empty or None, it applies to ALL account types (Wildcard).
    # If list is not empty, 'H' must be present to match.
    rule_account_types = rule.get('account_type')
    if is_not_empty(rule_account_types):
        if target_account_type not in rule_account_types:
            continue

    # 3. Check Merchant Category Code (MCC)
    # If list is empty or None, it applies to ALL MCCs (Wildcard).
    # If list is not empty, target_mcc must be present to match.
    rule_mccs = rule.get('merchant_category_code')
    if is_not_empty(rule_mccs):
        # Ensure we compare integers
        if target_mcc not in [int(x) for x in rule_mccs]:
            continue

    # --- Rule Matches! Calculate Fee ---
    # Formula: fee = fixed_amount + rate * transaction_value / 10000
    fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
    rate = coerce_to_float(rule.get('rate', 0))
    
    fee = fixed_amount + (rate * transaction_amount / 10000)
    calculated_fees.append(fee)

# --- Step 5 & 6: Calculate Average and Output ---
if calculated_fees:
    average_fee = sum(calculated_fees) / len(calculated_fees)
    print(f"{average_fee:.6f}")
else:
    print("0.000000")
import pandas as pd
import json

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------
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

# ---------------------------------------------------------
# MAIN ANALYSIS
# ---------------------------------------------------------

# File paths
fees_path = '/output/chunk2/data/context/fees.json'
payments_path = '/output/chunk2/data/context/payments.csv'
merchant_path = '/output/chunk2/data/context/merchant_data.json'

# 1. Load Data
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

df = pd.read_csv(payments_path)

# 2. Get Fee Rule 280 Details
fee_rule = next((f for f in fees_data if f['ID'] == 280), None)
if not fee_rule:
    print("Fee rule 280 not found")
    exit()

# 3. Get Merchant Details for 'Martinis_Fine_Steakhouse'
target_merchant = 'Martinis_Fine_Steakhouse'
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)

if not merchant_info:
    print(f"Merchant {target_merchant} not found")
    exit()

# 4. Check Static Merchant Applicability (MCC, Account Type)
# If the merchant's static attributes don't match the fee rule, the fee never applied, so delta is 0.

# Check Account Type (if rule specifies it)
rule_account_types = fee_rule.get('account_type')
if is_not_empty(rule_account_types):
    if merchant_info['account_type'] not in rule_account_types:
        print(f"{0.0:.14f}")
        exit()

# Check Merchant Category Code (if rule specifies it)
rule_mccs = fee_rule.get('merchant_category_code')
if is_not_empty(rule_mccs):
    if merchant_info['merchant_category_code'] not in rule_mccs:
        print(f"{0.0:.14f}")
        exit()

# 5. Filter Transactions
# Start with Merchant and Year
txs = df[
    (df['merchant'] == target_merchant) & 
    (df['year'] == 2023)
].copy()

# Filter by Card Scheme
if fee_rule.get('card_scheme'):
    txs = txs[txs['card_scheme'] == fee_rule['card_scheme']]

# Filter by Credit/Debit (is_credit)
# Note: JSON uses boolean true/false or null
if fee_rule.get('is_credit') is not None:
    txs = txs[txs['is_credit'] == fee_rule['is_credit']]

# Filter by ACI (Authorization Characteristics Indicator)
rule_aci = fee_rule.get('aci')
if is_not_empty(rule_aci):
    txs = txs[txs['aci'].isin(rule_aci)]

# Filter by Intracountry
# Rule: True if issuing_country == acquirer_country
rule_intra = fee_rule.get('intracountry')
if rule_intra is not None:
    # Determine if transaction is intracountry
    is_intra_tx = txs['issuing_country'] == txs['acquirer_country']
    
    # rule_intra might be 0.0 (False) or 1.0 (True)
    if float(rule_intra) == 1.0:
        txs = txs[is_intra_tx]
    elif float(rule_intra) == 0.0:
        txs = txs[~is_intra_tx]

# 6. Calculate Delta
# Formula: Fee = Fixed + (Rate * Amount / 10000)
# Delta = New_Fee - Old_Fee
# Since Fixed amount doesn't change, Delta = (New_Rate - Old_Rate) * Amount / 10000

original_rate = coerce_to_float(fee_rule['rate'])
new_rate = 1.0
rate_diff = new_rate - original_rate

total_volume = txs['eur_amount'].sum()

delta = (rate_diff * total_volume) / 10000

# Output with high precision as required for delta/financial calculations
print(f"{delta:.14f}")
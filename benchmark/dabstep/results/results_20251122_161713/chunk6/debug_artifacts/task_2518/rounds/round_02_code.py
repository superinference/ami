# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2518
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 6519 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════
def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value) if value is not None else 0.0

def is_not_empty(array):
    """Check if array/list is not empty."""
    if array is None:
        return False
    if hasattr(array, 'size'):
        return array.size > 0
    try:
        return len(array) > 0
    except TypeError:
        return False

def match_fee_rule(transaction_dict, fee_rule):
    """
    Determines if a transaction matches a specific fee rule.
    Matches on: card_scheme, is_credit, aci, account_type, mcc, intracountry.
    Ignores volume/fraud thresholds for this specific 'what if' scenario unless specified.
    """
    # 1. Card Scheme
    if fee_rule.get('card_scheme') and fee_rule['card_scheme'] != transaction_dict.get('card_scheme'):
        return False

    # 2. Is Credit (Boolean or None for wildcard)
    if fee_rule.get('is_credit') is not None:
        if fee_rule['is_credit'] != transaction_dict.get('is_credit'):
            return False

    # 3. ACI (List or None/Empty for wildcard)
    if is_not_empty(fee_rule.get('aci')):
        if transaction_dict.get('aci') not in fee_rule['aci']:
            return False

    # 4. Account Type (List or None/Empty for wildcard)
    # Requires transaction_dict to have 'account_type' from merchant_data
    if is_not_empty(fee_rule.get('account_type')):
        if transaction_dict.get('account_type') not in fee_rule['account_type']:
            return False

    # 5. Merchant Category Code (List or None/Empty for wildcard)
    # Requires transaction_dict to have 'mcc' from merchant_data
    if is_not_empty(fee_rule.get('merchant_category_code')):
        if transaction_dict.get('mcc') not in fee_rule['merchant_category_code']:
            return False

    # 6. Intracountry (Boolean or None for wildcard)
    # Requires transaction_dict to have 'is_intracountry' calculated
    if fee_rule.get('intracountry') is not None:
        # Convert boolean to float 1.0/0.0 if needed, or compare bools
        rule_intra = fee_rule['intracountry']
        tx_intra = transaction_dict.get('is_intracountry')
        
        # Handle cases where json might have 1.0/0.0 instead of true/false
        if isinstance(rule_intra, (int, float)):
            rule_intra = bool(rule_intra)
        
        if rule_intra != tx_intra:
            return False

    return True

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# File Paths
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'

# 1. Load Data
print("Loading data...")
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Get Target Fee (ID=787)
target_fee_id = 787
fee_787 = next((f for f in fees_data if f['ID'] == target_fee_id), None)

if not fee_787:
    print(f"Error: Fee ID {target_fee_id} not found.")
    exit()

print(f"Found Fee ID {target_fee_id}:")
print(json.dumps(fee_787, indent=2))

old_rate = fee_787['rate']
new_rate = 99
print(f"Old Rate: {old_rate}, New Rate: {new_rate}")

# 3. Get Merchant Metadata for Rafa_AI
target_merchant = 'Rafa_AI'
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)

if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
    exit()

rafa_account_type = merchant_info['account_type']
rafa_mcc = merchant_info['merchant_category_code']
print(f"Rafa_AI Metadata - Account Type: {rafa_account_type}, MCC: {rafa_mcc}")

# 4. Filter Transactions for Rafa_AI in 2023
# We filter first by merchant/year to reduce processing
rafa_txs = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == 2023)
].copy()

print(f"Total 2023 transactions for {target_merchant}: {len(rafa_txs)}")

# 5. Identify Matching Transactions
# We need to augment transactions with merchant metadata and derived fields (intracountry) to use the matcher
matching_indices = []

# Pre-calculate intracountry for the block
# Intracountry = (issuing_country == acquirer_country)
rafa_txs['is_intracountry'] = rafa_txs['issuing_country'] == rafa_txs['acquirer_country']

for idx, row in rafa_txs.iterrows():
    # Create a dictionary for the matcher
    tx_dict = {
        'card_scheme': row['card_scheme'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'account_type': rafa_account_type,
        'mcc': rafa_mcc,
        'is_intracountry': row['is_intracountry']
    }
    
    if match_fee_rule(tx_dict, fee_787):
        matching_indices.append(idx)

matching_txs = rafa_txs.loc[matching_indices]
print(f"Transactions matching Fee {target_fee_id}: {len(matching_txs)}")

# 6. Calculate Delta
# Formula: Fee = Fixed + (Rate * Amount / 10000)
# Delta = New_Fee - Old_Fee
# Delta = (Fixed + New_Rate * Amt / 10000) - (Fixed + Old_Rate * Amt / 10000)
# Delta = (New_Rate - Old_Rate) * Amt / 10000

if len(matching_txs) > 0:
    total_volume = matching_txs['eur_amount'].sum()
    print(f"Total Volume of matching transactions: {total_volume:.2f} EUR")
    
    # Calculate delta with high precision
    rate_diff = new_rate - old_rate
    delta = (rate_diff * total_volume) / 10000.0
    
    print(f"Delta Calculation: ({new_rate} - {old_rate}) * {total_volume} / 10000")
    print(f"{delta:.14f}") # Print with high precision as requested by anti-patterns
else:
    print("No matching transactions found. Delta is 0.")
    print("0.00000000000000")

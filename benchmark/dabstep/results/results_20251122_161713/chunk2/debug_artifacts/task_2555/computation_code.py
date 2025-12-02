import pandas as pd
import json

# Helper functions for robust data processing
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
        return float(v)
    return float(value)

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

# Load Data
fees_path = '/output/chunk2/data/context/fees.json'
merchant_path = '/output/chunk2/data/context/merchant_data.json'
payments_path = '/output/chunk2/data/context/payments.csv'

with open(fees_path, 'r') as f:
    fees_data = json.load(f)

with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

df = pd.read_csv(payments_path)

# 1. Get Rule 64
rule_64 = next((f for f in fees_data if f['ID'] == 64), None)
if not rule_64:
    print("Rule 64 not found.")
    exit()

# 2. Create Merchant Lookup (Name -> AccountType)
# We need to look up the account type for every merchant found in the transactions
merchant_lookup = {
    m['merchant']: m.get('account_type')
    for m in merchant_data
}

# 3. Filter Transactions based on Rule 64's TRANSACTION-LEVEL criteria
# These are the criteria that must be met regardless of the merchant's account type.
# Criteria: card_scheme, is_credit, aci

# Filter by Card Scheme
if rule_64.get('card_scheme'):
    df = df[df['card_scheme'] == rule_64['card_scheme']]

# Filter by Is Credit (Handle boolean/null)
if rule_64.get('is_credit') is not None:
    # Ensure strict boolean comparison
    target_credit = rule_64['is_credit']
    df = df[df['is_credit'] == target_credit]

# Filter by ACI (Rule has list, CSV has single value)
if is_not_empty(rule_64.get('aci')):
    allowed_acis = set(rule_64['aci'])
    df = df[df['aci'].isin(allowed_acis)]

# Filter by MCC (Rule has list, Merchant has single value)
# Note: MCC is a merchant property, but often checked alongside transaction filters
if is_not_empty(rule_64.get('merchant_category_code')):
    allowed_mccs = set(rule_64['merchant_category_code'])
    # Get merchants from merchant_data that match these MCCs
    valid_mcc_merchants = {
        m['merchant'] for m in merchant_data 
        if m.get('merchant_category_code') in allowed_mccs
    }
    df = df[df['merchant'].isin(valid_mcc_merchants)]

# 4. Identify Affected Merchants
# Get list of merchants who actually processed transactions matching the criteria
active_merchants = df['merchant'].unique()

affected_merchants = []

for merchant in active_merchants:
    # Get merchant's account type
    acct_type = merchant_lookup.get(merchant)
    
    # --- Logic for CURRENT Rule ---
    # If rule['account_type'] is empty/null, it applies to ALL.
    # If it has values, merchant must match one.
    current_rule_ats = rule_64.get('account_type', [])
    if not is_not_empty(current_rule_ats):
        matches_current = True
    else:
        matches_current = acct_type in current_rule_ats
        
    # --- Logic for NEW Rule ---
    # Rule is ONLY applied to account type 'H'
    matches_new = (acct_type == 'H')
    
    # --- Determine if Affected ---
    # Affected if status changes (True->False OR False->True)
    if matches_current != matches_new:
        affected_merchants.append(merchant)

# 5. Output Result
if affected_merchants:
    print(", ".join(sorted(affected_merchants)))
else:
    print("None")
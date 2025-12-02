# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2555
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 4150 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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

# 2. Create Merchant Lookup (Name -> {AccountType, MCC})
merchant_lookup = {
    m['merchant']: {
        'account_type': m['account_type'],
        'mcc': m['merchant_category_code']
    } 
    for m in merchant_data
}

# 3. Filter Transactions based on Rule 64's STATIC criteria
# (card_scheme, is_credit, aci, merchant_category_code)
# We do NOT filter by account_type yet, as we need to compare "Current" vs "New" logic.

# Filter by Card Scheme
if rule_64.get('card_scheme'):
    df = df[df['card_scheme'] == rule_64['card_scheme']]

# Filter by Is Credit
if rule_64.get('is_credit') is not None:
    df = df[df['is_credit'] == rule_64['is_credit']]

# Filter by ACI (Rule has list, CSV has single value)
if is_not_empty(rule_64.get('aci')):
    allowed_acis = set(rule_64['aci'])
    df = df[df['aci'].isin(allowed_acis)]

# Filter by MCC (Rule has list, Merchant has single value)
if is_not_empty(rule_64.get('merchant_category_code')):
    allowed_mccs = set(rule_64['merchant_category_code'])
    # Identify merchants in the current filtered DF that match the MCC criteria
    valid_merchants_mcc = [
        m for m in df['merchant'].unique()
        if merchant_lookup.get(m, {}).get('mcc') in allowed_mccs
    ]
    df = df[df['merchant'].isin(valid_merchants_mcc)]

# At this point, 'df' contains all transactions that match the Rule's transaction-level criteria.
# Now we determine which merchants are affected by the Account Type change.

potential_merchants = df['merchant'].unique()

# Set 1: Merchants currently matching Rule 64 (Original Logic)
current_rule_ats = rule_64.get('account_type', [])
current_matches = set()

for m in potential_merchants:
    m_at = merchant_lookup.get(m, {}).get('account_type')
    # If rule has no account_type restriction (empty), it matches everyone in potential_merchants
    if not is_not_empty(current_rule_ats):
        current_matches.add(m)
    # If rule has restrictions, check if merchant's AT is in the list
    elif m_at in current_rule_ats:
        current_matches.add(m)

# Set 2: Merchants matching if Rule 64 is ONLY applied to 'H' (New Logic)
new_matches = set()
for m in potential_merchants:
    m_at = merchant_lookup.get(m, {}).get('account_type')
    if m_at == 'H':
        new_matches.add(m)

# Affected = Symmetric Difference
# (Those who paid and stop) U (Those who didn't pay and start)
affected_merchants = current_matches.symmetric_difference(new_matches)

# Output
if len(affected_merchants) > 0:
    print(", ".join(sorted(list(affected_merchants))))
else:
    print("None")

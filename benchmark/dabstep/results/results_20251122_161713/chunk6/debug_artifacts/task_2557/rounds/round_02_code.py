# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2557
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 4653 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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

# ---------------------------------------------------------
# MAIN ANALYSIS
# ---------------------------------------------------------

# 1. Load Data
try:
    fees_data = json.load(open('/output/chunk6/data/context/fees.json', 'r'))
    merchant_data = json.load(open('/output/chunk6/data/context/merchant_data.json', 'r'))
    payments_df = pd.read_csv('/output/chunk6/data/context/payments.csv')
except FileNotFoundError as e:
    print(f"Error loading files: {e}")
    exit()

# 2. Get Fee 64 Configuration
fee_64 = next((f for f in fees_data if f['ID'] == 64), None)

if not fee_64:
    print("Fee ID 64 not found in fees.json")
    exit()

# print(f"DEBUG: Fee 64 Criteria: Scheme={fee_64.get('card_scheme')}, Credit={fee_64.get('is_credit')}, ACI={fee_64.get('aci')}, MCC={fee_64.get('merchant_category_code')}")

# 3. Create Merchant Lookup Map
# We need to look up MCC and Account Type by merchant name
merchant_lookup = {m['merchant']: m for m in merchant_data}

# 4. Filter Payments to find transactions matching Fee 64's TRANSACTION-LEVEL criteria
# Criteria: card_scheme, is_credit, aci
# Note: We do not filter by merchant-level criteria (MCC, account_type) yet, as those are properties of the merchant, not the transaction row.

filtered_txs = payments_df.copy()

# Filter by Card Scheme
if fee_64.get('card_scheme'):
    filtered_txs = filtered_txs[filtered_txs['card_scheme'] == fee_64['card_scheme']]

# Filter by Is Credit
if fee_64.get('is_credit') is not None:
    filtered_txs = filtered_txs[filtered_txs['is_credit'] == fee_64['is_credit']]

# Filter by ACI
# Fee definition has ACI as a list (e.g., ['D', 'A']). Transaction has single value.
if is_not_empty(fee_64.get('aci')):
    filtered_txs = filtered_txs[filtered_txs['aci'].isin(fee_64['aci'])]

# Get unique merchants from these matching transactions
candidate_merchants = filtered_txs['merchant'].unique()

# 5. Identify Affected Merchants
# Logic: 
#   a) Merchant must match ORIGINAL Fee 64 merchant-level criteria (MCC, original Account Type).
#      If they don't match originally, they aren't "using" the fee, so they aren't affected by a change to it.
#   b) Merchant must NOT match the NEW criteria (Account Type == 'S').
#      If they are 'S', they keep the fee (not affected/excluded). If they are not 'S', they lose it (affected).

affected_merchants = []

for merchant_name in candidate_merchants:
    m_info = merchant_lookup.get(merchant_name)
    
    if not m_info:
        continue

    # --- Check 5a: Does merchant match ORIGINAL Fee 64 criteria? ---
    
    # Check MCC
    if is_not_empty(fee_64.get('merchant_category_code')):
        if m_info['merchant_category_code'] not in fee_64['merchant_category_code']:
            continue # Merchant wasn't subject to this fee originally (wrong MCC)

    # Check Original Account Type (if Fee 64 had restrictions before)
    if is_not_empty(fee_64.get('account_type')):
        if m_info['account_type'] not in fee_64['account_type']:
            continue # Merchant wasn't subject to this fee originally (wrong Account Type)

    # --- Check 5b: Is merchant affected by the NEW restriction (Only 'S')? ---
    # If their account type is NOT 'S', they are affected (excluded).
    
    if m_info['account_type'] != 'S':
        affected_merchants.append(merchant_name)

# 6. Output Results
if not affected_merchants:
    print("No merchants would be affected.")
else:
    # Sort for consistent output
    affected_merchants.sort()
    print(", ".join(affected_merchants))

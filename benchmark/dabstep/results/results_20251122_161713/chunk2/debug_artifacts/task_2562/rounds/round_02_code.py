# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2562
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 5986 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json

# ═══════════════════════════════════════════════════════════
# Helper Functions
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

def is_match(value, criteria):
    """
    Generic matcher for fee rules.
    - If criteria is None or Empty List: Match (Wildcard)
    - If criteria is List: Match if value in list
    - If criteria is Scalar: Match if value == criteria
    """
    if criteria is None or criteria == [] or criteria == "":
        return True
    if isinstance(criteria, list):
        return value in criteria
    return value == criteria

# ═══════════════════════════════════════════════════════════
# Main Analysis Script
# ═══════════════════════════════════════════════════════════

# 1. Load Data
try:
    fees_path = '/output/chunk2/data/context/fees.json'
    merchants_path = '/output/chunk2/data/context/merchant_data.json'
    payments_path = '/output/chunk2/data/context/payments.csv'

    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    
    with open(merchants_path, 'r') as f:
        merchant_data = json.load(f)
        
    df_payments = pd.read_csv(payments_path)
    
    # Create Merchant Lookup Dictionary: Name -> {Details}
    merchant_lookup = {m['merchant']: m for m in merchant_data}

except Exception as e:
    print(f"Error loading data: {e}")
    exit()

# 2. Get Fee 384 Criteria
fee_384 = next((f for f in fees_data if f['ID'] == 384), None)

if not fee_384:
    print("Fee ID 384 not found.")
    exit()

print(f"Analyzing Fee ID 384: {json.dumps(fee_384, indent=2)}")

# 3. Filter Transactions (2023 Only)
# We only care about 2023 as per the question
df_2023 = df_payments[df_payments['year'] == 2023].copy()

# 4. Identify Merchants Matching Fee 384's CURRENT Criteria
# We filter the dataframe to find transactions that match the fee's rules.
# Note: We do NOT filter by account_type here, because we want to see who matches the *other* criteria first.

# 4a. Match Transaction-Level Criteria
# Criteria: card_scheme, is_credit, aci, intracountry
# Note: intracountry requires calculation (issuing_country == acquirer_country)

# Calculate intracountry for the dataset
df_2023['is_intracountry'] = df_2023['issuing_country'] == df_2023['acquirer_country']

# Apply Filters
# Card Scheme
if fee_384.get('card_scheme'):
    df_2023 = df_2023[df_2023['card_scheme'] == fee_384['card_scheme']]

# Is Credit
if fee_384.get('is_credit') is not None:
    df_2023 = df_2023[df_2023['is_credit'] == fee_384['is_credit']]

# ACI (List check)
if fee_384.get('aci'):
    # Filter rows where the transaction's ACI is in the fee's ACI list
    df_2023 = df_2023[df_2023['aci'].isin(fee_384['aci'])]

# Intracountry
if fee_384.get('intracountry') is not None:
    # If fee requires intracountry (True) or international (False)
    target_intra = fee_384['intracountry']
    # Handle string '0.0'/'1.0' or bool
    if isinstance(target_intra, str):
        target_intra = (float(target_intra) == 1.0)
    df_2023 = df_2023[df_2023['is_intracountry'] == target_intra]

# 4b. Match Merchant-Level Criteria
# Criteria: merchant_category_code, capture_delay
# We iterate through the remaining unique merchants and check their static data.

potential_merchants = df_2023['merchant'].unique()
matching_merchants = []

for merchant_name in potential_merchants:
    m_info = merchant_lookup.get(merchant_name)
    if not m_info:
        continue

    # Check Merchant Category Code (MCC)
    # Fee MCC is a list of allowed codes. Merchant MCC is a single int.
    fee_mcc = fee_384.get('merchant_category_code')
    merchant_mcc = m_info.get('merchant_category_code')
    
    if fee_mcc and merchant_mcc not in fee_mcc:
        continue # MCC doesn't match

    # Check Capture Delay
    fee_delay = fee_384.get('capture_delay')
    merchant_delay = m_info.get('capture_delay')
    
    if fee_delay and fee_delay != merchant_delay:
        # Note: Simple equality check. If ranges are involved, more complex logic needed.
        # Based on file analysis, exact string matches (e.g. "manual") are common.
        continue

    # If we reached here, the merchant matches all CURRENT criteria of Fee 384
    matching_merchants.append(merchant_name)

print(f"\nMerchants currently matching Fee 384 (before account_type change): {matching_merchants}")

# 5. Determine "Affected" Merchants
# The change: Fee 384 is now ONLY applied to account_type 'F'.
# Affected Merchants = Those who matched before, but have account_type != 'F'.

affected_merchants = []

for merchant_name in matching_merchants:
    m_info = merchant_lookup.get(merchant_name)
    account_type = m_info.get('account_type')
    
    # If the merchant is NOT type F, they lose the fee -> Affected
    if account_type != 'F':
        affected_merchants.append(merchant_name)

# 6. Output Results
print("\nAnalysis Results:")
print("-" * 30)
if affected_merchants:
    print("The following merchants would be affected (they currently match Fee 384 but are not Account Type F):")
    # Sort for consistent output
    affected_merchants.sort()
    print(", ".join(affected_merchants))
else:
    print("No merchants would be affected. All currently matching merchants are already Account Type F.")

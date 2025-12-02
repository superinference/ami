# ═══════════════════════════════════════════════════════════
# Round 4 - Task 1365
# ═══════════════════════════════════════════════════════════
# Temperature: 0.35
# Verification: insufficient
# Plan steps: 2
# Code length: 3068 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except:
                return 0.0
        try:
            return float(v)
        except:
            return 0.0
    return 0.0

# --- Main Analysis ---

# 1. Identify MCC for the specific description
mcc_df = pd.read_csv('/output/chunk3/data/context/merchant_category_codes.csv')
target_description = "Drinking Places (Alcoholic Beverages) - Bars, Taverns, Nightclubs, Cocktail Lounges, and Discotheques"

# Find the row matching the description
mcc_row = mcc_df[mcc_df['description'] == target_description]

if mcc_row.empty:
    print(f"Error: MCC not found for description: {target_description}")
    exit()

target_mcc = int(mcc_row.iloc[0]['mcc'])

# 2. Load Fee Rules
with open('/output/chunk3/data/context/fees.json') as f:
    fees = json.load(f)

# 3. Find Applicable Rules and Calculate Fees
# We are looking for rules that *could* apply to a merchant with Account Type 'H' and the specific MCC.
# Since the question is hypothetical ("what would be the average fee"), we consider all rules 
# that match the known criteria (Scheme, Account Type, MCC) and average them, 
# effectively averaging across the unknown variables (Volume, Fraud, etc.).

applicable_fees = []
target_tx_value = 4321.0

for rule in fees:
    # 1. Check Card Scheme (Must be GlobalCard)
    if rule.get('card_scheme') != 'GlobalCard':
        continue
        
    # 2. Check Account Type
    # Rule applies if:
    # - account_type is None (Wildcard)
    # - account_type is empty list (Wildcard)
    # - 'H' is in account_type list
    r_acc = rule.get('account_type')
    if r_acc is not None and isinstance(r_acc, list) and len(r_acc) > 0:
        if 'H' not in r_acc:
            continue
            
    # 3. Check MCC
    # Rule applies if:
    # - mcc is None (Wildcard)
    # - mcc is empty list (Wildcard)
    # - target_mcc is in mcc list
    r_mcc = rule.get('merchant_category_code')
    if r_mcc is not None and isinstance(r_mcc, list) and len(r_mcc) > 0:
        if target_mcc not in r_mcc:
            continue
            
    # 4. Calculate Fee for this rule
    # Formula: fee = fixed_amount + rate * transaction_value / 10000
    fixed = coerce_to_float(rule.get('fixed_amount'))
    rate = coerce_to_float(rule.get('rate'))
    
    fee = fixed + (rate * target_tx_value / 10000.0)
    applicable_fees.append(fee)

# 4. Calculate Average and Output
if not applicable_fees:
    print("No applicable fee rules found.")
else:
    avg_fee = np.mean(applicable_fees)
    print(f"{avg_fee:.6f} EUR")

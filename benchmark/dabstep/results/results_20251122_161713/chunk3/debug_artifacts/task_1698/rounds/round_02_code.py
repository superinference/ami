# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1698
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7781 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
    if pd.isna(value) or value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v:
            return float(v.replace('k', '')) * 1000
        if 'm' in v:
            return float(v.replace('m', '')) * 1000000
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range(rule_value, actual_value):
    """
    Check if actual_value fits into rule_value range.
    rule_value examples: '100k-1m', '>8.3%', '3-5', 'immediate'
    """
    if rule_value is None:
        return True
    
    # Handle exact string matches for non-numeric rules (like 'immediate', 'manual')
    if isinstance(rule_value, str) and isinstance(actual_value, str):
        if rule_value == actual_value:
            return True
        # If one is numeric string and other is not, continue to numeric parsing
        # But 'manual' vs 'immediate' should fail here if not equal
        if not any(c.isdigit() for c in rule_value) and not any(c.isdigit() for c in actual_value):
            return False

    # Convert actual_value to float if possible for numeric comparison
    try:
        actual_num = float(actual_value)
    except (ValueError, TypeError):
        # If actual value isn't a number (e.g. 'manual'), and we didn't match above, 
        # we can't compare with a range like '>5'.
        return False

    # Handle inequalities
    if isinstance(rule_value, str):
        if rule_value.startswith('>'):
            limit = coerce_to_float(rule_value[1:])
            return actual_num > limit
        if rule_value.startswith('<'):
            limit = coerce_to_float(rule_value[1:])
            return actual_num < limit
        
        # Handle ranges "min-max"
        if '-' in rule_value:
            parts = rule_value.split('-')
            if len(parts) == 2:
                min_val = coerce_to_float(parts[0])
                max_val = coerce_to_float(parts[1])
                return min_val <= actual_num <= max_val

    # Fallback exact match for numbers
    return actual_num == coerce_to_float(rule_value)

def match_fee_rule(transaction, rule):
    """
    Check if a fee rule applies to a specific transaction context.
    transaction: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != transaction.get('card_scheme'):
        return False

    # 2. Account Type (List match, empty = wildcard)
    if rule.get('account_type'):
        if transaction.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if transaction.get('mcc') not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match, None = wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != transaction.get('is_credit'):
            return False

    # 5. ACI (List match, empty = wildcard)
    if rule.get('aci'):
        if transaction.get('aci') not in rule['aci']:
            return False

    # 6. Intracountry (Boolean/Float match, None = wildcard)
    if rule.get('intracountry') is not None:
        # Convert rule value to bool (0.0 -> False, 1.0 -> True)
        rule_intra = bool(rule['intracountry'])
        if rule_intra != transaction.get('intracountry'):
            return False

    # 7. Capture Delay (Range/Exact match)
    if rule.get('capture_delay'):
        if not parse_range(rule['capture_delay'], transaction.get('capture_delay')):
            return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not parse_range(rule['monthly_volume'], transaction.get('monthly_volume')):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        # Fraud level in rule is usually %, e.g., ">8.3%"
        # Transaction fraud level should be a float ratio (e.g., 0.0841)
        if not parse_range(rule['monthly_fraud_level'], transaction.get('monthly_fraud_level')):
            return False

    return True

# ---------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------

# Paths
payments_path = '/output/chunk3/data/context/payments.csv'
merchant_data_path = '/output/chunk3/data/context/merchant_data.json'
fees_path = '/output/chunk3/data/context/fees.json'

# Load Data
df_payments = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees = json.load(f)

# Target Parameters
target_merchant = 'Golfclub_Baron_Friso'
target_day = 365
target_year = 2023

# 1. Get Merchant Static Data
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found.")
    exit()

mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

# 2. Calculate Monthly Metrics (December 2023)
# Day 365 is in December. We need stats for the whole month of December.
# December days in non-leap year: 335 to 365.
december_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= 335) &
    (df_payments['day_of_year'] <= 365)
]

monthly_volume = december_txs['eur_amount'].sum()
fraud_volume = december_txs[december_txs['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_level = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 3. Filter Transactions for the Specific Day (365)
day_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] == target_day)
].copy()

# Calculate intracountry for these transactions
day_txs['intracountry'] = day_txs['issuing_country'] == day_txs['acquirer_country']

# 4. Find Applicable Fee IDs
applicable_fee_ids = set()

# We iterate through unique profiles to save time, as many transactions are identical for fee purposes
# Fee determinants: card_scheme, is_credit, aci, intracountry
# (Static determinants: mcc, account_type, capture_delay, monthly_volume, monthly_fraud_level)

unique_profiles = day_txs[['card_scheme', 'is_credit', 'aci', 'intracountry']].drop_duplicates()

for _, row in unique_profiles.iterrows():
    # Construct transaction context dictionary
    tx_context = {
        'card_scheme': row['card_scheme'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': row['intracountry'],
        'mcc': mcc,
        'account_type': account_type,
        'capture_delay': capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level
    }
    
    # Check against all fee rules
    for rule in fees:
        if match_fee_rule(tx_context, rule):
            applicable_fee_ids.add(rule['ID'])

# 5. Output Result
sorted_ids = sorted(list(applicable_fee_ids))
print(", ".join(map(str, sorted_ids)))

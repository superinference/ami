# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1716
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7505 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json

# --- Helper Functions ---

def coerce_to_float(value):
    """
    Robustly converts a value to float, handling:
    - Strings with currency symbols ($, €)
    - Strings with percentages (%) -> divides by 100
    - Strings with suffixes (k, m) -> multiplies by 1000, 1000000
    - Comparison operators (>, <) -> strips them
    """
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v.lower():
            return float(v.lower().replace('k', '')) * 1000
        if 'm' in v.lower():
            return float(v.lower().replace('m', '')) * 1000000
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range(rule_value, actual_value):
    """
    Checks if actual_value fits into rule_value range/condition.
    rule_value examples: '100k-1m', '<3', '>5', '3-5', 'immediate', 'manual', '0.0%-0.5%'
    actual_value: float or string
    """
    if rule_value is None:
        return True
    
    # Handle exact string matches (e.g. 'immediate', 'manual')
    if isinstance(rule_value, str) and rule_value.isalpha():
        return str(actual_value).lower() == rule_value.lower()

    # Convert actual to float for numeric comparisons
    try:
        act_float = float(actual_value)
    except (ValueError, TypeError):
        # If actual is not a number (and rule wasn't alpha match above), fail unless rule is None
        return False

    rv = str(rule_value).strip()
    
    # Range "min-max"
    if '-' in rv:
        try:
            parts = rv.split('-')
            # Handle negative numbers or complex ranges if necessary, but standard format is "min-max"
            # For "0.0%-0.5%", split works.
            min_val = coerce_to_float(parts[0])
            max_val = coerce_to_float(parts[1])
            return min_val <= act_float <= max_val
        except:
            return False
            
    # Inequality "<X", ">X"
    if rv.startswith('<'):
        limit = coerce_to_float(rv[1:])
        return act_float < limit
    if rv.startswith('>'):
        limit = coerce_to_float(rv[1:])
        return act_float > limit
        
    # Exact numeric match
    return act_float == coerce_to_float(rv)

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List in rule, single in tx)
    # Rule: [] or None means ALL. Else must contain tx value.
    if rule.get('account_type') and tx_ctx['account_type'] not in rule['account_type']:
        return False
        
    # 3. Merchant Category Code (List in rule, single in tx)
    if rule.get('merchant_category_code') and tx_ctx['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 4. ACI (List in rule, single in tx)
    if rule.get('aci') and tx_ctx['aci'] not in rule['aci']:
        return False
        
    # 5. Is Credit (Bool)
    if rule.get('is_credit') is not None and rule['is_credit'] != tx_ctx['is_credit']:
        return False
        
    # 6. Intracountry (Bool)
    # Intracountry = (Issuer Country == Acquirer Country)
    is_intra = (tx_ctx['issuing_country'] == tx_ctx['acquirer_country'])
    if rule.get('intracountry') is not None:
        # Convert rule value to bool (0.0 -> False, 1.0 -> True)
        rule_intra = bool(float(rule['intracountry']))
        if rule_intra != is_intra:
            return False

    # 7. Capture Delay (Rule is range/string, Tx is merchant attribute)
    if not parse_range(rule.get('capture_delay'), tx_ctx['capture_delay']):
        return False
        
    # 8. Monthly Volume (Rule is range, Tx is calculated stat)
    if not parse_range(rule.get('monthly_volume'), tx_ctx['monthly_volume']):
        return False
        
    # 9. Monthly Fraud Level (Rule is range, Tx is calculated stat)
    if not parse_range(rule.get('monthly_fraud_level'), tx_ctx['monthly_fraud_rate']):
        return False
        
    return True

def calculate_fee(amount, rule):
    """
    Calculates fee based on fixed amount and rate.
    Fee = Fixed + (Rate * Amount / 10000)
    """
    fixed = rule.get('fixed_amount', 0.0) or 0.0
    rate = rule.get('rate', 0.0) or 0.0
    return fixed + (rate * amount / 10000.0)

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk2/data/context/payments.csv'
merchant_path = '/output/chunk2/data/context/merchant_data.json'
fees_path = '/output/chunk2/data/context/fees.json'

df_payments = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees = json.load(f)

# 2. Define Target
target_merchant = 'Belles_cookbook_store'
target_year = 2023
target_day = 365

# 3. Get Merchant Attributes
# Find the merchant dictionary in the list
merch_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merch_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

# 4. Calculate Monthly Stats (December 2023)
# Manual states: "Monthly volumes and rates are computed always in natural months"
# 2023 is not a leap year.
# Dec 1 is Day 335. Dec 31 is Day 365.
dec_start = 335
dec_end = 365

dec_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= dec_start) &
    (df_payments['day_of_year'] <= dec_end)
]

monthly_vol = dec_txs['eur_amount'].sum()
monthly_fraud_count = dec_txs['has_fraudulent_dispute'].sum()
monthly_tx_count = len(dec_txs)
monthly_fraud_rate = (monthly_fraud_count / monthly_tx_count) if monthly_tx_count > 0 else 0.0

# Debug prints
# print(f"Merchant: {target_merchant}")
# print(f"Dec Volume: {monthly_vol}")
# print(f"Dec Fraud Rate: {monthly_fraud_rate:.4%}")

# 5. Filter Target Transactions (Day 365)
target_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] == target_day)
]

# 6. Calculate Fees
total_fees = 0.0

for _, tx in target_txs.iterrows():
    # Build context for matching
    tx_ctx = {
        'card_scheme': tx['card_scheme'],
        'aci': tx['aci'],
        'is_credit': tx['is_credit'],
        'issuing_country': tx['issuing_country'],
        'acquirer_country': tx['acquirer_country'],
        'eur_amount': tx['eur_amount'],
        # Merchant attributes
        'account_type': merch_info['account_type'],
        'mcc': merch_info['merchant_category_code'],
        'capture_delay': merch_info['capture_delay'],
        # Monthly stats
        'monthly_volume': monthly_vol,
        'monthly_fraud_rate': monthly_fraud_rate
    }
    
    matched_rule = None
    # Iterate fees to find the first matching rule
    for rule in fees:
        if match_fee_rule(tx_ctx, rule):
            matched_rule = rule
            break 
            
    if matched_rule:
        fee = calculate_fee(tx['eur_amount'], matched_rule)
        total_fees += fee
    else:
        # Fallback or error logging if needed
        pass

# 7. Output Result
print(f"{total_fees:.2f}")

# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1866
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: sufficient
# Plan steps: 2
# Code length: 8515 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    
    s = str(value).strip().lower()
    s = s.replace(',', '').replace('€', '').replace('$', '').replace('%', '')
    s = s.replace('>', '').replace('<', '').replace('=', '').replace('≥', '').replace('≤', '')
    
    multiplier = 1
    if s.endswith('k'):
        multiplier = 1000
        s = s[:-1]
    elif s.endswith('m'):
        multiplier = 1000000
        s = s[:-1]
        
    try:
        return float(s) * multiplier
    except ValueError:
        return 0.0

def parse_range_check(value, rule_string):
    """
    Checks if a numeric value fits within a rule string.
    Rule strings can be: '100k-1m', '>5', '<3', '7.7%-8.3%', 'immediate', 'manual'
    """
    if rule_string is None:
        return True
        
    # Handle exact string matches for non-numeric rules (e.g., 'manual', 'immediate')
    if isinstance(value, str) and isinstance(rule_string, str):
        if value.lower() == rule_string.lower():
            return True
        # If value is 'manual' but rule is numeric/range, it's a mismatch
        # Unless the rule is also just a string like "manual" which is caught above.
        # If rule is ">5" and value is "manual", we can't compare easily, usually False or specific logic.
        # For capture_delay, merchant has "manual", rule might be "manual".
        # If merchant has "1", rule ">5", we convert merchant to float.
        try:
            num_val = float(value)
        except ValueError:
            return False # Value is non-numeric string, rule is likely numeric/range -> mismatch
    elif isinstance(value, (int, float)):
        num_val = float(value)
    else:
        return False

    s = str(rule_string).strip().lower()
    
    # Handle inequalities
    if s.startswith('>'):
        limit = coerce_to_float(s)
        return num_val > limit
    if s.startswith('<'):
        limit = coerce_to_float(s)
        return num_val < limit
    if s.startswith('≥') or s.startswith('>='):
        limit = coerce_to_float(s)
        return num_val >= limit
    if s.startswith('≤') or s.startswith('<='):
        limit = coerce_to_float(s)
        return num_val <= limit
        
    # Handle ranges (e.g., "100k-1m", "3-5", "7.7%-8.3%")
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            min_val = coerce_to_float(parts[0])
            max_val = coerce_to_float(parts[1])
            return min_val <= num_val <= max_val
            
    # Handle exact numeric match (as string)
    return num_val == coerce_to_float(s)

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_ctx: Dictionary containing transaction and merchant details.
    rule: Dictionary containing the fee rule.
    """
    # 1. Card Scheme (Exact Match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False

    # 2. Account Type (List Membership)
    # Rule has list of allowed types. Merchant has one type.
    # If rule list is empty/null, it matches all.
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List Membership)
    # Rule has list of allowed MCCs. Merchant has one MCC.
    if rule.get('merchant_category_code'):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean Match)
    # If rule is None, matches both.
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 5. ACI (List Membership)
    # Rule has list of allowed ACIs. Transaction has one ACI.
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Boolean Match)
    if rule.get('intracountry') is not None:
        if rule['intracountry'] != tx_ctx['intracountry']:
            return False

    # 7. Capture Delay (Complex Match)
    # Compares merchant's delay setting to rule's requirement
    if rule.get('capture_delay'):
        if not parse_range_check(tx_ctx['capture_delay'], rule['capture_delay']):
            return False

    # 8. Monthly Volume (Range Match)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range Match)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_ctx['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Manual: "Variable rate to be especified to be multiplied by the transaction value and divided by 10000."
    variable = (rate * amount) / 10000.0
    return fixed + variable

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'
fees_path = '/output/chunk6/data/context/fees.json'

df_payments = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_rules = json.load(f)

# 2. Filter for Rafa_AI and August 2023
# August is roughly day 213 to 243 (Non-leap year 2023)
# Jan: 31, Feb: 28, Mar: 31, Apr: 30, May: 31, Jun: 30, Jul: 31
# Sum: 31+28+31+30+31+30+31 = 212. So August 1st is day 213.
# August has 31 days. 212 + 31 = 243. So August 31st is day 243.
target_merchant = 'Rafa_AI'
df_rafa = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == 2023) &
    (df_payments['day_of_year'] >= 213) & 
    (df_payments['day_of_year'] <= 243)
].copy()

# 3. Get Merchant Metadata
merchant_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

# 4. Calculate Monthly Stats (Volume & Fraud)
# Volume is sum of eur_amount.
monthly_volume = df_rafa['eur_amount'].sum()

# Fraud rate is percentage of transactions with fraudulent dispute (Volume based).
# Manual Section 7: "Fraud is defined as the ratio of fraudulent volume over total volume."
fraud_volume = df_rafa[df_rafa['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_rate_decimal = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0
monthly_fraud_rate_percent = monthly_fraud_rate_decimal * 100

# 5. Calculate Fees
total_fees = 0.0
matched_count = 0
unmatched_count = 0

# Pre-process merchant info for speed
m_account_type = merchant_info.get('account_type')
m_mcc = merchant_info.get('merchant_category_code')
m_capture_delay = merchant_info.get('capture_delay')

for _, row in df_rafa.iterrows():
    # Construct Transaction Context
    # Intracountry: Issuer == Acquirer
    is_intracountry = (row['issuing_country'] == row['acquirer_country'])
    
    tx_ctx = {
        'card_scheme': row['card_scheme'],
        'account_type': m_account_type,
        'mcc': m_mcc,
        'is_credit': bool(row['is_credit']),
        'aci': row['aci'],
        'intracountry': is_intracountry,
        'capture_delay': m_capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate_percent # Rules use % strings like "8.3%" which coerce_to_float handles as 8.3
    }
    
    # Find first matching rule
    fee_applied = 0.0
    rule_found = False
    
    for rule in fees_rules:
        if match_fee_rule(tx_ctx, rule):
            fee_applied = calculate_fee(row['eur_amount'], rule)
            rule_found = True
            break # Apply first match only
            
    if rule_found:
        total_fees += fee_applied
        matched_count += 1
    else:
        unmatched_count += 1

# Final Answer Formatting
print(f"{total_fees:.2f}")

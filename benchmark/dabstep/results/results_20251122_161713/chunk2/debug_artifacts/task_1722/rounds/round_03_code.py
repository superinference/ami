# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1722
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: sufficient
# Plan steps: 2
# Code length: 7940 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

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
            return float(v.replace('%', '')) / 100
        # Range handling (e.g., "50-60") - return mean for simple coercion
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
    return 0.0

def parse_range_check(value, rule_range_str):
    """
    Checks if a numeric value falls within a range string.
    Handles: '100k-1m', '>5', '<3', '7.7%-8.3%', 'immediate', 'manual'
    """
    if not rule_range_str:  # None or empty matches all
        return True
    
    s = str(rule_range_str).lower().strip()
    
    # Handle non-numeric specific strings (e.g., capture_delay='manual')
    if s in ['immediate', 'manual']:
        # If the value passed in is also a string (like 'manual'), check equality
        if isinstance(value, str):
            return s == value.lower()
        return False

    # If value is a string (e.g. 'manual') but rule is numeric range (e.g. '>5'), return False
    if isinstance(value, str) and not value.replace('.','',1).isdigit():
        return False

    try:
        # Normalize k/m suffixes
        s = s.replace(',', '')
        
        # Handle percentages in rule
        is_percent = '%' in s
        s = s.replace('%', '')
        
        # Scale value if rule is percent (assuming value is 0-1 ratio)
        check_val = value * 100 if is_percent else value
        
        # Helper to parse number with k/m
        def parse_num(n_str):
            n_str = n_str.strip()
            mult = 1
            if 'k' in n_str:
                mult = 1000
                n_str = n_str.replace('k', '')
            elif 'm' in n_str:
                mult = 1000000
                n_str = n_str.replace('m', '')
            return float(n_str) * mult

        if '-' in s:
            parts = s.split('-')
            low = parse_num(parts[0])
            high = parse_num(parts[1])
            return low <= check_val <= high
        elif '>' in s:
            limit = parse_num(s.replace('>', ''))
            return check_val > limit
        elif '<' in s:
            limit = parse_num(s.replace('<', ''))
            return check_val < limit
        else:
            # Exact numeric match
            return check_val == parse_num(s)
    except:
        return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_context must contain: 
      card_scheme, account_type, mcc, is_credit, aci, 
      intracountry, capture_delay, monthly_volume, monthly_fraud_rate
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match, wildcard=[])
    if rule.get('account_type'): # If rule has specific types (not empty list)
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. MCC (List match, wildcard=[])
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Bool match, wildcard=None)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False
            
    # 5. ACI (List match, wildcard=[])
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Bool/Float match, wildcard=None)
    # fees.json uses 0.0/1.0 for bools often
    if rule.get('intracountry') is not None:
        rule_intra = bool(float(rule['intracountry']))
        if rule_intra != tx_context['intracountry']:
            return False
            
    # 7. Capture Delay (String/Range match, wildcard=None)
    if rule.get('capture_delay'):
        if not parse_range_check(tx_context['capture_delay'], rule['capture_delay']):
            return False
            
    # 8. Monthly Volume (Range check)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_context['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 9. Monthly Fraud Level (Range check)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_context['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False
            
    return True

# ═══════════════════════════════════════════════════════════
# MAIN LOGIC
# ═══════════════════════════════════════════════════════════

# File paths
payments_path = '/output/chunk2/data/context/payments.csv'
fees_path = '/output/chunk2/data/context/fees.json'
merchant_path = '/output/chunk2/data/context/merchant_data.json'

# 1. Load Data
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Get Merchant Context for 'Crossfit_Hanna'
target_merchant = 'Crossfit_Hanna'
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)

if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

# 3. Calculate Monthly Stats for December 2023
# Day 365 is in December. December starts day 335 (365-30).
# Exact: Jan(31)+Feb(28)+Mar(31)+Apr(30)+May(31)+Jun(30)+Jul(31)+Aug(31)+Sep(30)+Oct(31)+Nov(30) = 334
# So Dec 1 is Day 335.
dec_start = 335
dec_end = 365

df_december = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == 2023) &
    (df_payments['day_of_year'] >= dec_start) &
    (df_payments['day_of_year'] <= dec_end)
]

monthly_volume = df_december['eur_amount'].sum()
monthly_fraud_count = df_december['has_fraudulent_dispute'].sum()
monthly_tx_count = len(df_december)
monthly_fraud_rate = monthly_fraud_count / monthly_tx_count if monthly_tx_count > 0 else 0.0

# 4. Filter Target Transactions (Day 365)
df_target = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == 2023) &
    (df_payments['day_of_year'] == 365)
]

# 5. Calculate Fees
total_fees = 0.0

for _, tx in df_target.iterrows():
    # Build context for this specific transaction
    tx_context = {
        'card_scheme': tx['card_scheme'],
        'account_type': merchant_info['account_type'],
        'mcc': merchant_info['merchant_category_code'],
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': tx['issuing_country'] == tx['acquirer_country'],
        'capture_delay': merchant_info['capture_delay'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate
    }
    
    # Find matching rule
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            matched_rule = rule
            break # Stop at first match
            
    if matched_rule:
        # Calculate fee: fixed + (rate * amount / 10000)
        fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * tx['eur_amount'] / 10000)
        total_fees += fee

# Final Answer Output
print(f"{total_fees:.2f}")

# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1688
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8231 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
        # Range handling (e.g., "50-60") - return mean for single value conversion, 
        # but for parsing ranges in rules, we need a different approach.
        # This function is primarily for converting single scalar values.
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        return float(v)
    return float(value)

def parse_range(range_str):
    """Parses a range string like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip().lower()
    
    # Handle suffixes
    def parse_val(val_str):
        val_str = val_str.strip()
        multiplier = 1
        if val_str.endswith('k'):
            multiplier = 1000
            val_str = val_str[:-1]
        elif val_str.endswith('m'):
            multiplier = 1000000
            val_str = val_str[:-1]
        elif val_str.endswith('%'):
            multiplier = 0.01
            val_str = val_str[:-1]
        return float(val_str) * multiplier

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif s.startswith('>'):
        return parse_val(s[1:]), float('inf')
    elif s.startswith('<'):
        return float('-inf'), parse_val(s[1:])
    else:
        # Exact match treated as range [val, val]
        try:
            val = parse_val(s)
            return val, val
        except:
            return None, None

def check_range(value, range_str):
    """Checks if a numeric value falls within a rule's range string."""
    if range_str is None:
        return True
    
    # Handle special non-numeric cases if necessary, though usually these are numeric ranges
    # For capture_delay, it might be 'manual' or 'immediate' vs numeric ranges
    if isinstance(value, str) and not value.replace('.','',1).isdigit():
        # Value is string (e.g. 'manual'), rule is string
        return value.lower() == range_str.lower()
    
    # If value is numeric (or string number) and rule is numeric range
    try:
        num_val = float(value)
        min_v, max_v = parse_range(range_str)
        if min_v is None: # Parsing failed, maybe exact string match required
             return str(value).lower() == str(range_str).lower()
        return min_v <= num_val <= max_v
    except:
        return str(value).lower() == str(range_str).lower()

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_context must contain:
    - card_scheme, account_type, capture_delay, monthly_fraud_level, monthly_volume,
      merchant_category_code, is_credit, aci, intracountry
    """
    
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match - if rule has list, merchant type must be in it)
    # Wildcard: Empty list or None in rule means ALL allowed
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 3. Capture Delay (Range/Value match)
    # Wildcard: None in rule
    if rule.get('capture_delay'):
        if not check_range(tx_context['capture_delay'], rule['capture_delay']):
            return False

    # 4. Monthly Fraud Level (Range match)
    # Wildcard: None in rule
    if rule.get('monthly_fraud_level'):
        if not check_range(tx_context['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    # 5. Monthly Volume (Range match)
    # Wildcard: None in rule
    if rule.get('monthly_volume'):
        if not check_range(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 6. Merchant Category Code (List match)
    # Wildcard: Empty list or None
    if rule.get('merchant_category_code'):
        if tx_context['merchant_category_code'] not in rule['merchant_category_code']:
            return False

    # 7. Is Credit (Boolean match)
    # Wildcard: None
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 8. ACI (List match)
    # Wildcard: Empty list or None
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 9. Intracountry (Boolean match)
    # Wildcard: None
    if rule.get('intracountry') is not None:
        # Rule expects 1.0/0.0 or True/False. Convert to bool for comparison
        rule_intra = bool(rule['intracountry'])
        tx_intra = bool(tx_context['intracountry'])
        if rule_intra != tx_intra:
            return False

    return True

# ---------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk3/data/context/payments.csv'
fees_path = '/output/chunk3/data/context/fees.json'
merchant_data_path = '/output/chunk3/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Define Target
target_merchant = "Crossfit_Hanna"
target_year = 2023
target_day = 12

# 3. Get Merchant Attributes
merchant_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not merchant_info:
    print(f"Merchant {target_merchant} not found in merchant_data.json")
    exit()

m_account_type = merchant_info['account_type']
m_mcc = merchant_info['merchant_category_code']
m_capture_delay = merchant_info['capture_delay']

# 4. Calculate Monthly Stats (January 2023)
# Day 12 is in January. We need stats for the full month of January (Days 1-31).
jan_payments = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= 1) &
    (df_payments['day_of_year'] <= 31)
]

monthly_volume = jan_payments['eur_amount'].sum()
fraud_volume = jan_payments[jan_payments['has_fraudulent_dispute'] == True]['eur_amount'].sum()

# Avoid division by zero
monthly_fraud_level = (fraud_volume / monthly_volume) if monthly_volume > 0 else 0.0

# 5. Filter Transactions for Target Day
day_payments = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] == target_day)
]

# 6. Find Applicable Fee IDs
applicable_fee_ids = set()

# Iterate through each transaction on that day
for _, tx in day_payments.iterrows():
    # Construct context for this transaction
    # Note: Intracountry is True if issuing_country == acquirer_country
    is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
    
    tx_context = {
        'card_scheme': tx['card_scheme'],
        'account_type': m_account_type,
        'capture_delay': m_capture_delay,
        'monthly_fraud_level': monthly_fraud_level,
        'monthly_volume': monthly_volume,
        'merchant_category_code': m_mcc,
        'is_credit': bool(tx['is_credit']),
        'aci': tx['aci'],
        'intracountry': is_intracountry
    }
    
    # Check against all fee rules
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            applicable_fee_ids.add(rule['ID'])

# 7. Output Result
# Sort IDs for consistent output
sorted_ids = sorted(list(applicable_fee_ids))
print(", ".join(map(str, sorted_ids)))

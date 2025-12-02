# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1791
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8499 characters (FULL CODE)
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
        # Range handling (e.g., "50-60") - return mean
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        return float(v)
    return float(value)

def parse_range(range_str, value_type='float'):
    """
    Parses a range string like '100k-1m' or '7.7%-8.3%' or '>5'.
    Returns (min_val, max_val).
    """
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    # Handle k/m suffixes for volume
    def parse_val(x):
        x = x.strip()
        if x.endswith('%'):
            return float(x.replace('%', '')) / 100.0
        factor = 1
        if x.endswith('k'):
            factor = 1000
            x = x[:-1]
        elif x.endswith('m'):
            factor = 1000000
            x = x[:-1]
        return float(x) * factor

    if '>' in s:
        val = parse_val(s.replace('>', ''))
        return val, float('inf')
    if '<' in s:
        val = parse_val(s.replace('<', ''))
        return float('-inf'), val
    
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            return parse_val(parts[0]), parse_val(parts[1])
            
    # Exact match or immediate/manual (handled elsewhere usually, but for numeric ranges)
    try:
        val = parse_val(s)
        return val, val
    except:
        return None, None

def check_range(value, range_str):
    """Checks if a value falls within a range string."""
    if range_str is None:
        return True
    min_v, max_v = parse_range(range_str)
    if min_v is None: # Not a standard range string, maybe categorical like 'immediate'
        return str(value).lower() == str(range_str).lower()
    return min_v <= value <= max_v

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    tx_context must contain:
      - card_scheme
      - account_type
      - merchant_category_code
      - capture_delay
      - monthly_volume
      - monthly_fraud_level
      - is_credit
      - aci
      - intracountry
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match)
    if rule.get('account_type') and tx_context['account_type'] not in rule['account_type']:
        return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code') and tx_context['merchant_category_code'] not in rule['merchant_category_code']:
        return False

    # 4. Capture Delay (Exact match or wildcard)
    # Note: capture_delay in fees.json can be ranges like '>5', but merchant data is usually specific 'immediate', 'manual', '1'.
    # However, looking at fees.json, capture_delay values are '3-5', '>5', '<3', 'immediate', 'manual'.
    # Merchant data has 'immediate', 'manual', '1', '2', '7'.
    # We need to map merchant specific days to the rule categories if necessary, or just string match.
    # Let's handle the numeric mapping for days.
    
    rule_delay = rule.get('capture_delay')
    if rule_delay:
        merch_delay = str(tx_context['capture_delay'])
        if merch_delay.isdigit():
            days = int(merch_delay)
            if rule_delay == '>5' and days > 5: pass
            elif rule_delay == '<3' and days < 3: pass
            elif rule_delay == '3-5' and 3 <= days <= 5: pass
            else: return False # Numeric mismatch
        else:
            # String match (immediate, manual)
            if rule_delay != merch_delay:
                return False

    # 5. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not check_range(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 6. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not check_range(tx_context['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    # 7. Is Credit (Boolean match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 8. ACI (List match)
    if rule.get('aci') and tx_context['aci'] not in rule['aci']:
        return False

    # 9. Intracountry (Boolean match)
    # In fees.json, intracountry is 0.0 (False) or 1.0 (True) or null
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False

    return True

# ---------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------

# 1. Load Data
df_payments = pd.read_csv('/output/chunk6/data/context/payments.csv')
with open('/output/chunk6/data/context/fees.json', 'r') as f:
    fees_data = json.load(f)
with open('/output/chunk6/data/context/merchant_data.json', 'r') as f:
    merchant_data = json.load(f)

# 2. Define Context
target_merchant = 'Martinis_Fine_Steakhouse'
target_year = 2023
# May is days 121 to 151 (non-leap year)
start_day = 121
end_day = 151

# 3. Filter Payments for Merchant and Month
# We need the whole month to calculate volume/fraud stats correctly
df_month = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= start_day) &
    (df_payments['day_of_year'] <= end_day)
].copy()

if df_month.empty:
    print("No transactions found for this merchant in May 2023.")
else:
    # 4. Calculate Monthly Stats (Volume & Fraud)
    # Manual Section 7: "Fraud is defined as the ratio of fraudulent volume over total volume."
    total_volume = df_month['eur_amount'].sum()
    fraud_volume = df_month[df_month['has_fraudulent_dispute']]['eur_amount'].sum()
    
    monthly_fraud_ratio = fraud_volume / total_volume if total_volume > 0 else 0.0
    
    # 5. Get Static Merchant Attributes
    merch_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
    if not merch_info:
        print(f"Merchant {target_merchant} not found in merchant_data.json")
        exit()

    # 6. Identify Applicable Fee IDs
    # We must check every transaction because fees depend on dynamic attributes (scheme, credit, aci, intracountry)
    applicable_fee_ids = set()

    # Optimization: Get unique transaction profiles to reduce iterations
    # Profile keys: card_scheme, is_credit, aci, issuing_country, acquirer_country
    # We need issuing/acquirer to determine intracountry
    df_month['intracountry'] = df_month['issuing_country'] == df_month['acquirer_country']
    
    unique_tx_profiles = df_month[[
        'card_scheme', 'is_credit', 'aci', 'intracountry'
    ]].drop_duplicates()

    for _, tx in unique_tx_profiles.iterrows():
        # Build context for matching
        context = {
            'card_scheme': tx['card_scheme'],
            'account_type': merch_info['account_type'],
            'merchant_category_code': merch_info['merchant_category_code'],
            'capture_delay': merch_info['capture_delay'],
            'monthly_volume': total_volume,
            'monthly_fraud_level': monthly_fraud_ratio,
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'intracountry': tx['intracountry']
        }
        
        # Check against all rules
        for rule in fees_data:
            if match_fee_rule(context, rule):
                applicable_fee_ids.add(rule['ID'])

    # 7. Output Result
    # Sort IDs for consistent output
    sorted_ids = sorted(list(applicable_fee_ids))
    
    # Print simply the list of IDs as requested by "What were the applicable Fee IDs"
    print(", ".join(map(str, sorted_ids)))

# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2538
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 10362 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ==========================================
# HELPER FUNCTIONS
# ==========================================

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

def parse_range_check(value, range_str, is_percentage=False):
    """
    Parses a range string (e.g., '100k-1m', '>5', '7.7%-8.3%') and checks if value is within it.
    Returns True if value matches or if range_str is None (wildcard).
    """
    if range_str is None:
        return True
    
    # Normalize string
    s = str(range_str).lower().strip().replace(',', '').replace('€', '').replace('$', '')
    
    # Handle multipliers
    multiplier = 1
    if not is_percentage:
        if 'k' in s: multiplier = 1000; s = s.replace('k', '')
        if 'm' in s: multiplier = 1000000; s = s.replace('m', '')
    
    # Handle percentage conversion for the value being checked
    # If the rule is percentage (e.g. "8%"), the input value should be comparable (e.g. 0.08)
    # The parsing below handles the string side.
    
    try:
        if '-' in s:
            parts = s.split('-')
            low_str = parts[0].strip()
            high_str = parts[1].strip()
            
            low = float(low_str.replace('%', '')) * multiplier
            high = float(high_str.replace('%', '')) * multiplier
            
            if is_percentage and '%' in range_str:
                low /= 100
                high /= 100
                
            return low <= value <= high
            
        elif '>' in s:
            limit_str = s.replace('>', '').strip()
            limit = float(limit_str.replace('%', '')) * multiplier
            if is_percentage and '%' in range_str:
                limit /= 100
            return value > limit
            
        elif '<' in s:
            limit_str = s.replace('<', '').strip()
            limit = float(limit_str.replace('%', '')) * multiplier
            if is_percentage and '%' in range_str:
                limit /= 100
            return value < limit
            
        else:
            # Exact match (rare for these fields, but possible)
            target = float(s.replace('%', '')) * multiplier
            if is_percentage and '%' in range_str:
                target /= 100
            return value == target
            
    except ValueError:
        return False

def match_fee_rule(tx_context, rule):
    """
    Checks if a transaction context matches a fee rule.
    tx_context must contain: 
      card_scheme, account_type, capture_delay, monthly_fraud_rate, 
      monthly_volume, mcc, is_credit, aci, intracountry
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match - Rule has list of allowed types)
    # If rule['account_type'] is empty/None, it applies to all.
    if rule['account_type'] and len(rule['account_type']) > 0:
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 3. Capture Delay (String/Range match)
    # merchant_data has specific value (e.g. "manual"), rule has criteria (e.g. "manual" or ">5")
    # Simplified: exact match or simple logic. 
    # Based on file analysis, capture_delay in fees.json can be null, "immediate", "manual", ">5", "<3", "3-5"
    if rule['capture_delay']:
        rd = rule['capture_delay']
        td = str(tx_context['capture_delay'])
        if rd != td:
            # Handle numeric comparisons if delay is numeric-like
            # But merchant data has "manual", "immediate", "1", "2".
            # Let's try direct match first, then logic.
            matched = False
            if rd == td: matched = True
            elif rd == 'manual' and td == 'manual': matched = True
            elif rd == 'immediate' and td == 'immediate': matched = True
            elif any(c in rd for c in ['<', '>', '-']):
                # Try to convert merchant delay to int
                try:
                    delay_days = float(td)
                    matched = parse_range_check(delay_days, rd, is_percentage=False)
                except ValueError:
                    matched = False # Merchant delay is "manual"/"immediate" but rule is numeric range
            if not matched:
                return False

    # 4. Monthly Fraud Level (Range match)
    if rule['monthly_fraud_level']:
        if not parse_range_check(tx_context['monthly_fraud_rate'], rule['monthly_fraud_level'], is_percentage=True):
            return False

    # 5. Monthly Volume (Range match)
    if rule['monthly_volume']:
        if not parse_range_check(tx_context['monthly_volume'], rule['monthly_volume'], is_percentage=False):
            return False

    # 6. Merchant Category Code (List match)
    # Rule has list of MCCs. Merchant has one MCC.
    if rule['merchant_category_code'] and len(rule['merchant_category_code']) > 0:
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False

    # 7. Is Credit (Boolean match)
    # If rule['is_credit'] is None, applies to both.
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 8. ACI (List match)
    if rule['aci'] and len(rule['aci']) > 0:
        if tx_context['aci'] not in rule['aci']:
            return False

    # 9. Intracountry (Boolean match)
    if rule['intracountry'] is not None:
        # rule['intracountry'] might be 0.0 (False) or 1.0 (True) or boolean
        rule_intra = bool(float(rule['intracountry'])) if isinstance(rule['intracountry'], (int, float, str)) else rule['intracountry']
        if rule_intra != tx_context['intracountry']:
            return False

    return True

def calculate_fee_amount(amount, rule):
    """Calculates fee: fixed + (rate * amount / 10000)"""
    fixed = float(rule['fixed_amount'])
    rate = float(rule['rate'])
    return fixed + (rate * amount / 10000.0)

# ==========================================
# MAIN SCRIPT
# ==========================================

# 1. Load Data
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_path = '/output/chunk5/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data_list = json.load(f)

# 2. Filter for Merchant and Year
target_merchant = 'Golfclub_Baron_Friso'
target_year = 2023

df_filtered = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
].copy()

# 3. Get Merchant Context (Static Data)
merchant_info = next((item for item in merchant_data_list if item["merchant"] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

original_mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']
new_mcc = 8062

# 4. Preprocessing: Add Month and Intracountry
# Convert day_of_year to month. 2023 is not a leap year.
# Simple trick: create a date range for 2023 and map day_of_year
dates = pd.date_range(start='2023-01-01', end='2023-12-31')
doy_to_month = {i+1: d.month for i, d in enumerate(dates)}
df_filtered['month'] = df_filtered['day_of_year'].map(doy_to_month)

# Intracountry: issuing == acquirer
df_filtered['intracountry'] = df_filtered['issuing_country'] == df_filtered['acquirer_country']

# 5. Calculate Monthly Stats (Volume and Fraud Rate)
# Fraud Rate = Volume of Fraud / Total Volume (as per manual.md)
monthly_stats = {}
grouped = df_filtered.groupby('month')

for month, group in grouped:
    total_vol = group['eur_amount'].sum()
    fraud_vol = group[group['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    monthly_stats[month] = {
        'volume': total_vol,
        'fraud_rate': fraud_rate
    }

# 6. Calculate Fees for Both Scenarios
total_fee_original = 0.0
total_fee_new = 0.0

# Iterate through every transaction
for _, tx in df_filtered.iterrows():
    month = tx['month']
    stats = monthly_stats.get(month, {'volume': 0, 'fraud_rate': 0})
    
    # Base Context
    base_context = {
        'card_scheme': tx['card_scheme'],
        'account_type': account_type,
        'capture_delay': capture_delay,
        'monthly_fraud_rate': stats['fraud_rate'],
        'monthly_volume': stats['volume'],
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': tx['intracountry']
    }
    
    # Scenario A: Original MCC
    context_original = base_context.copy()
    context_original['mcc'] = original_mcc
    
    rule_original = None
    for rule in fees_data:
        if match_fee_rule(context_original, rule):
            rule_original = rule
            break
            
    if rule_original:
        total_fee_original += calculate_fee_amount(tx['eur_amount'], rule_original)
    else:
        # Fallback or error if no rule matches? 
        # Assuming dataset is complete, but good to track.
        pass

    # Scenario B: New MCC
    context_new = base_context.copy()
    context_new['mcc'] = new_mcc
    
    rule_new = None
    for rule in fees_data:
        if match_fee_rule(context_new, rule):
            rule_new = rule
            break
            
    if rule_new:
        total_fee_new += calculate_fee_amount(tx['eur_amount'], rule_new)

# 7. Calculate Delta
delta = total_fee_new - total_fee_original

# Output Result
print(f"{delta:.14f}")

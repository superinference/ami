# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2520
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9356 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import os
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
        v = v.lstrip('><≤≥')  # Remove comparison operators for simple conversion
        if '%' in v:
            return float(v.replace('%', '')) / 100
        # Range handling (e.g., "50-60") - return mean for simple coercion, 
        # but specific logic needed for range matching
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                # Check if parts are numbers
                return (float(parts[0].strip().replace('%','')) + float(parts[1].strip().replace('%',''))) / 2
            except:
                pass
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value)

def parse_volume_string(vol_str):
    """Parses volume strings like '100k', '1m' into floats."""
    if not isinstance(vol_str, str):
        return float(vol_str)
    s = vol_str.lower().strip()
    multiplier = 1
    if s.endswith('k'):
        multiplier = 1000
        s = s[:-1]
    elif s.endswith('m'):
        multiplier = 1000000
        s = s[:-1]
    return float(s) * multiplier

def check_range_match(rule_val, actual_val, is_percentage=False):
    """
    Checks if actual_val matches the rule_val range/condition.
    rule_val examples: '100k-1m', '>5', '<3', '7.7%-8.3%', 'immediate', 'manual'
    """
    if rule_val is None:
        return True
    
    # Handle exact string matches (e.g., 'immediate', 'manual')
    if isinstance(rule_val, str) and rule_val in ['immediate', 'manual']:
        return str(actual_val) == rule_val

    # Convert actual_val to float for numeric comparison
    try:
        # If actual value is 'immediate'/'manual' but rule is numeric/range, it's a mismatch
        if isinstance(actual_val, str) and actual_val in ['immediate', 'manual']:
            return False
        val = float(actual_val)
    except (ValueError, TypeError):
        return False

    # Handle numeric rules
    if isinstance(rule_val, (int, float)):
        return val == float(rule_val)

    s = str(rule_val).strip()
    
    # Handle percentages in rule
    if '%' in s:
        is_percentage = True
        s = s.replace('%', '')
    
    # Adjust actual value if percentage comparison expected
    # If rule was '8%', it's now '8'. If actual is 0.08, we convert to 8.0 for comparison
    # OR we convert rule to 0.08. Let's convert rule to float.
    
    # Helper to parse rule number
    def parse_rule_num(n_str):
        f = parse_volume_string(n_str)
        if is_percentage:
            return f / 100.0
        return f

    if '-' in s:
        # Range: "min-max"
        parts = s.split('-')
        if len(parts) == 2:
            min_v = parse_rule_num(parts[0])
            max_v = parse_rule_num(parts[1])
            return min_v <= val <= max_v
            
    elif s.startswith('>'):
        limit = parse_rule_num(s[1:])
        return val > limit
    elif s.startswith('<'):
        limit = parse_rule_num(s[1:])
        return val < limit
    else:
        # Exact match string number
        return val == parse_rule_num(s)
    
    return False

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a transaction context matches a fee rule.
    tx_ctx: dict containing transaction details + monthly stats
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False

    # 2. Account Type (List match)
    # Rule has list (e.g., ['F', 'D']) or None. Merchant has string (e.g., 'F').
    if rule['account_type']:
        if tx_ctx['account_type'] not in rule['account_type']:
            return False

    # 3. Capture Delay (Complex match)
    if rule['capture_delay']:
        if not check_range_match(rule['capture_delay'], tx_ctx['capture_delay']):
            return False

    # 4. Merchant Category Code (List match)
    if rule['merchant_category_code']:
        if tx_ctx['merchant_category_code'] not in rule['merchant_category_code']:
            return False

    # 5. Is Credit (Bool match)
    # Rule can be True, False, or None
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 6. ACI (List match)
    if rule['aci']:
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 7. Intracountry (Bool match)
    # Rule can be 0.0 (False), 1.0 (True), or None
    if rule['intracountry'] is not None:
        # Convert rule float 0.0/1.0 to bool
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['intracountry']:
            return False

    # 8. Monthly Volume (Range match)
    if rule['monthly_volume']:
        if not check_range_match(rule['monthly_volume'], tx_ctx['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule['monthly_fraud_level']:
        if not check_range_match(rule['monthly_fraud_level'], tx_ctx['monthly_fraud_level'], is_percentage=True):
            return False

    return True

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# File paths
payments_path = '/output/chunk2/data/context/payments.csv'
fees_path = '/output/chunk2/data/context/fees.json'
merchant_path = '/output/chunk2/data/context/merchant_data.json'
acquirer_path = '/output/chunk2/data/context/acquirer_countries.csv'

# 1. Load Data
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Get Target Fee Rule (ID=276)
target_fee_id = 276
target_rule = next((f for f in fees_data if f['ID'] == target_fee_id), None)

if not target_rule:
    print(f"Error: Fee ID {target_fee_id} not found.")
    exit()

original_rate = target_rule['rate']
new_rate = 99
print(f"Target Fee ID: {target_fee_id}")
print(f"Original Rate: {original_rate}")
print(f"New Rate: {new_rate}")

# 3. Filter Transactions for Rafa_AI in 2023
merchant_name = 'Rafa_AI'
df_rafa = df_payments[
    (df_payments['merchant'] == merchant_name) & 
    (df_payments['year'] == 2023)
].copy()

print(f"Transactions for {merchant_name} in 2023: {len(df_rafa)}")

# 4. Get Merchant Static Data
merchant_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
if not merchant_info:
    print(f"Error: Merchant {merchant_name} not found in merchant_data.json")
    exit()

# 5. Calculate Monthly Stats (Volume & Fraud)
# Convert day_of_year to month
# 2023 is not a leap year.
# Create a date column to extract month easily
df_rafa['date'] = pd.to_datetime(df_rafa['year'] * 1000 + df_rafa['day_of_year'], format='%Y%j')
df_rafa['month'] = df_rafa['date'].dt.month

# Group by month to calculate stats
monthly_stats = {}
for month, group in df_rafa.groupby('month'):
    total_vol = group['eur_amount'].sum()
    
    # Fraud volume: sum of eur_amount where has_fraudulent_dispute is True
    fraud_vol = group[group['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    
    # Fraud level = fraud_vol / total_vol
    fraud_level = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    monthly_stats[month] = {
        'volume': total_vol,
        'fraud_level': fraud_level
    }

# 6. Iterate Transactions and Match Fee Rule
matching_amount_sum = 0.0
match_count = 0

for idx, row in df_rafa.iterrows():
    # Build Transaction Context
    month = row['month']
    
    # Determine Intracountry
    # True if issuing_country == acquirer_country
    is_intracountry = (row['issuing_country'] == row['acquirer_country'])
    
    tx_ctx = {
        'card_scheme': row['card_scheme'],
        'account_type': merchant_info['account_type'],
        'capture_delay': merchant_info['capture_delay'],
        'merchant_category_code': merchant_info['merchant_category_code'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': is_intracountry,
        'monthly_volume': monthly_stats[month]['volume'],
        'monthly_fraud_level': monthly_stats[month]['fraud_level']
    }
    
    # Check Match
    if match_fee_rule(tx_ctx, target_rule):
        matching_amount_sum += row['eur_amount']
        match_count += 1

print(f"Matching Transactions: {match_count}")
print(f"Total Matching Amount: {matching_amount_sum:.2f}")

# 7. Calculate Delta
# Fee = fixed + (rate * amount / 10000)
# Delta = New_Fee - Old_Fee
# Delta = (fixed + new_rate * amt / 10000) - (fixed + old_rate * amt / 10000)
# Delta = (new_rate - old_rate) * amt / 10000
# Sum Delta = (new_rate - old_rate) * Sum(amt) / 10000

delta = (new_rate - original_rate) * matching_amount_sum / 10000

# 8. Output Result
# High precision required for delta questions
print(f"{delta:.14f}")

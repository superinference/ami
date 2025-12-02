# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2539
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: sufficient
# Plan steps: 2
# Code length: 9098 characters (FULL CODE)
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
        # Handle 'k' and 'm' for volumes
        if v.lower().endswith('k'):
            return float(v[:-1]) * 1000
        if v.lower().endswith('m'):
            return float(v[:-1]) * 1000000
        # Range handling (e.g., "50-60") - return mean
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                p1 = parts[0].strip()
                p2 = parts[1].strip()
                val1 = float(p1.replace('%', '')) / 100 if '%' in p1 else float(p1)
                val2 = float(p2.replace('%', '')) / 100 if '%' in p2 else float(p2)
                return (val1 + val2) / 2
            except:
                pass
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value)

def parse_range_check(value, rule_string):
    """
    Checks if a numeric value fits within a rule string (e.g., '>5', '100k-1m', '7.7%-8.3%').
    Returns True if match, False otherwise.
    """
    if rule_string is None:
        return True
    
    # Handle simple equality
    if isinstance(rule_string, (int, float)):
        return value == rule_string

    s = str(rule_string).strip().lower()
    
    # Handle operators
    if s.startswith('>='):
        limit = coerce_to_float(s[2:])
        return value >= limit
    if s.startswith('<='):
        limit = coerce_to_float(s[2:])
        return value <= limit
    if s.startswith('>'):
        limit = coerce_to_float(s[1:])
        return value > limit
    if s.startswith('<'):
        limit = coerce_to_float(s[1:])
        return value < limit
        
    # Handle ranges (e.g., "100k-1m")
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            lower = coerce_to_float(parts[0])
            upper = coerce_to_float(parts[1])
            return lower <= value <= upper
            
    # Handle exact match string (fallback)
    return s == str(value).lower()

def check_capture_delay(merchant_val, rule_val):
    """
    Matches merchant capture delay (e.g., '1', 'manual') against rule (e.g., '<3', 'manual').
    """
    if rule_val is None:
        return True
    
    # Exact string match
    if str(merchant_val).lower() == str(rule_val).lower():
        return True
        
    # If merchant value is numeric (e.g. '1'), check against range rules
    try:
        m_float = float(merchant_val)
        return parse_range_check(m_float, rule_val)
    except ValueError:
        # Merchant value is non-numeric (e.g. 'manual') and didn't match exact string
        return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_context must contain:
      - card_scheme (str)
      - is_credit (bool)
      - aci (str)
      - intracountry (bool)
      - monthly_volume (float)
      - monthly_fraud_rate (float)
    
    Note: Static fields (mcc, account_type, capture_delay) are assumed pre-filtered.
    """
    # 1. Card Scheme (Exact match required)
    if rule.get('card_scheme') != tx_context['card_scheme']:
        return False

    # 2. Is Credit (Boolean match or Wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 3. ACI (List match or Wildcard)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 4. Intracountry (Boolean match or Wildcard)
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False

    # 5. Monthly Volume (Range check or Wildcard)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 6. Monthly Fraud Level (Range check or Wildcard)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_context['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000.0)

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk3/data/context/payments.csv'
fees_path = '/output/chunk3/data/context/fees.json'
merchant_path = '/output/chunk3/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Merchant and Year
target_merchant = 'Golfclub_Baron_Friso'
target_year = 2023

# Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

original_mcc = merchant_info['merchant_category_code']
target_mcc = 5411
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

# Filter Transactions
df = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
].copy()

# 3. Pre-calculate Monthly Stats (Volume and Fraud Rate)
# Convert day_of_year to month
df['month'] = pd.to_datetime(df['year'] * 1000 + df['day_of_year'], format='%Y%j').dt.month

# Calculate stats per month
monthly_stats = {}
for month in df['month'].unique():
    month_txs = df[df['month'] == month]
    
    # Volume: Sum of eur_amount
    vol = month_txs['eur_amount'].sum()
    
    # Fraud Rate: Count of fraud / Total count (ratio 0.0-1.0)
    fraud_count = month_txs['has_fraudulent_dispute'].sum()
    total_count = len(month_txs)
    fraud_rate = fraud_count / total_count if total_count > 0 else 0.0
    
    monthly_stats[month] = {
        'volume': vol,
        'fraud_rate': fraud_rate
    }

# 4. Pre-filter Rules for Performance
# We filter rules that match the STATIC properties of the merchant (Account Type, Capture Delay, MCC)
# This avoids checking these for every single transaction.

def filter_rules_static(rules, mcc, acc_type, cap_delay):
    filtered = []
    for rule in rules:
        # MCC Match
        if rule.get('merchant_category_code'):
            if mcc not in rule['merchant_category_code']:
                continue
        
        # Account Type Match
        if rule.get('account_type'):
            if acc_type not in rule['account_type']:
                continue
                
        # Capture Delay Match
        if rule.get('capture_delay'):
            if not check_capture_delay(cap_delay, rule['capture_delay']):
                continue
        
        filtered.append(rule)
    return filtered

rules_original = filter_rules_static(fees_data, original_mcc, account_type, capture_delay)
rules_new = filter_rules_static(fees_data, target_mcc, account_type, capture_delay)

# 5. Calculate Fees and Delta
total_delta = 0.0

for idx, row in df.iterrows():
    # Build Dynamic Context
    is_intracountry = (row['issuing_country'] == row['acquirer_country'])
    month = row['month']
    current_vol = monthly_stats[month]['volume']
    current_fraud = monthly_stats[month]['fraud_rate']
    
    tx_context = {
        'card_scheme': row['card_scheme'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': is_intracountry,
        'monthly_volume': current_vol,
        'monthly_fraud_rate': current_fraud
    }
    
    # --- Scenario A: Original MCC ---
    fee_original = 0.0
    for rule in rules_original:
        if match_fee_rule(tx_context, rule):
            fee_original = calculate_fee(row['eur_amount'], rule)
            break # Take first matching rule
            
    # --- Scenario B: New MCC (5411) ---
    fee_new = 0.0
    for rule in rules_new:
        if match_fee_rule(tx_context, rule):
            fee_new = calculate_fee(row['eur_amount'], rule)
            break # Take first matching rule
    
    # Calculate Delta (New - Old)
    delta = fee_new - fee_original
    total_delta += delta

# 6. Output Result
# Question asks for "amount delta".
print(f"{total_delta:.14f}")

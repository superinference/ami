# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2057
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8802 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if pd.isna(value) or value is None:
        return 0.0
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
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value)

def parse_range(range_str):
    """Parses a range string like '100k-1m', '<3', '>5', '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip().replace(',', '').replace('%', '')
    multiplier = 1
    if 'k' in s:
        multiplier = 1000
        s = s.replace('k', '')
    elif 'm' in s:
        multiplier = 1000000
        s = s.replace('m', '')
        
    if '-' in s:
        try:
            parts = s.split('-')
            return float(parts[0]) * multiplier, float(parts[1]) * multiplier
        except:
            return None, None
    elif '>' in s:
        try:
            return float(s.replace('>', '')) * multiplier, float('inf')
        except:
            return None, None
    elif '<' in s:
        try:
            return float('-inf'), float(s.replace('<', '')) * multiplier
        except:
            return None, None
    elif s == 'immediate':
        return 0, 0
    elif s == 'manual':
        return 999, 999 # Represent manual as a high number or specific code
    
    return None, None

def match_fee_rule(tx_dict, rule):
    """
    Determines if a transaction matches a fee rule.
    tx_dict must contain: 
        card_scheme, account_type, merchant_category_code, is_credit, aci, 
        intracountry, capture_delay, monthly_volume, monthly_fraud_level
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_dict.get('card_scheme'):
        return False
        
    # 2. Account Type (List in rule, single value in tx)
    if rule.get('account_type'):
        if tx_dict.get('account_type') not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List in rule, single value in tx)
    if rule.get('merchant_category_code'):
        if tx_dict.get('merchant_category_code') not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Bool)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_dict.get('is_credit'):
            return False
            
    # 5. ACI (List in rule, single value in tx)
    if rule.get('aci'):
        if tx_dict.get('aci') not in rule['aci']:
            return False
            
    # 6. Intracountry (Bool)
    if rule.get('intracountry') is not None:
        # Convert 1.0/0.0 to bool if necessary
        rule_intra = bool(rule['intracountry'])
        tx_intra = bool(tx_dict.get('intracountry'))
        if rule_intra != tx_intra:
            return False
            
    # 7. Capture Delay (Range/Value)
    if rule.get('capture_delay'):
        # Handle specific strings
        if rule['capture_delay'] == 'manual':
            if tx_dict.get('capture_delay') != 'manual':
                return False
        elif rule['capture_delay'] == 'immediate':
            if tx_dict.get('capture_delay') != 'immediate':
                return False
        else:
            # Numeric range
            min_val, max_val = parse_range(rule['capture_delay'])
            tx_val = tx_dict.get('capture_delay')
            # If tx_val is string 'manual'/'immediate' but rule is numeric, no match
            if isinstance(tx_val, str) and not tx_val.replace('.','',1).isdigit():
                return False
            try:
                val = float(tx_val)
                if min_val is not None and not (min_val <= val <= max_val):
                    return False
            except:
                return False

    # 8. Monthly Volume (Range)
    if rule.get('monthly_volume'):
        min_vol, max_vol = parse_range(rule['monthly_volume'])
        tx_vol = tx_dict.get('monthly_volume', 0)
        if min_vol is not None and not (min_vol <= tx_vol <= max_vol):
            return False

    # 9. Monthly Fraud Level (Range)
    if rule.get('monthly_fraud_level'):
        min_fraud, max_fraud = parse_range(rule['monthly_fraud_level'])
        # tx_fraud is expected to be a ratio (e.g., 0.08 for 8%) if parse_range handles %
        # parse_range divides by 100? No, parse_range strips %. 
        # So '8.3%' -> 8.3. 
        # We need to ensure tx_fraud is in the same unit.
        # Let's standardize: tx_fraud should be percentage (0-100).
        tx_fraud = tx_dict.get('monthly_fraud_level', 0) * 100
        if min_fraud is not None and not (min_fraud <= tx_fraud <= max_fraud):
            return False

    return True

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_path = '/output/chunk6/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Setup Target
target_merchant = 'Crossfit_Hanna'
target_fee_id = 150
new_rate = 1

# 3. Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

# 4. Get Fee Rule
fee_rule = next((f for f in fees_data if f['ID'] == target_fee_id), None)
if not fee_rule:
    raise ValueError(f"Fee ID {target_fee_id} not found in fees.json")

original_rate = fee_rule['rate']

# 5. Calculate Monthly Stats for July 2023 (Day 182-212)
# Manual: "Monthly volumes and rates are computed always in natural months"
# July is roughly day 182 to 212 (non-leap year).
# Let's filter strictly for July.
july_mask = (df_payments['merchant'] == target_merchant) & \
            (df_payments['day_of_year'] >= 182) & \
            (df_payments['day_of_year'] <= 212) & \
            (df_payments['year'] == 2023)

df_july = df_payments[july_mask].copy()

# Calculate Volume (EUR)
monthly_volume = df_july['eur_amount'].sum()

# Calculate Fraud Level (Volume Ratio)
# Manual: "Fraud is defined as the ratio of fraudulent volume over total volume."
fraud_volume = df_july[df_july['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_ratio = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 6. Identify Affected Transactions
affected_volume = 0.0
matching_count = 0

for _, tx in df_july.iterrows():
    # Construct transaction context for matching
    tx_ctx = {
        'card_scheme': tx['card_scheme'],
        'account_type': merchant_info['account_type'],
        'merchant_category_code': merchant_info['merchant_category_code'],
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': tx['issuing_country'] == tx['acquirer_country'],
        'capture_delay': merchant_info['capture_delay'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_ratio # Passed as ratio 0.0-1.0
    }
    
    if match_fee_rule(tx_ctx, fee_rule):
        affected_volume += tx['eur_amount']
        matching_count += 1

# 7. Calculate Delta
# Formula: fee = fixed + rate * amount / 10000
# Delta = (New Fee) - (Old Fee)
#       = (fixed + new_rate * amt / 10000) - (fixed + old_rate * amt / 10000)
#       = (new_rate - old_rate) * amt / 10000
# Total Delta = (new_rate - old_rate) * Total_Affected_Volume / 10000

rate_diff = new_rate - original_rate
delta = (rate_diff * affected_volume) / 10000

# 8. Output
print(f"Merchant: {target_merchant}")
print(f"Month: July 2023")
print(f"Fee ID: {target_fee_id}")
print(f"Original Rate: {original_rate}")
print(f"New Rate: {new_rate}")
print(f"Matching Transactions: {matching_count}")
print(f"Affected Volume: {affected_volume:.2f}")
print(f"Delta: {delta:.14f}")

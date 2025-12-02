# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2439
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 7241 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import re

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
    return 0.0

def parse_range(range_str):
    """Parses a string range like '100k-1m' or '0%-1%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    # Handle k/m suffixes
    def parse_val(v):
        v = v.strip().lower()
        mult = 1
        if 'k' in v:
            mult = 1000
            v = v.replace('k', '')
        elif 'm' in v:
            mult = 1000000
            v = v.replace('m', '')
        elif '%' in v:
            v = v.replace('%', '')
            mult = 0.01
        return float(v) * mult

    if '-' in range_str:
        parts = range_str.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif range_str.startswith('>'):
        return parse_val(range_str[1:]), float('inf')
    elif range_str.startswith('<'):
        return float('-inf'), parse_val(range_str[1:])
    return None, None

def match_fee_rule(tx_context, rule):
    """
    Checks if a transaction context matches a fee rule.
    tx_context must contain: 
      card_scheme, account_type, merchant_category_code, is_credit, aci, 
      intracountry, capture_delay, monthly_volume, monthly_fraud_level
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context.get('card_scheme'):
        return False

    # 2. Account Type (List match: merchant's type must be in rule's list. Empty list = Any)
    if rule.get('account_type'):
        if tx_context.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_context.get('merchant_category_code') not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match. None = Any)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context.get('is_credit'):
            return False

    # 5. ACI (List match)
    if rule.get('aci'):
        if tx_context.get('aci') not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match. None = Any)
    if rule.get('intracountry') is not None:
        # Ensure strict boolean comparison
        is_intra = bool(tx_context.get('intracountry'))
        rule_intra = bool(rule['intracountry'])
        if is_intra != rule_intra:
            return False

    # 7. Capture Delay (Exact match. None = Any)
    if rule.get('capture_delay'):
        if rule['capture_delay'] != str(tx_context.get('capture_delay')):
            return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        vol = tx_context.get('monthly_volume', 0)
        if not (min_v <= vol <= max_v):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        fraud = tx_context.get('monthly_fraud_level', 0)
        if not (min_f <= fraud <= max_f):
            return False

    return True

# ==========================================
# MAIN SCRIPT
# ==========================================

# 1. Load Data
payments_path = '/output/chunk4/data/context/payments.csv'
fees_path = '/output/chunk4/data/context/fees.json'
merchant_path = '/output/chunk4/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Rafa_AI in September 2023
# September 2023 = Day 244 to 273
merchant_name = 'Rafa_AI'
sept_start = 244
sept_end = 273

# Filter transactions
rafa_txs = df_payments[
    (df_payments['merchant'] == merchant_name) & 
    (df_payments['day_of_year'] >= sept_start) & 
    (df_payments['day_of_year'] <= sept_end) &
    (df_payments['year'] == 2023)
].copy()

# 3. Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
if not merchant_info:
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

# 4. Calculate Monthly Stats for Rafa_AI (Required for fee matching)
# Volume in Euros
monthly_volume = rafa_txs['eur_amount'].sum()

# Fraud Level (Ratio of Fraud Volume / Total Volume)
# Manual: "ratio between monthly total volume and monthly volume notified as fraud"
# Interpreted as Fraud Volume / Total Volume based on standard industry practice and percentage formats in fees.json
fraud_txs = rafa_txs[rafa_txs['has_fraudulent_dispute'] == True]
monthly_fraud_volume = fraud_txs['eur_amount'].sum()
monthly_fraud_level = monthly_fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 5. Get Fee Rule ID 276
target_fee_id = 276
fee_rule_276 = next((f for f in fees_data if f['ID'] == target_fee_id), None)

if not fee_rule_276:
    print(f"Fee ID {target_fee_id} not found.")
else:
    # 6. Identify Transactions that match Fee 276
    matching_amount_sum = 0.0
    matching_count = 0

    for _, row in rafa_txs.iterrows():
        # Construct context for matching
        # Intracountry: Issuer == Acquirer
        is_intracountry = (row['issuing_country'] == row['acquirer_country'])
        
        context = {
            'card_scheme': row['card_scheme'],
            'account_type': merchant_info.get('account_type'),
            'merchant_category_code': merchant_info.get('merchant_category_code'),
            'is_credit': row['is_credit'],
            'aci': row['aci'],
            'intracountry': is_intracountry,
            'capture_delay': merchant_info.get('capture_delay'),
            'monthly_volume': monthly_volume,
            'monthly_fraud_level': monthly_fraud_level
        }

        if match_fee_rule(context, fee_rule_276):
            matching_amount_sum += row['eur_amount']
            matching_count += 1

    # 7. Calculate Delta
    # Formula: Fee = Fixed + (Rate * Amount / 10000)
    # Delta = New_Fee - Old_Fee
    # Fixed amount cancels out.
    # Delta = (New_Rate * Amount / 10000) - (Old_Rate * Amount / 10000)
    # Delta = (New_Rate - Old_Rate) * Total_Amount / 10000
    
    old_rate = fee_rule_276['rate']
    new_rate = 1  # As per question "relative fee ... changed to 1"
    
    delta = (new_rate - old_rate) * matching_amount_sum / 10000

    # Output results with high precision
    print(f"{delta:.14f}")

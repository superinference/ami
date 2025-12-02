# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1709
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 7213 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', ''))  # Return as percentage value (e.g. 8.3 for 8.3%)
        try:
            return float(v)
        except:
            return 0.0
    return 0.0

def parse_range_check(rule_value, actual_value):
    """Checks if actual_value fits in rule_value range string."""
    if rule_value is None:
        return True
    
    # Handle k/m suffixes for volume and % for fraud
    def parse_num(s):
        s = str(s).lower().strip().replace('%', '')
        mult = 1
        if 'k' in s:
            mult = 1000
            s = s.replace('k', '')
        elif 'm' in s:
            mult = 1000000
            s = s.replace('m', '')
        try:
            return float(s) * mult
        except ValueError:
            return 0.0

    try:
        if isinstance(rule_value, str):
            if '-' in rule_value:
                parts = rule_value.split('-')
                if len(parts) == 2:
                    low, high = parts
                    return parse_num(low) <= actual_value <= parse_num(high)
            elif rule_value.startswith('>'):
                return actual_value > parse_num(rule_value[1:])
            elif rule_value.startswith('<'):
                return actual_value < parse_num(rule_value[1:])
            elif rule_value.startswith('='):
                return actual_value == parse_num(rule_value[1:])
        
        # Fallback for exact matches (strings or numbers)
        return str(actual_value).lower() == str(rule_value).lower()
    except:
        return False

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a transaction context.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List in rule, single in tx)
    # If rule list is empty/null, it applies to all.
    if rule.get('account_type') and tx_ctx['account_type'] not in rule['account_type']:
        return False
        
    # 3. Merchant Category Code (List in rule, single in tx)
    if rule.get('merchant_category_code') and tx_ctx['merchant_category_code'] not in rule['merchant_category_code']:
        return False
        
    # 4. Capture Delay (String/Range in rule, String in tx)
    if rule.get('capture_delay'):
        # If exact string match (e.g. "manual" == "manual")
        if str(rule['capture_delay']) == str(tx_ctx['capture_delay']):
            pass
        # If rule is a range/numeric (e.g. ">5") and tx is numeric string
        elif any(c.isdigit() for c in str(rule['capture_delay'])) and str(tx_ctx['capture_delay']).replace('.','',1).isdigit():
             if not parse_range_check(rule['capture_delay'], float(tx_ctx['capture_delay'])):
                 return False
        # If mismatch in types (e.g. rule ">5" but tx "manual") -> No match
        else:
            return False

    # 5. Monthly Volume (Range in rule, float in tx)
    if rule.get('monthly_volume'):
        if not parse_range_check(rule['monthly_volume'], tx_ctx['monthly_volume']):
            return False
            
    # 6. Monthly Fraud Level (Range in rule, float % in tx)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(rule['monthly_fraud_level'], tx_ctx['monthly_fraud_level']):
            return False

    # 7. Is Credit (Bool)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 8. ACI (List in rule, single in tx)
    if rule.get('aci') and tx_ctx['aci'] not in rule['aci']:
        return False
        
    # 9. Intracountry (Bool/Float)
    if rule.get('intracountry') is not None:
        # Convert 0.0/1.0 to bool
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['intracountry']:
            return False
            
    return True

# --- Main Execution ---

# File paths
payments_path = '/output/chunk3/data/context/payments.csv'
fees_path = '/output/chunk3/data/context/fees.json'
merchant_path = '/output/chunk3/data/context/merchant_data.json'

# Load data
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# Target Parameters
target_merchant = 'Rafa_AI'
target_day = 300
target_year = 2023

# 1. Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found.")
    exit()

mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

# 2. Calculate Monthly Stats for October (Days 274-304)
# Day 300 is in October. We need stats for the full month of October.
oct_mask = (df_payments['merchant'] == target_merchant) & \
           (df_payments['year'] == target_year) & \
           (df_payments['day_of_year'] >= 274) & \
           (df_payments['day_of_year'] <= 304)

df_oct = df_payments[oct_mask]

monthly_volume = df_oct['eur_amount'].sum()
fraud_txs = df_oct['has_fraudulent_dispute'].sum()
total_txs = len(df_oct)
# Fraud rate as percentage (e.g., 8.5 for 8.5%)
monthly_fraud_rate = (fraud_txs / total_txs * 100) if total_txs > 0 else 0.0

# 3. Filter Transactions for Day 300
day_mask = (df_payments['merchant'] == target_merchant) & \
           (df_payments['year'] == target_year) & \
           (df_payments['day_of_year'] == target_day)

df_day = df_payments[day_mask]

# 4. Find Applicable Fee IDs
applicable_ids = set()

# Sort fees by ID to ensure deterministic order if needed, 
# though typically fee engines process top-down. 
# Assuming fees_data is already ordered by priority or ID.
# We will iterate through fees for each transaction and pick the FIRST match.

for _, tx in df_day.iterrows():
    # Determine intracountry status
    is_intra = (tx['issuing_country'] == tx['acquirer_country'])
    
    # Build context for this transaction
    tx_ctx = {
        'card_scheme': tx['card_scheme'],
        'account_type': account_type,
        'merchant_category_code': mcc,
        'capture_delay': capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_rate,
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': is_intra
    }
    
    # Find the first matching rule for this transaction
    for rule in fees_data:
        if match_fee_rule(tx_ctx, rule):
            applicable_ids.add(rule['ID'])
            break # Stop after first match per transaction

# 5. Output Result
sorted_ids = sorted(list(applicable_ids))
print(", ".join(map(str, sorted_ids)))

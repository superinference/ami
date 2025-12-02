# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1826
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 6338 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json

# --- Helper Functions ---

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
        return float(v)
    return float(value)

def parse_range(range_str, scale=1.0):
    """Parses strings like '100k-1m', '0%-0.5%', '>5' into (min, max)."""
    if range_str is None:
        return -float('inf'), float('inf')
    if not isinstance(range_str, str):
        # If it's already a number, treat as exact match range
        return float(range_str), float(range_str)
    
    s = range_str.lower().strip()
    
    # Handle percentages
    if '%' in s:
        s = s.replace('%', '')
        scale = 0.01
        
    # Handle k/m suffixes
    def parse_val(v):
        v = v.strip()
        mult = 1
        if 'k' in v:
            mult = 1000
            v = v.replace('k', '')
        elif 'm' in v:
            mult = 1000000
            v = v.replace('m', '')
        try:
            return float(v) * mult * scale
        except ValueError:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif s.startswith('>'):
        return parse_val(s[1:]), float('inf')
    elif s.startswith('<'):
        return -float('inf'), parse_val(s[1:])
    else:
        val = parse_val(s)
        return val, val

def match_fee_rule(tx, rule):
    """
    Checks if a transaction matches a fee rule.
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx.get('card_scheme'):
        return False
        
    # 2. Account Type (List)
    if rule.get('account_type') and len(rule['account_type']) > 0:
        if tx.get('account_type') not in rule['account_type']:
            return False
            
    # 3. Capture Delay
    if rule.get('capture_delay'):
        rule_delay = str(rule['capture_delay'])
        tx_delay = str(tx.get('capture_delay'))
        
        # Map keywords to numbers for range comparison
        delay_map = {'immediate': 0, 'manual': 999}
        
        # If rule is a range or comparison
        if any(c in rule_delay for c in ['<', '>', '-']):
            val = delay_map.get(tx_delay)
            if val is None:
                try:
                    val = float(tx_delay)
                except:
                    val = 999 # Default to high if unknown string
            
            min_v, max_v = parse_range(rule_delay)
            if not (min_v <= val <= max_v):
                return False
        else:
            # Exact match
            if rule_delay != tx_delay:
                return False

    # 4. Merchant Category Code (List)
    if rule.get('merchant_category_code') and len(rule['merchant_category_code']) > 0:
        if tx.get('merchant_category_code') not in rule['merchant_category_code']:
            return False

    # 5. Is Credit
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx.get('is_credit'):
            return False

    # 6. ACI (List)
    if rule.get('aci') and len(rule['aci']) > 0:
        if tx.get('aci') not in rule['aci']:
            return False

    # 7. Intracountry
    if rule.get('intracountry') is not None:
        is_intra = (tx.get('issuing_country') == tx.get('acquirer_country'))
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    # 8. Monthly Volume
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= tx.get('monthly_volume', 0) <= max_v):
            return False

    # 9. Monthly Fraud Level
    if rule.get('monthly_fraud_level'):
        min_v, max_v = parse_range(rule['monthly_fraud_level'])
        if not (min_v <= tx.get('monthly_fraud_level', 0) <= max_v):
            return False

    return True

# --- Main Execution ---

# Load Data
payments_path = '/output/chunk4/data/context/payments.csv'
merchant_path = '/output/chunk4/data/context/merchant_data.json'
fees_path = '/output/chunk4/data/context/fees.json'

df = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# Filter for Crossfit_Hanna and April 2023 (Day 91-120)
target_merchant = 'Crossfit_Hanna'
df_april = df[
    (df['merchant'] == target_merchant) & 
    (df['year'] == 2023) & 
    (df['day_of_year'] >= 91) & 
    (df['day_of_year'] <= 120)
].copy()

# Get Merchant Info
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    print("Merchant not found")
    exit()

# Calculate Monthly Stats
monthly_volume = df_april['eur_amount'].sum()
fraud_volume = df_april[df_april['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_level = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# Calculate Fees
total_fees = 0.0

for _, row in df_april.iterrows():
    tx_context = {
        'card_scheme': row['card_scheme'],
        'account_type': merchant_info['account_type'],
        'capture_delay': merchant_info['capture_delay'],
        'merchant_category_code': merchant_info['merchant_category_code'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'issuing_country': row['issuing_country'],
        'acquirer_country': row['acquirer_country'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level
    }
    
    # Find first matching rule
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * row['eur_amount'] / 10000)
        total_fees += fee

print(f"{total_fees:.2f}")

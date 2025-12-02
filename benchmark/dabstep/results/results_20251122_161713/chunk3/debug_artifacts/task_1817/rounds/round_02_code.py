# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1817
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8456 characters (FULL CODE)
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

def parse_range(range_str):
    """Parses a range string like '100k-1m' or '0%-1%' into a tuple (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    # Handle k/m suffixes
    def parse_val(v):
        v = v.strip()
        mult = 1
        if v.lower().endswith('k'):
            mult = 1000
            v = v[:-1]
        elif v.lower().endswith('m'):
            mult = 1000000
            v = v[:-1]
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

def check_capture_delay(merchant_delay, rule_delay):
    """Checks if merchant capture delay matches the rule."""
    if rule_delay is None:
        return True
    
    # Convert merchant delay to numeric if possible (e.g. "1" -> 1)
    try:
        merch_val = float(merchant_delay)
    except (ValueError, TypeError):
        merch_val = merchant_delay # Keep as string (e.g. "immediate", "manual")

    if rule_delay == 'immediate':
        return merch_val == 'immediate' or merch_val == 0
    elif rule_delay == 'manual':
        return merch_val == 'manual'
    elif rule_delay.startswith('<'):
        limit = float(rule_delay[1:])
        return isinstance(merch_val, (int, float)) and merch_val < limit
    elif rule_delay.startswith('>'):
        limit = float(rule_delay[1:])
        return isinstance(merch_val, (int, float)) and merch_val > limit
    elif '-' in rule_delay:
        low, high = map(float, rule_delay.split('-'))
        return isinstance(merch_val, (int, float)) and low <= merch_val <= high
    
    return str(merch_val) == str(rule_delay)

def match_fee_rule(tx_context, rule):
    """
    Matches a transaction context against a fee rule.
    tx_context must contain: 
      card_scheme, account_type, mcc, is_credit, aci, intracountry, 
      capture_delay, monthly_volume, monthly_fraud_rate
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List)
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List)
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Bool)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False
            
    # 5. ACI (List)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Bool)
    if rule.get('intracountry') is not None:
        # Convert rule value to bool (0.0 -> False, 1.0 -> True)
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False

    # 7. Capture Delay
    if rule.get('capture_delay'):
        if not check_capture_delay(tx_context['capture_delay'], rule['capture_delay']):
            return False

    # 8. Monthly Volume
    if rule.get('monthly_volume'):
        min_vol, max_vol = parse_range(rule['monthly_volume'])
        if min_vol is not None:
            if not (min_vol <= tx_context['monthly_volume'] <= max_vol):
                return False

    # 9. Monthly Fraud Level
    if rule.get('monthly_fraud_level'):
        min_fraud, max_fraud = parse_range(rule['monthly_fraud_level'])
        if min_fraud is not None:
            # tx_context['monthly_fraud_rate'] is a ratio (e.g. 0.08 for 8%)
            if not (min_fraud <= tx_context['monthly_fraud_rate'] <= max_fraud):
                return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0)
    rate = rule.get('rate', 0)
    return fixed + (rate * amount / 10000)

# ==========================================
# MAIN SCRIPT
# ==========================================

# 1. Load Data
print("Loading data...")
payments_path = '/output/chunk3/data/context/payments.csv'
fees_path = '/output/chunk3/data/context/fees.json'
merchant_path = '/output/chunk3/data/context/merchant_data.json'

df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_path, 'r') as f:
    merchants = json.load(f)

# 2. Filter for Merchant and Timeframe (July 2023)
merchant_name = 'Belles_cookbook_store'
start_day = 182
end_day = 212
year = 2023

# Filter transactions
df_merchant = df[df['merchant'] == merchant_name].copy()
df_july = df_merchant[
    (df_merchant['day_of_year'] >= start_day) & 
    (df_merchant['day_of_year'] <= end_day) &
    (df_merchant['year'] == year)
].copy()

print(f"Transactions for {merchant_name} in July: {len(df_july)}")

# 3. Get Merchant Metadata
merchant_info = next((m for m in merchants if m['merchant'] == merchant_name), None)
if not merchant_info:
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

account_type = merchant_info['account_type']
mcc = merchant_info['merchant_category_code']
capture_delay = merchant_info['capture_delay']

# 4. Calculate Monthly Stats (Volume and Fraud) for July
# Manual says: "Monthly volumes and rates are computed always in natural months"
# Fraud level: "ratio between monthly total volume and monthly volume notified as fraud"

monthly_volume = df_july['eur_amount'].sum()
fraud_volume = df_july[df_july['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

print(f"Monthly Volume: €{monthly_volume:,.2f}")
print(f"Monthly Fraud Volume: €{fraud_volume:,.2f}")
print(f"Monthly Fraud Rate: {monthly_fraud_rate:.4%}")

# 5. Calculate Fees
total_fees = 0.0
matched_count = 0
unmatched_count = 0

# Pre-process fees to ensure numeric types where needed
for rule in fees:
    if rule.get('fixed_amount') is None: rule['fixed_amount'] = 0.0
    if rule.get('rate') is None: rule['rate'] = 0

# Iterate through transactions
for _, tx in df_july.iterrows():
    # Build transaction context
    tx_context = {
        'card_scheme': tx['card_scheme'],
        'account_type': account_type,
        'mcc': mcc,
        'is_credit': bool(tx['is_credit']),
        'aci': tx['aci'],
        'intracountry': tx['issuing_country'] == tx['acquirer_country'],
        'capture_delay': capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate
    }
    
    # Find matching rule
    matched_rule = None
    for rule in fees:
        if match_fee_rule(tx_context, rule):
            matched_rule = rule
            break # Stop at first match
            
    if matched_rule:
        fee = calculate_fee(tx['eur_amount'], matched_rule)
        total_fees += fee
        matched_count += 1
    else:
        unmatched_count += 1
        # Optional: Print unmatched details for debugging
        # print(f"Unmatched: {tx_context}")

print(f"\nMatched Transactions: {matched_count}")
print(f"Unmatched Transactions: {unmatched_count}")
print(f"Total Fees: €{total_fees:.2f}")

# Final Answer Output
print(f"{total_fees:.2f}")

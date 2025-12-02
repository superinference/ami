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
    """Parses a range string like '100k-1m', '>5', '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip().replace(',', '').replace('€', '').replace('$', '')
    
    # Handle multipliers
    multiplier = 1
    if 'k' in s:
        multiplier = 1000
        s = s.replace('k', '')
    elif 'm' in s:
        multiplier = 1000000
        s = s.replace('m', '')
        
    # Handle percentages (strip % but keep value as is, e.g., 5% -> 5.0)
    # The matching logic will scale the comparison value to match this
    if '%' in s:
        s = s.replace('%', '')

    try:
        if '>' in s:
            val = float(s.replace('>', '')) * multiplier
            return (val, float('inf'))
        elif '<' in s:
            val = float(s.replace('<', '')) * multiplier
            return (float('-inf'), val)
        elif '-' in s:
            parts = s.split('-')
            min_val = float(parts[0]) * multiplier
            max_val = float(parts[1]) * multiplier
            return (min_val, max_val)
        else:
            val = float(s) * multiplier
            return (val, val)
    except:
        return None, None

def match_fee_rule(tx_ctx, rule):
    """
    Checks if a transaction context matches a fee rule.
    tx_ctx: dict containing transaction and merchant details
    rule: dict containing fee rule definition
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx.get('card_scheme'):
        return False

    # 2. Account Type (List in rule, single value in tx)
    # Wildcard: Empty list or None in rule means ALL match
    if rule.get('account_type'):
        if tx_ctx.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List in rule, single value in tx)
    if rule.get('merchant_category_code'):
        if tx_ctx.get('merchant_category_code') not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Bool)
    # Wildcard: None in rule means ALL match
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx.get('is_credit'):
            return False

    # 5. ACI (List in rule, single value in tx)
    if rule.get('aci'):
        if tx_ctx.get('aci') not in rule['aci']:
            return False

    # 6. Intracountry (Bool)
    if rule.get('intracountry') is not None:
        # Convert rule value to bool if it's 0.0/1.0
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx.get('intracountry'):
            return False
            
    # 7. Capture Delay (String in rule, String in tx)
    if rule.get('capture_delay'):
        # Handle range strings like '>5', '3-5' or exact matches 'manual'
        # If it's a numeric range
        if any(c in rule['capture_delay'] for c in ['>', '<', '-']) and rule['capture_delay'] not in ['manual', 'immediate']:
             # This dataset has 'manual', 'immediate', '1', '2', '7' as values in merchant_data
             # fees.json has '>5', '<3', '3-5'
             # We need to parse the merchant's value to a number if possible
             merch_delay = tx_ctx.get('capture_delay')
             if merch_delay in ['manual', 'immediate']:
                 if rule['capture_delay'] != merch_delay:
                     return False
             else:
                 try:
                     delay_days = float(merch_delay)
                     min_d, max_d = parse_range(rule['capture_delay'])
                     if min_d is not None and (delay_days < min_d or delay_days > max_d):
                         return False
                 except:
                     # If parsing fails, fallback to exact string match
                     if rule['capture_delay'] != merch_delay:
                         return False
        else:
            # Exact match
            if rule['capture_delay'] != tx_ctx.get('capture_delay'):
                return False

    # 8. Monthly Volume (Range string in rule)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        vol = tx_ctx.get('monthly_volume', 0)
        if min_v is not None and (vol < min_v or vol > max_v):
            return False

    # 9. Monthly Fraud Level (Range string in rule)
    if rule.get('monthly_fraud_level'):
        # Rule is likely "0-5%" or similar. parse_range returns 0.0, 5.0
        # tx_ctx has ratio 0.05. We multiply by 100 -> 5.0
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        fraud_pct = tx_ctx.get('monthly_fraud_level', 0) * 100 
        if min_f is not None and (fraud_pct < min_f or fraud_pct > max_f):
            return False

    return True

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_path = '/output/chunk5/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Define Target Parameters
target_merchant = 'Belles_cookbook_store'
target_year = 2023
target_fee_id = 398
new_rate = 1

# 3. Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

# 4. Get Target Fee Rule
fee_rule = next((f for f in fees_data if f['ID'] == target_fee_id), None)
if not fee_rule:
    raise ValueError(f"Fee ID {target_fee_id} not found in fees.json")

old_rate = fee_rule['rate']
print(f"Fee ID: {target_fee_id}")
print(f"Old Rate: {old_rate}")
print(f"New Rate: {new_rate}")

# 5. Filter Transactions (Jan 2023)
# Jan is day_of_year 1 to 31
df_filtered = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] <= 31)
].copy()

print(f"Transactions found for {target_merchant} in Jan {target_year}: {len(df_filtered)}")

# 6. Calculate Monthly Stats for Merchant (Required for matching)
# Volume in Euros
monthly_volume = df_filtered['eur_amount'].sum()
# Fraud Rate (Ratio)
fraud_count = df_filtered['has_fraudulent_dispute'].sum()
total_count = len(df_filtered)
monthly_fraud_ratio = fraud_count / total_count if total_count > 0 else 0.0

print(f"Monthly Volume: {monthly_volume}")
print(f"Monthly Fraud Ratio: {monthly_fraud_ratio}")

# 7. Identify Matching Transactions and Sum Volume
affected_volume = 0.0
matching_tx_count = 0

for _, tx in df_filtered.iterrows():
    # Build Context
    # Intracountry: Issuer == Acquirer
    is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
    
    ctx = {
        'card_scheme': tx['card_scheme'],
        'account_type': merchant_info['account_type'],
        'merchant_category_code': merchant_info['merchant_category_code'],
        'capture_delay': merchant_info['capture_delay'],
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': is_intracountry,
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_ratio # Matcher converts to %
    }
    
    if match_fee_rule(ctx, fee_rule):
        affected_volume += tx['eur_amount']
        matching_tx_count += 1

print(f"Matching Transactions: {matching_tx_count}")
print(f"Affected Volume: {affected_volume}")

# 8. Calculate Delta
# Formula: Delta = (New Rate - Old Rate) * Volume / 10000
# Note: Rate is per 10,000 units of currency (basis points logic in this dataset context)
delta = (new_rate - old_rate) * affected_volume / 10000

# 9. Output Result
print(f"{delta:.14f}")
# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1822
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 10039 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

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
    """Parses a range string like '100k-1m' or '>5' into (min, max)."""
    if range_str is None:
        return (float('-inf'), float('inf'))
    
    s = str(range_str).lower().strip()
    
    # Handle suffixes
    multipliers = {'k': 1000, 'm': 1000000, '%': 0.01}
    
    def parse_val(val_str):
        val_str = val_str.strip()
        mult = 1
        if val_str.endswith('%'):
            mult = 0.01
            val_str = val_str[:-1]
        elif val_str.endswith('k'):
            mult = 1000
            val_str = val_str[:-1]
        elif val_str.endswith('m'):
            mult = 1000000
            val_str = val_str[:-1]
        return float(val_str) * mult

    if '>' in s:
        val = parse_val(s.replace('>', '').replace('=', ''))
        return (val, float('inf'))
    elif '<' in s:
        val = parse_val(s.replace('<', '').replace('=', ''))
        return (float('-inf'), val)
    elif '-' in s:
        parts = s.split('-')
        return (parse_val(parts[0]), parse_val(parts[1]))
    elif s == 'immediate':
        return (0, 0) # Treat as 0 days
    elif s == 'manual':
        return (999, 999) # Treat as very high days
    else:
        try:
            val = parse_val(s)
            return (val, val)
        except:
            return (float('-inf'), float('inf'))

def match_fee_rule(tx_context, rule):
    """
    Checks if a transaction context matches a fee rule.
    tx_context keys: card_scheme, account_type, capture_delay, monthly_fraud_level, 
                     monthly_volume, merchant_category_code, is_credit, aci, intracountry
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context.get('card_scheme'):
        return False

    # 2. Account Type (List match)
    # Rule has list of allowed types. Tx has single type.
    if rule.get('account_type'):
        if tx_context.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_context.get('merchant_category_code') not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match)
    # If rule['is_credit'] is None, it applies to both.
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context.get('is_credit'):
            return False

    # 5. ACI (List match)
    if rule.get('aci'):
        if tx_context.get('aci') not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match)
    # If rule['intracountry'] is None, applies to both.
    # JSON uses 0.0/1.0 for boolean sometimes, or null.
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry']) # 1.0 -> True, 0.0 -> False
        if rule_intra != tx_context.get('intracountry'):
            return False

    # 7. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        vol = tx_context.get('monthly_volume', 0)
        # Range is inclusive? Usually yes.
        # Handle edge cases where max_v is exact match
        if not (min_v <= vol <= max_v):
             # Special check for range overlap if needed, but usually point-in-range
             # If vol is 150k, and range is 100k-1m, it matches.
             return False

    # 8. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        fraud = tx_context.get('monthly_fraud_level', 0)
        if not (min_f <= fraud <= max_f):
            return False

    # 9. Capture Delay (Range/Exact match)
    if rule.get('capture_delay'):
        # Capture delay in merchant_data is string ('manual', 'immediate', '1').
        # Rule is string range ('>5', '3-5', 'manual').
        # We map specific values to numeric or string matching.
        
        tx_delay = str(tx_context.get('capture_delay'))
        rule_delay = str(rule['capture_delay'])
        
        if rule_delay == 'manual':
            if tx_delay != 'manual': return False
        elif rule_delay == 'immediate':
            if tx_delay != 'immediate': return False
        else:
            # Numeric comparison
            # Map tx_delay to number
            try:
                if tx_delay == 'manual': val = 999
                elif tx_delay == 'immediate': val = 0
                else: val = float(tx_delay)
                
                min_d, max_d = parse_range(rule_delay)
                if not (min_d <= val <= max_d):
                    return False
            except:
                # Fallback if parsing fails
                if tx_delay != rule_delay: return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0)
    rate = rule.get('rate', 0)
    # Rate is "per 10000" (basis points * 100?) or just defined as /10000 in manual
    # Manual: "Variable rate to be especified to be multiplied by the transaction value and divided by 10000."
    variable = (rate * amount) / 10000
    return fixed + variable

# ═══════════════════════════════════════════════════════════
# MAIN LOGIC
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk5/data/context/payments.csv'
merchant_path = '/output/chunk5/data/context/merchant_data.json'
fees_path = '/output/chunk5/data/context/fees.json'

df_payments = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Filter for Merchant and Date (December 2023)
target_merchant = 'Belles_cookbook_store'
# December is day_of_year >= 335 (2023 is not leap year)
df_belles_dec = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['day_of_year'] >= 335)
].copy()

# 3. Get Merchant Context (Static)
merchant_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

# 4. Calculate Monthly Stats (Dynamic Context)
# "Monthly volumes and rates are computed always in natural months"
# We are calculating fees for December, so we use December's stats.
monthly_volume = df_belles_dec['eur_amount'].sum()

# Fraud Level: "ratio between monthly total volume and monthly volume notified as fraud"
fraud_txs = df_belles_dec[df_belles_dec['has_fraudulent_dispute'] == True]
monthly_fraud_volume = fraud_txs['eur_amount'].sum()

if monthly_volume > 0:
    monthly_fraud_level = monthly_fraud_volume / monthly_volume
else:
    monthly_fraud_level = 0.0

print(f"Merchant: {target_merchant}")
print(f"Transactions in Dec: {len(df_belles_dec)}")
print(f"Monthly Volume: €{monthly_volume:,.2f}")
print(f"Monthly Fraud Volume: €{monthly_fraud_volume:,.2f}")
print(f"Monthly Fraud Level: {monthly_fraud_level:.4%}")

# 5. Calculate Fees per Transaction
total_fees = 0.0
matched_count = 0
unmatched_count = 0

for idx, row in df_belles_dec.iterrows():
    # Build Transaction Context
    # Intracountry: Issuer == Acquirer
    is_intracountry = (row['issuing_country'] == row['acquirer_country'])
    
    tx_context = {
        'card_scheme': row['card_scheme'],
        'account_type': account_type,
        'capture_delay': capture_delay,
        'monthly_fraud_level': monthly_fraud_level, # Float ratio
        'monthly_volume': monthly_volume,           # Float amount
        'merchant_category_code': mcc,
        'is_credit': bool(row['is_credit']),
        'aci': row['aci'],
        'intracountry': is_intracountry
    }
    
    # Find matching rule
    # "The fee then is provided by fee = fixed_amount + rate * transaction_value / 10000"
    # We assume the first matching rule in the list is the correct one (priority order usually implied in rule engines, or rules are mutually exclusive).
    # Given the complexity, we iterate until first match.
    
    match_found = False
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            fee = calculate_fee(row['eur_amount'], rule)
            total_fees += fee
            match_found = True
            matched_count += 1
            # Debug first few matches
            if matched_count <= 3:
                print(f"Tx {idx}: Amt={row['eur_amount']}, Scheme={row['card_scheme']}, ACI={row['aci']} -> Matched Rule ID {rule['ID']}, Fee={fee:.4f}")
            break
    
    if not match_found:
        unmatched_count += 1
        # print(f"WARNING: No fee rule found for Tx {idx}: {tx_context}")

print(f"\nTotal Fees Calculation:")
print(f"Matched Transactions: {matched_count}")
print(f"Unmatched Transactions: {unmatched_count}")
print(f"Total Fees: €{total_fees:,.2f}")

# Final Output for the user question
print(f"{total_fees:.2f}")

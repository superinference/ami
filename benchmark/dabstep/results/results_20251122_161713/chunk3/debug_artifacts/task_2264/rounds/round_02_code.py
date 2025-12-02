# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2264
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7949 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
    if pd.isna(value) or value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        
        # Handle percentages
        if '%' in v:
            return float(v.replace('%', '')) / 100
            
        # Handle suffixes
        multiplier = 1
        if v.lower().endswith('k'):
            multiplier = 1000
            v = v[:-1]
        elif v.lower().endswith('m'):
            multiplier = 1_000_000
            v = v[:-1]
            
        try:
            # Handle ranges (e.g., "50-60") - return mean for simple coercion, 
            # but specific parsers should handle ranges for matching.
            if '-' in v and len(v.split('-')) == 2:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2 * multiplier
            return float(v) * multiplier
        except:
            return 0.0
    return 0.0

def parse_range(range_str):
    """Parses strings like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip()
    
    # Handle Percentage Ranges
    is_percent = '%' in s
    
    # Helper to clean and scale
    def clean_val(x):
        val = coerce_to_float(x)
        # coerce_to_float handles % division by 100, but for range parsing logic 
        # we might want to be careful. coerce_to_float("8.3%") -> 0.083. Correct.
        return val

    if '-' in s:
        parts = s.split('-')
        return clean_val(parts[0]), clean_val(parts[1])
    elif s.startswith('>'):
        return clean_val(s[1:]), float('inf')
    elif s.startswith('<'):
        return float('-inf'), clean_val(s[1:])
    else:
        # Exact value treated as range [val, val]
        val = clean_val(s)
        return val, val

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a transaction context matches a fee rule.
    tx_ctx: dict containing transaction and merchant details
    rule: dict representing a fee rule
    """
    # 1. Card Scheme (Exact Match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx.get('card_scheme'):
        return False

    # 2. Account Type (List Match - Wildcard if empty)
    if rule.get('account_type'):
        if tx_ctx.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List Match - Wildcard if empty)
    if rule.get('merchant_category_code'):
        if tx_ctx.get('mcc') not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean Match - Wildcard if None)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx.get('is_credit'):
            return False

    # 5. ACI (List Match - Wildcard if empty/None)
    if rule.get('aci'):
        if tx_ctx.get('aci') not in rule['aci']:
            return False

    # 6. Intracountry (Boolean/Float Match - Wildcard if None)
    if rule.get('intracountry') is not None:
        # Rule might use 0.0/1.0 or False/True
        rule_intra = bool(rule['intracountry'])
        tx_intra = bool(tx_ctx.get('intracountry'))
        if rule_intra != tx_intra:
            return False

    # 7. Capture Delay (Exact String Match - Wildcard if None)
    # Note: Manual implies specific categories.
    if rule.get('capture_delay'):
        if rule['capture_delay'] != tx_ctx.get('capture_delay'):
            return False

    # 8. Monthly Volume (Range Match - Wildcard if None)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        vol = tx_ctx.get('monthly_volume', 0)
        if not (min_v <= vol <= max_v):
            return False

    # 9. Monthly Fraud Level (Range Match - Wildcard if None)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        fraud = tx_ctx.get('monthly_fraud_rate', 0)
        # Handle edge cases where fraud might be slightly off due to float precision
        if not (min_f <= fraud <= max_f):
            # Allow tiny epsilon for float comparison if needed, but strict for now
            return False

    return True

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# File Paths
payments_path = '/output/chunk3/data/context/payments.csv'
fees_path = '/output/chunk3/data/context/fees.json'
merchant_data_path = '/output/chunk3/data/context/merchant_data.json'

# 1. Load Data
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Merchant and Timeframe (April 2023)
target_merchant = "Martinis_Fine_Steakhouse"
# April 2023: Day of Year 91 to 120
df_merchant_april = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['day_of_year'] >= 91) & 
    (df_payments['day_of_year'] <= 120)
].copy()

# 3. Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

# 4. Calculate Monthly Stats (Volume and Fraud Rate)
# Manual: "Monthly volumes and rates are computed always in natural months"
# We use the filtered April data for these stats.
monthly_volume = df_merchant_april['eur_amount'].sum()

# Fraud Rate: "ratio between monthly total volume and monthly volume notified as fraud"
fraud_volume = df_merchant_april[df_merchant_april['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

print(f"Merchant: {target_merchant}")
print(f"April Volume: €{monthly_volume:,.2f}")
print(f"April Fraud Rate: {monthly_fraud_rate:.4%}")

# 5. Find Fee Rule ID=12
fee_rule_12 = next((r for r in fees_data if r['ID'] == 12), None)
if not fee_rule_12:
    raise ValueError("Fee rule ID=12 not found in fees.json")

original_rate = fee_rule_12['rate']
new_rate = 99
rate_delta = new_rate - original_rate

print(f"Fee Rule 12 Original Rate: {original_rate}")
print(f"Fee Rule 12 New Rate: {new_rate}")
print(f"Rate Delta: {rate_delta}")

# 6. Calculate Delta for Matching Transactions
total_delta_eur = 0.0
matching_tx_count = 0

for _, tx in df_merchant_april.iterrows():
    # Build Transaction Context
    # Note: 'intracountry' is True if issuing_country == acquirer_country
    is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
    
    tx_ctx = {
        'card_scheme': tx['card_scheme'],
        'account_type': merchant_info['account_type'],
        'mcc': merchant_info['merchant_category_code'],
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': is_intracountry,
        'capture_delay': merchant_info['capture_delay'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate
    }
    
    # Check if this transaction matches Fee Rule 12
    if match_fee_rule(tx_ctx, fee_rule_12):
        matching_tx_count += 1
        amount = tx['eur_amount']
        # Fee formula: fixed + rate * amount / 10000
        # Delta formula: (new_rate - old_rate) * amount / 10000
        delta = rate_delta * amount / 10000.0
        total_delta_eur += delta

# 7. Output Result
print(f"Matching Transactions: {matching_tx_count}")
print(f"Total Delta: {total_delta_eur:.14f}")

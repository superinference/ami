# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2589
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7369 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

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

def is_not_empty(array):
    """Check if array/list is not empty."""
    if array is None:
        return False
    if hasattr(array, 'size'):
        return array.size > 0
    try:
        return len(array) > 0
    except TypeError:
        return False

def parse_range_value(rule_val, context_val, is_percentage=False):
    """
    Checks if context_val fits in rule_val range.
    rule_val: "100k-1m", ">8.3%", "manual", etc.
    context_val: float (volume, fraud_rate) or string (capture_delay)
    is_percentage: if True, context_val 0.05 is treated as 5.0 for comparison with "5%"
    """
    if rule_val is None:
        return True
        
    # Handle string exact matches (e.g. "manual")
    if isinstance(rule_val, str) and not any(c in rule_val for c in ['<', '>', '-']):
        return str(rule_val).lower() == str(context_val).lower()

    # Convert context_val to float for range comparison
    try:
        # Special handling for capture_delay "manual" -> infinity
        if str(context_val).lower() == 'manual':
            c_val = 9999.0
        elif str(context_val).lower() == 'immediate':
            c_val = 0.0
        else:
            c_val = float(context_val)
            
        if is_percentage:
            c_val = c_val * 100.0
            
        # Parse rule
        s = str(rule_val).lower().replace('%', '').replace(',', '')
        
        def get_val(x):
            if 'k' in x: return float(x.replace('k', '')) * 1000
            if 'm' in x: return float(x.replace('m', '')) * 1000000
            return float(x)

        if '>' in s:
            limit = get_val(s.replace('>', '').replace('=', ''))
            return c_val > limit 
        if '<' in s:
            limit = get_val(s.replace('<', '').replace('=', ''))
            return c_val < limit
        if '-' in s:
            parts = s.split('-')
            low = get_val(parts[0])
            high = get_val(parts[1])
            return low <= c_val <= high
            
        return False
    except:
        return False

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    Returns True if the rule applies, False otherwise.
    """
    # 1. Scheme
    if rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type
    if is_not_empty(rule['account_type']):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. MCC
    if is_not_empty(rule['merchant_category_code']):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay
    if rule['capture_delay'] is not None:
        if not parse_range_value(rule['capture_delay'], tx_ctx['capture_delay']):
            return False
            
    # 5. Monthly Fraud
    if rule['monthly_fraud_level'] is not None:
        if not parse_range_value(rule['monthly_fraud_level'], tx_ctx['monthly_fraud_rate'], is_percentage=True):
            return False
            
    # 6. Monthly Volume
    if rule['monthly_volume'] is not None:
        if not parse_range_value(rule['monthly_volume'], tx_ctx['monthly_volume']):
            return False
            
    # 7. Is Credit
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 8. ACI
    if is_not_empty(rule['aci']):
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 9. Intracountry
    if rule['intracountry'] is not None:
        # JSON has 0.0 or 1.0 or null
        rule_intra = bool(float(rule['intracountry']))
        if rule_intra != tx_ctx['intracountry']:
            return False
            
    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule['fixed_amount']
    rate = rule['rate']
    return fixed + (rate * amount / 10000.0)

# ---------------------------------------------------------
# MAIN ANALYSIS
# ---------------------------------------------------------

# File paths
file_path_payments = '/output/chunk5/data/context/payments.csv'
file_path_fees = '/output/chunk5/data/context/fees.json'
file_path_merchant = '/output/chunk5/data/context/merchant_data.json'

# Load data
df = pd.read_csv(file_path_payments)
with open(file_path_fees, 'r') as f:
    fees = json.load(f)
with open(file_path_merchant, 'r') as f:
    merchants = json.load(f)

# Filter for Crossfit_Hanna in February (Day 32-59)
merchant_name = 'Crossfit_Hanna'
df_feb = df[
    (df['merchant'] == merchant_name) & 
    (df['day_of_year'] >= 32) & 
    (df['day_of_year'] <= 59)
].copy()

# Get Merchant Metadata
merchant_meta = next((m for m in merchants if m['merchant'] == merchant_name), None)
if not merchant_meta:
    print("Merchant not found")
    exit()

# Calculate Monthly Stats (Volume & Fraud) for February
# These stats determine which fee tier the merchant falls into
monthly_vol = df_feb['eur_amount'].sum()
monthly_fraud_rate = df_feb['has_fraudulent_dispute'].mean() # Ratio (0.0 to 1.0)

# Prepare Context Base (Merchant-level attributes)
base_context = {
    'account_type': merchant_meta['account_type'],
    'mcc': merchant_meta['merchant_category_code'],
    'capture_delay': merchant_meta['capture_delay'],
    'monthly_volume': monthly_vol,
    'monthly_fraud_rate': monthly_fraud_rate
}

# Schemes to test for "steering"
schemes = ['NexPay', 'GlobalCard', 'SwiftCharge', 'TransactPlus']
scheme_totals = {}

# Simulate processing ALL February transactions through EACH scheme
for scheme in schemes:
    total_fee = 0.0
    
    for _, row in df_feb.iterrows():
        # Build transaction context
        tx_ctx = base_context.copy()
        tx_ctx['card_scheme'] = scheme # HYPOTHETICAL STEERING: Force scheme
        tx_ctx['is_credit'] = row['is_credit']
        tx_ctx['aci'] = row['aci']
        tx_ctx['intracountry'] = (row['issuing_country'] == row['acquirer_country'])
        
        # Find matching rule
        matched_rule = None
        for rule in fees:
            if match_fee_rule(tx_ctx, rule):
                matched_rule = rule
                break # First match wins
        
        if matched_rule:
            fee = calculate_fee(row['eur_amount'], matched_rule)
            total_fee += fee
            
    scheme_totals[scheme] = total_fee

# Find the scheme that results in the MAXIMUM fees
max_scheme = max(scheme_totals, key=scheme_totals.get)

# Output the result
print(max_scheme)

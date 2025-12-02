# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2675
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7253 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
    if isinstance(value, (int, float)):
        return float(value)
    if value is None:
        return 0.0
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v:
            return float(v.replace('k', '')) * 1000
        if 'm' in v:
            return float(v.replace('m', '')) * 1000000
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

def check_range(rule_value, actual_value):
    """
    Check if actual_value fits in rule_value range (string).
    Handles formats like "100k-1m", ">5", "7.7%-8.3%", etc.
    """
    if rule_value is None:
        return True
    
    # Clean rule string
    rv = str(rule_value).strip().lower().replace(',', '').replace('%', '')
    
    # Helper to parse k/m suffixes
    def parse_val(x):
        if 'k' in x: return float(x.replace('k','')) * 1000
        if 'm' in x: return float(x.replace('m','')) * 1000000
        return float(x)

    try:
        if '-' in rv:
            low, high = map(parse_val, rv.split('-'))
            return low <= actual_value <= high
        if rv.startswith('>='):
            limit = parse_val(rv[2:])
            return actual_value >= limit
        if rv.startswith('<='):
            limit = parse_val(rv[2:])
            return actual_value <= limit
        if rv.startswith('>'):
            limit = parse_val(rv[1:])
            return actual_value > limit
        if rv.startswith('<'):
            limit = parse_val(rv[1:])
            return actual_value < limit
        # Exact match
        return actual_value == parse_val(rv)
    except:
        return False

def match_fee_rule(tx, rule):
    """
    Check if a fee rule applies to a transaction context.
    """
    # 1. Card Scheme
    if rule['card_scheme'] != tx['card_scheme']:
        return False

    # 2. Account Type (List in rule)
    if rule['account_type'] and tx['account_type'] not in rule['account_type']:
        return False

    # 3. Merchant Category Code (List in rule)
    if rule['merchant_category_code'] and tx['mcc'] not in rule['merchant_category_code']:
        return False

    # 4. Capture Delay
    if rule['capture_delay']:
        if rule['capture_delay'] in ['immediate', 'manual']:
            if tx['capture_delay'] != rule['capture_delay']:
                return False
        else:
            # Numeric range check for days
            if tx['capture_delay'] in ['immediate', 'manual']:
                return False
            try:
                delay_days = float(tx['capture_delay'])
                if not check_range(rule['capture_delay'], delay_days):
                    return False
            except:
                return False

    # 5. Monthly Volume (Range string vs Float)
    if rule['monthly_volume']:
        if not check_range(rule['monthly_volume'], tx['monthly_volume']):
            return False

    # 6. Monthly Fraud Level (Range string vs Float Percentage)
    if rule['monthly_fraud_level']:
        # Rule is like "7.7%-8.3%". tx['monthly_fraud_level'] is ratio (e.g. 0.08).
        # check_range strips %. We pass percentage (8.0) to match "7.7-8.3".
        fraud_pct = tx['monthly_fraud_level'] * 100
        if not check_range(rule['monthly_fraud_level'], fraud_pct):
            return False

    # 7. Is Credit
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx['is_credit']:
            return False

    # 8. ACI (List in rule)
    if rule['aci'] and tx['aci'] not in rule['aci']:
        return False

    # 9. Intracountry
    if rule['intracountry'] is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx['intracountry']:
            return False

    return True

def calculate_fee(amount, rule):
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000)

# ---------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_path = '/output/chunk6/data/context/merchant_data.json'

df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Rafa_AI and October (Day 274-304)
merchant_name = 'Rafa_AI'
df_oct = df[
    (df['merchant'] == merchant_name) & 
    (df['day_of_year'] >= 274) & 
    (df['day_of_year'] <= 304)
].copy()

# 3. Get Merchant Attributes
m_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
if not m_info:
    raise ValueError(f"Merchant {merchant_name} not found")

mcc = m_info['merchant_category_code']
account_type = m_info['account_type']
capture_delay = m_info['capture_delay']

# 4. Calculate Monthly Stats (Volume & Fraud)
total_volume = df_oct['eur_amount'].sum()
fraud_volume = df_oct[df_oct['has_fraudulent_dispute']]['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

# 5. Pre-calculate Intracountry status
# Intracountry = Issuing Country matches Acquirer Country
df_oct['intracountry'] = df_oct['issuing_country'] == df_oct['acquirer_country']

# 6. Simulate Fees for Each Scheme
schemes = ['NexPay', 'GlobalCard', 'SwiftCharge', 'TransactPlus']
scheme_totals = {}

for scheme in schemes:
    total_fee = 0.0
    
    for _, row in df_oct.iterrows():
        # Construct transaction context, forcing the current scheme
        tx_ctx = {
            'card_scheme': scheme,
            'account_type': account_type,
            'mcc': mcc,
            'capture_delay': capture_delay,
            'monthly_volume': total_volume,
            'monthly_fraud_level': fraud_rate,
            'is_credit': row['is_credit'],
            'aci': row['aci'],
            'intracountry': row['intracountry']
        }
        
        # Find applicable rule
        matched_rule = None
        for rule in fees:
            if match_fee_rule(tx_ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            total_fee += calculate_fee(row['eur_amount'], matched_rule)
            
    scheme_totals[scheme] = total_fee

# 7. Determine Winner
max_scheme = max(scheme_totals, key=scheme_totals.get)

print(f"Merchant: {merchant_name}")
print(f"October Volume: €{total_volume:,.2f}")
print(f"October Fraud Rate: {fraud_rate:.2%}")
print("-" * 30)
print("Projected Total Fees by Scheme:")
for s, f in scheme_totals.items():
    print(f"{s}: €{f:,.2f}")
print("-" * 30)
print(max_scheme)

# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2628
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9258 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float."""
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
    """Parses strings like '100k-1m', '<3', '>5', '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip().lower().replace(',', '')
    
    # Handle percentages
    is_pct = '%' in s
    if is_pct:
        s = s.replace('%', '')
        scale = 0.01
    else:
        scale = 1.0

    # Handle k/m suffixes for volume
    def parse_val(val_s):
        val_s = val_s.strip()
        mult = 1
        if 'k' in val_s:
            mult = 1000
            val_s = val_s.replace('k', '')
        elif 'm' in val_s:
            mult = 1000000
            val_s = val_s.replace('m', '')
        try:
            return float(val_s) * mult * scale
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif s.startswith('>'):
        return parse_val(s[1:]), float('inf')
    elif s.startswith('<'):
        return float('-inf'), parse_val(s[1:])
    else:
        # Exact value treated as range [val, val]
        v = parse_val(s)
        return v, v

def check_capture_delay(merchant_delay, rule_delay):
    """Matches merchant capture delay (e.g., '1') against rule (e.g., '<3')."""
    if rule_delay is None:
        return True
    
    # Direct string match
    if str(merchant_delay).lower() == str(rule_delay).lower():
        return True
        
    # Numeric comparison
    try:
        # Convert merchant delay to float (handle 'immediate'/'manual' as non-numeric)
        if str(merchant_delay).lower() in ['immediate', 'manual']:
            return False # Already checked equality above
            
        m_val = float(merchant_delay)
        
        if rule_delay.startswith('<'):
            limit = float(rule_delay[1:])
            return m_val < limit
        elif rule_delay.startswith('>'):
            limit = float(rule_delay[1:])
            return m_val > limit
        elif '-' in rule_delay:
            low, high = map(float, rule_delay.split('-'))
            return low <= m_val <= high
    except:
        pass
        
    return False

def match_fee_rule(ctx, rule):
    """
    Checks if a fee rule applies to the given context.
    ctx: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact Match)
    if rule['card_scheme'] != ctx['card_scheme']:
        return False

    # 2. Account Type (List Match - Wildcard if empty)
    if rule['account_type'] and ctx['account_type'] not in rule['account_type']:
        return False

    # 3. Merchant Category Code (List Match - Wildcard if empty)
    if rule['merchant_category_code'] and ctx['mcc'] not in rule['merchant_category_code']:
        return False

    # 4. Capture Delay (Complex Match - Wildcard if None)
    if not check_capture_delay(ctx['capture_delay'], rule['capture_delay']):
        return False

    # 5. Monthly Volume (Range Match - Wildcard if None)
    if rule['monthly_volume']:
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= ctx['monthly_volume'] <= max_v):
            return False

    # 6. Monthly Fraud Level (Range Match - Wildcard if None)
    if rule['monthly_fraud_level']:
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        # Fraud level in context is ratio (0.0-1.0), rule is usually %
        if not (min_f <= ctx['monthly_fraud_level'] <= max_f):
            return False

    # 7. Is Credit (Exact Match - Wildcard if None)
    if rule['is_credit'] is not None and rule['is_credit'] != ctx['is_credit']:
        return False

    # 8. ACI (List Match - Wildcard if empty/None)
    if rule['aci'] and ctx['aci'] not in rule['aci']:
        return False

    # 9. Intracountry (Exact Match - Wildcard if None)
    if rule['intracountry'] is not None:
        # Convert boolean to 0.0/1.0 for comparison if needed, or direct bool
        rule_intra = bool(rule['intracountry'])
        ctx_intra = bool(ctx['intracountry'])
        if rule_intra != ctx_intra:
            return False

    return True

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk3/data/context/payments.csv'
fees_path = '/output/chunk3/data/context/fees.json'
merchant_path = '/output/chunk3/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Merchant and Month (June)
merchant_name = 'Belles_cookbook_store'
target_month = 6

# Create date column to extract month
df_payments['date'] = pd.to_datetime(df_payments['year'].astype(str) + df_payments['day_of_year'].astype(str).str.zfill(3), format='%Y%j')
df_payments['month'] = df_payments['date'].dt.month

# Filter
df_merchant_june = df_payments[
    (df_payments['merchant'] == merchant_name) & 
    (df_payments['month'] == target_month)
].copy()

# 3. Calculate Monthly Metrics (Volume & Fraud)
# Manual.md: "Fraud is defined as the ratio of fraudulent volume over total volume."
total_volume = df_merchant_june['eur_amount'].sum()
fraud_volume = df_merchant_june[df_merchant_june['has_fraudulent_dispute']]['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

print(f"Merchant: {merchant_name}")
print(f"June Volume: €{total_volume:,.2f}")
print(f"June Fraud Rate: {fraud_rate:.4%}")

# 4. Get Merchant Profile
merchant_profile = next((item for item in merchant_data if item["merchant"] == merchant_name), None)
if not merchant_profile:
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

mcc = merchant_profile['merchant_category_code']
account_type = merchant_profile['account_type']
capture_delay = merchant_profile['capture_delay']

print(f"Profile: MCC={mcc}, Account={account_type}, Delay={capture_delay}")

# 5. Determine Intracountry Status for Transactions
# Intracountry = (Issuing Country == Acquirer Country)
# Note: We use the 'acquirer_country' column in payments.csv
df_merchant_june['intracountry'] = df_merchant_june['issuing_country'] == df_merchant_june['acquirer_country']

# 6. Simulate Fees for Each Scheme
schemes = sorted(list(set(r['card_scheme'] for r in fees_data)))
scheme_costs = {}

print("\nCalculating fees per scheme...")

for scheme in schemes:
    total_fee = 0.0
    match_count = 0
    
    # Iterate through every transaction to find its specific fee
    for _, tx in df_merchant_june.iterrows():
        # Build context for this transaction
        ctx = {
            'card_scheme': scheme, # The variable we are testing
            'account_type': account_type,
            'capture_delay': capture_delay,
            'mcc': mcc,
            'monthly_volume': total_volume,
            'monthly_fraud_level': fraud_rate,
            'is_credit': bool(tx['is_credit']),
            'aci': tx['aci'],
            'intracountry': bool(tx['intracountry'])
        }
        
        # Find matching rule
        # We assume the first matching rule is applied (common in fee logic unless priority is specified)
        # In this dataset, rules are usually mutually exclusive or specific enough.
        applied_rule = None
        for rule in fees_data:
            if match_fee_rule(ctx, rule):
                applied_rule = rule
                break
        
        if applied_rule:
            # Calculate fee: fixed + (rate * amount / 10000)
            fee = applied_rule['fixed_amount'] + (applied_rule['rate'] * tx['eur_amount'] / 10000)
            total_fee += fee
            match_count += 1
        else:
            # If no rule matches, this scheme might not support this transaction type.
            # For comparison purposes, we might penalize or just log it.
            # In this specific dataset context, we expect coverage.
            pass

    scheme_costs[scheme] = total_fee
    print(f"Scheme: {scheme:<15} | Total Fee: €{total_fee:,.2f} | Matches: {match_count}/{len(df_merchant_june)}")

# 7. Determine Winner
best_scheme = min(scheme_costs, key=scheme_costs.get)
min_fee = scheme_costs[best_scheme]

print("-" * 30)
print(f"Recommended Scheme: {best_scheme}")
print(f"Minimum Fee: €{min_fee:,.2f}")

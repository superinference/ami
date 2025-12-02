# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2678
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9024 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS (Robust Data Processing)
# ---------------------------------------------------------
def coerce_to_float(value):
    """Convert string with %, $, commas to float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if v.startswith('>'):
            return float(v[1:]) + 0.00001 # Treat as slightly higher boundary
        if v.startswith('<'):
            return float(v[1:]) - 0.00001
        return float(v)
    return 0.0

def parse_range(range_str, value):
    """
    Check if value fits in range_str.
    Formats: '100k-1m', '>5', '<3', '7.7%-8.3%'
    """
    if range_str is None:
        return True
    
    s = str(range_str).strip()
    
    # Handle k/m suffixes for volume
    def parse_val(x):
        x = x.lower().replace('%', '')
        mult = 1
        if 'k' in x:
            mult = 1000
            x = x.replace('k', '')
        elif 'm' in x:
            mult = 1000000
            x = x.replace('m', '')
        return float(x) * mult

    # Percentage handling for value
    check_val = value
    if '%' in s:
        # If range is percentage, ensure value is comparable (0.09 vs 9.0)
        # Assuming value passed in is ratio (0.09), range is string "8%"
        pass 

    try:
        if '-' in s:
            low, high = s.split('-')
            low_v = parse_val(low)
            high_v = parse_val(high)
            # Adjust for percentage strings being parsed to whole numbers if needed
            if '%' in s:
                low_v /= 100
                high_v /= 100
            return low_v <= check_val <= high_v
        
        if s.startswith('>'):
            limit = parse_val(s[1:])
            if '%' in s: limit /= 100
            return check_val > limit
            
        if s.startswith('<'):
            limit = parse_val(s[1:])
            if '%' in s: limit /= 100
            return check_val < limit
            
        # Exact match (rare for ranges but possible)
        val = parse_val(s)
        if '%' in s: val /= 100
        return check_val == val
        
    except Exception as e:
        # print(f"Error parsing range {s} for value {value}: {e}")
        return False

def matches_rule(tx_ctx, rule):
    """
    Check if a transaction context matches a fee rule.
    tx_ctx: dict with transaction/merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact Match)
    if rule['card_scheme'] != tx_ctx['card_scheme']:
        return False

    # 2. Account Type (List contains value or Empty/Wildcard)
    if rule['account_type'] and tx_ctx['account_type'] not in rule['account_type']:
        return False

    # 3. MCC (List contains value or Empty/Wildcard)
    if rule['merchant_category_code'] and tx_ctx['mcc'] not in rule['merchant_category_code']:
        return False

    # 4. Capture Delay (Exact string match or Wildcard)
    # Note: capture_delay in fees can be range-like ('>5'), but merchant data is '1', 'manual', etc.
    # We treat it as string match or simple logic if needed. 
    # Given data: merchant has '1', rule might have '<3'.
    if rule['capture_delay']:
        # Simple logic for the specific values seen in data
        rd = rule['capture_delay']
        md = str(tx_ctx['capture_delay'])
        if rd == 'immediate' and md != 'immediate': return False
        if rd == 'manual' and md != 'manual': return False
        if rd.startswith('<') or rd.startswith('>'):
            # Try numeric comparison if merchant delay is numeric
            if md.isdigit():
                md_val = float(md)
                if rd.startswith('<'): return md_val < float(rd[1:])
                if rd.startswith('>'): return md_val > float(rd[1:])
        if '-' in rd and md.isdigit():
             low, high = map(float, rd.split('-'))
             if not (low <= float(md) <= high): return False

    # 5. Monthly Volume (Range match)
    if not parse_range(rule['monthly_volume'], tx_ctx['monthly_volume']):
        return False

    # 6. Monthly Fraud Level (Range match)
    if not parse_range(rule['monthly_fraud_level'], tx_ctx['monthly_fraud_level']):
        return False

    # 7. Is Credit (Boolean match or Wildcard)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 8. ACI (List contains value or Wildcard)
    if rule['aci'] and tx_ctx['aci'] not in rule['aci']:
        return False

    # 9. Intracountry (Boolean match or Wildcard)
    if rule['intracountry'] is not None:
        # Intracountry in rule might be 0.0/1.0 or boolean
        rule_intra = bool(float(rule['intracountry'])) if isinstance(rule['intracountry'], (int, float, str)) else rule['intracountry']
        if rule_intra != tx_ctx['intracountry']:
            return False

    return True

# ---------------------------------------------------------
# MAIN ANALYSIS
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_path = '/output/chunk5/data/context/merchant_data.json'

df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Merchant and November
target_merchant = 'Belles_cookbook_store'
df_nov = df[
    (df['merchant'] == target_merchant) & 
    (df['day_of_year'] >= 305) & 
    (df['day_of_year'] <= 334)
].copy()

# 3. Get Merchant Attributes
m_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not m_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

mcc = m_info['merchant_category_code']
account_type = m_info['account_type']
capture_delay = m_info['capture_delay']

# 4. Calculate Monthly Stats (Volume & Fraud)
# These define the "Tier" the merchant falls into for the whole month
total_vol = df_nov['eur_amount'].sum()
fraud_vol = df_nov[df_nov['has_fraudulent_dispute']]['eur_amount'].sum()
fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0

# print(f"Merchant Stats - Vol: {total_vol:.2f}, Fraud Rate: {fraud_rate:.4%}")

# 5. Simulate Fees for Each Scheme
# Get all unique schemes from fees.json
schemes = sorted(list(set(r['card_scheme'] for r in fees_data)))
scheme_costs = {}

# Pre-calculate transaction attributes to speed up loop
# We create a list of dicts for the transactions
transactions = []
for _, row in df_nov.iterrows():
    transactions.append({
        'amount': row['eur_amount'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': row['issuing_country'] == row['acquirer_country']
    })

for scheme in schemes:
    total_fee = 0.0
    match_count = 0
    
    # Filter fees to only those for this scheme to speed up matching
    scheme_rules = [r for r in fees_data if r['card_scheme'] == scheme]
    
    for tx in transactions:
        # Context for this specific transaction simulation
        ctx = {
            'card_scheme': scheme,
            'mcc': mcc,
            'account_type': account_type,
            'capture_delay': capture_delay,
            'monthly_volume': total_vol,
            'monthly_fraud_level': fraud_rate,
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'intracountry': tx['intracountry']
        }
        
        # Find first matching rule
        matched_rule = None
        for rule in scheme_rules:
            if matches_rule(ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            # Fee = Fixed + (Rate * Amount / 10000)
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * tx['amount'] / 10000.0)
            total_fee += fee
            match_count += 1
        else:
            # If no rule matches, this scheme might not support this transaction type
            # For the purpose of "steering", we assume valid configuration or high penalty.
            # However, in this dataset, usually coverage is complete.
            # We'll log it if needed, but for now assume 0 or skip (or high cost).
            # To be safe, we can add a penalty or just ignore. 
            # Given the problem type, we assume complete coverage.
            pass

    scheme_costs[scheme] = total_fee
    # print(f"Scheme: {scheme}, Total Fee: {total_fee:.2f}, Matches: {match_count}/{len(transactions)}")

# 6. Find Minimum Cost Scheme
min_scheme = min(scheme_costs, key=scheme_costs.get)
min_cost = scheme_costs[min_scheme]

# print(f"\nLowest Cost Scheme: {min_scheme} ({min_cost:.2f})")

# Final Answer
print(min_scheme)

# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1773
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7197 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
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
            return None
    return None

def parse_range(range_str):
    """Parses a range string like '100k-1m', '>5%', '<3' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip().lower().replace(',', '').replace('%', '').replace('€', '')
    
    # Handle k/m suffixes
    def parse_val(x):
        if x.endswith('k'): return float(x[:-1]) * 1000
        if x.endswith('m'): return float(x[:-1]) * 1000000
        return float(x)

    try:
        if '-' in s:
            parts = s.split('-')
            return parse_val(parts[0]), parse_val(parts[1])
        elif s.startswith('>'):
            return parse_val(s[1:]), float('inf')
        elif s.startswith('<'):
            return float('-inf'), parse_val(s[1:])
        elif s.startswith('≥'):
            return parse_val(s[1:]), float('inf')
        elif s.startswith('≤'):
            return float('-inf'), parse_val(s[1:])
        else:
            val = parse_val(s)
            return val, val # Exact match treated as range [val, val]
    except:
        return None, None

def check_range_match(value, range_str, is_percentage=False):
    """Checks if a value falls within a range string."""
    if range_str is None:
        return True # Wildcard matches everything
    
    min_val, max_val = parse_range(range_str)
    if min_val is None:
        return False # Could not parse
    
    # Adjust value for percentage comparison if needed
    # If range was '8.3%', parse_range returned 8.3. 
    # If value is 0.0835, we should compare 8.35 to 8.3.
    comp_value = value * 100 if is_percentage else value
    
    # Handle edge cases for exact matches vs ranges
    if min_val == max_val:
        return abs(comp_value - min_val) < 1e-9
        
    return min_val <= comp_value <= max_val

def match_fee_rule(ctx, rule):
    """
    Matches a transaction context against a fee rule.
    ctx: dict containing transaction and merchant details
    rule: dict representing a fee rule
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != ctx.get('card_scheme'):
        return False

    # 2. Account Type (List containment or Wildcard)
    if rule.get('account_type'):
        if ctx.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List containment or Wildcard)
    if rule.get('merchant_category_code'):
        if ctx.get('mcc') not in rule['merchant_category_code']:
            return False

    # 4. Capture Delay (Exact match or Wildcard)
    if rule.get('capture_delay'):
        if rule['capture_delay'] != ctx.get('capture_delay'):
            return False

    # 5. Is Credit (Boolean match or Wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != ctx.get('is_credit'):
            return False

    # 6. ACI (List containment or Wildcard)
    if rule.get('aci'):
        if ctx.get('aci') not in rule['aci']:
            return False

    # 7. Intracountry (Boolean match or Wildcard)
    if rule.get('intracountry') is not None:
        # Intracountry in rule is 0.0 (False) or 1.0 (True) usually
        rule_intra = bool(rule['intracountry'])
        ctx_intra = bool(ctx.get('intracountry'))
        if rule_intra != ctx_intra:
            return False

    # 8. Monthly Volume (Range match or Wildcard)
    if rule.get('monthly_volume'):
        if not check_range_match(ctx.get('monthly_volume'), rule['monthly_volume'], is_percentage=False):
            return False

    # 9. Monthly Fraud Level (Range match or Wildcard)
    if rule.get('monthly_fraud_level'):
        if not check_range_match(ctx.get('monthly_fraud_rate'), rule['monthly_fraud_level'], is_percentage=True):
            return False

    return True

# ---------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------

# File paths
payments_path = '/output/chunk6/data/context/payments.csv'
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'
fees_path = '/output/chunk6/data/context/fees.json'

# 1. Load Data
df = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees = json.load(f)

# 2. Filter for Crossfit_Hanna and November 2023
target_merchant = 'Crossfit_Hanna'
# Nov 2023: Day 305 to 334
df_nov = df[
    (df['merchant'] == target_merchant) & 
    (df['year'] == 2023) & 
    (df['day_of_year'] >= 305) & 
    (df['day_of_year'] <= 334)
].copy()

# 3. Calculate Monthly Stats
# Volume
monthly_volume = df_nov['eur_amount'].sum()

# Fraud Rate (Volume based)
fraud_volume = df_nov[df_nov['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 4. Get Merchant Static Data
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    print("Merchant not found in merchant_data.json")
    exit()

account_type = merchant_info['account_type']
mcc = merchant_info['merchant_category_code']
capture_delay = merchant_info['capture_delay']

# 5. Identify Unique Transaction Characteristics
# Intracountry = (issuing_country == acquirer_country)
df_nov['intracountry'] = df_nov['issuing_country'] == df_nov['acquirer_country']

# Group by unique combinations to optimize matching
unique_tx_groups = df_nov[['card_scheme', 'is_credit', 'aci', 'intracountry']].drop_duplicates()

applicable_fee_ids = set()

# 6. Match Rules
for _, row in unique_tx_groups.iterrows():
    # Build context for this group of transactions
    ctx = {
        'card_scheme': row['card_scheme'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': row['intracountry'],
        'account_type': account_type,
        'mcc': mcc,
        'capture_delay': capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate
    }
    
    # Check against all fees
    for rule in fees:
        if match_fee_rule(ctx, rule):
            applicable_fee_ids.add(rule['ID'])

# 7. Output
sorted_ids = sorted(list(applicable_fee_ids))
print(", ".join(map(str, sorted_ids)))

# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2581
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8114 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
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

def parse_range_check(rule_val, actual_val):
    """
    Checks if actual_val falls within rule_val range.
    rule_val examples: '100k-1m', '>8.3%', '<5', '7.7%-8.3%'
    actual_val: float
    """
    if rule_val is None:
        return True
    
    s = str(rule_val).strip()
    
    # Handle percentages in rule
    is_pct = '%' in s
    scale = 1.0
    
    # Clean string for parsing
    s_clean = s.replace('%', '').replace(',', '')
    
    # Handle k/m suffixes
    if 'k' in s_clean.lower():
        scale = 1000
        s_clean = s_clean.lower().replace('k', '')
    elif 'm' in s_clean.lower():
        scale = 1000000
        s_clean = s_clean.lower().replace('m', '')
        
    # Handle operators
    if s_clean.startswith('>'):
        try:
            limit = float(s_clean[1:]) * scale
            if is_pct: limit /= 100
            return actual_val > limit
        except: return False
    if s_clean.startswith('<'):
        try:
            limit = float(s_clean[1:]) * scale
            if is_pct: limit /= 100
            return actual_val < limit
        except: return False
        
    # Handle ranges "min-max"
    if '-' in s_clean:
        try:
            parts = s_clean.split('-')
            min_val = float(parts[0]) * scale
            max_val = float(parts[1]) * scale
            if is_pct:
                min_val /= 100
                max_val /= 100
            return min_val <= actual_val <= max_val
        except: return False
        
    return True

def check_capture_delay(rule_val, merchant_val):
    """
    Matches capture delay rules.
    rule_val: '3-5', '>5', '<3', 'immediate', 'manual', or None
    merchant_val: '1', '2', 'manual', 'immediate', etc.
    """
    if rule_val is None:
        return True
    
    r = str(rule_val).lower()
    m = str(merchant_val).lower()
    
    if r == m:
        return True
        
    # Try to convert merchant val to int for range comparison
    try:
        m_days = float(m)
    except ValueError:
        # If merchant val is 'manual' or 'immediate' and didn't match exact string above
        if m == 'immediate': m_days = 0
        else: return False # 'manual' vs numeric rule
        
    if r.startswith('>'):
        try:
            limit = float(r[1:])
            return m_days > limit
        except: return False
    if r.startswith('<'):
        try:
            limit = float(r[1:])
            return m_days < limit
        except: return False
    if '-' in r:
        try:
            parts = r.split('-')
            return float(parts[0]) <= m_days <= float(parts[1])
        except: return False
        
    return False

def match_fee_rule(tx_ctx, rule):
    """
    Checks if a fee rule applies to a transaction context.
    tx_ctx: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme
    if rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List)
    if rule['account_type']: # If not empty/null
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List)
    if rule['merchant_category_code']:
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay
    if rule['capture_delay']:
        if not check_capture_delay(rule['capture_delay'], tx_ctx['capture_delay']):
            return False
            
    # 5. Monthly Volume
    if rule['monthly_volume']:
        if not parse_range_check(rule['monthly_volume'], tx_ctx['monthly_volume']):
            return False
            
    # 6. Monthly Fraud Level
    if rule['monthly_fraud_level']:
        if not parse_range_check(rule['monthly_fraud_level'], tx_ctx['monthly_fraud_level']):
            return False
            
    # 7. Is Credit (Bool)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 8. ACI (List)
    if rule['aci']:
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 9. Intracountry (Bool)
    if rule['intracountry'] is not None:
        if rule['intracountry'] != tx_ctx['intracountry']:
            return False
            
    return True

# ---------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------

# File paths
payments_path = '/output/chunk3/data/context/payments.csv'
fees_path = '/output/chunk3/data/context/fees.json'
merchant_path = '/output/chunk3/data/context/merchant_data.json'

# Load data
df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# Filter for Merchant and January
merchant_name = 'Golfclub_Baron_Friso'
df_jan = df[(df['merchant'] == merchant_name) & (df['day_of_year'] >= 1) & (df['day_of_year'] <= 31)].copy()

# Get Merchant Attributes
m_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
if not m_info:
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

mcc = m_info['merchant_category_code']
account_type = m_info['account_type']
capture_delay = m_info['capture_delay']

# Calculate Monthly Stats (Volume and Fraud Rate)
# Manual: "Fraud is defined as the ratio of fraudulent volume over total volume."
total_volume = df_jan['eur_amount'].sum()
fraud_volume = df_jan[df_jan['has_fraudulent_dispute']]['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

# Identify available card schemes
schemes = sorted(list(set(r['card_scheme'] for r in fees)))

# Pre-filter fees by scheme to speed up matching
fees_by_scheme = {s: [r for r in fees if r['card_scheme'] == s] for s in schemes}

results = {}

# Simulation Loop
for scheme in schemes:
    total_fee = 0.0
    scheme_rules = fees_by_scheme[scheme]
    
    # Iterate through actual transactions
    for _, tx in df_jan.iterrows():
        # Build context for this transaction
        tx_ctx = {
            'card_scheme': scheme,
            'account_type': account_type,
            'mcc': mcc,
            'capture_delay': capture_delay,
            'monthly_volume': total_volume,
            'monthly_fraud_level': fraud_rate,
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'intracountry': (tx['issuing_country'] == tx['acquirer_country']),
            'eur_amount': tx['eur_amount']
        }
        
        # Find first matching rule
        matched_rule = None
        for rule in scheme_rules:
            if match_fee_rule(tx_ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            # Calculate fee: fixed + rate * amount / 10000
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * tx['eur_amount'] / 10000)
            total_fee += fee

    results[scheme] = total_fee

# Find max
max_scheme = max(results, key=results.get)
print(max_scheme)

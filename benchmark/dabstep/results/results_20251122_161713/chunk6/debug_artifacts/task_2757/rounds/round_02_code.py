# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2757
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8029 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---
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

def parse_volume_range(range_str):
    """Parses volume strings like '100k-1m' into (min, max)."""
    if not range_str:
        return (0, float('inf'))
    
    s = str(range_str).lower().replace(',', '')
    
    def parse_val(x):
        mul = 1
        if 'k' in x:
            mul = 1000
            x = x.replace('k', '')
        elif 'm' in x:
            mul = 1000000
            x = x.replace('m', '')
        try:
            return float(x) * mul
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return (parse_val(parts[0]), parse_val(parts[1]))
    elif '>' in s:
        return (parse_val(s.replace('>', '')), float('inf'))
    elif '<' in s:
        return (0, parse_val(s.replace('<', '')))
    return (0, float('inf'))

def parse_fraud_range(range_str):
    """Parses fraud strings like '>8.3%' into (min, max)."""
    if not range_str:
        return (0.0, 1.0)
    
    s = str(range_str).replace('%', '')
    
    if '-' in s:
        parts = s.split('-')
        return (float(parts[0])/100, float(parts[1])/100)
    elif '>' in s:
        return (float(s.replace('>', ''))/100, 1.0)
    elif '<' in s:
        return (0.0, float(s.replace('<', ''))/100)
    return (0.0, 1.0)

def match_fee_rule(tx_ctx, rule):
    """
    Checks if a fee rule applies to a transaction context.
    tx_ctx: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Explicit match required for the simulation)
    if rule['card_scheme'] != tx_ctx['target_scheme']:
        return False

    # 2. Account Type (Rule list must contain merchant's type, or be empty)
    if rule['account_type'] and tx_ctx['account_type'] not in rule['account_type']:
        return False

    # 3. Merchant Category Code (Rule list must contain merchant's MCC, or be empty)
    if rule['merchant_category_code'] and tx_ctx['mcc'] not in rule['merchant_category_code']:
        return False

    # 4. Capture Delay
    if rule['capture_delay']:
        rd = str(rule['capture_delay'])
        md = str(tx_ctx['capture_delay'])
        
        if rd == md:
            pass # Match
        elif rd == 'immediate':
            if md != 'immediate': return False
        elif rd == 'manual':
            if md != 'manual': return False
        elif rd.startswith('>'):
            try:
                limit = float(rd[1:])
                val = float(md)
                if not (val > limit): return False
            except:
                return False
        elif rd.startswith('<'):
            try:
                limit = float(rd[1:])
                val = float(md)
                if not (val < limit): return False
            except:
                return False
        elif '-' in rd:
            try:
                parts = rd.split('-')
                low, high = float(parts[0]), float(parts[1])
                val = float(md)
                if not (low <= val <= high): return False
            except:
                return False
        else:
            return False

    # 5. Monthly Volume
    if rule['monthly_volume']:
        min_v, max_v = parse_volume_range(rule['monthly_volume'])
        if not (min_v <= tx_ctx['monthly_volume'] <= max_v):
            return False

    # 6. Monthly Fraud Level
    if rule['monthly_fraud_level']:
        min_f, max_f = parse_fraud_range(rule['monthly_fraud_level'])
        if not (min_f <= tx_ctx['fraud_rate'] <= max_f):
            return False

    # 7. Is Credit
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 8. ACI
    if rule['aci'] and tx_ctx['aci'] not in rule['aci']:
        return False

    # 9. Intracountry
    if rule['intracountry'] is not None:
        is_intra = (tx_ctx['issuing_country'] == tx_ctx['acquirer_country'])
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    return True

def calculate_fee(amount, rule):
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000)

# --- Main Execution ---

# Load Data
try:
    payments = pd.read_csv('/output/chunk6/data/context/payments.csv')
    merchant_data = pd.read_json('/output/chunk6/data/context/merchant_data.json')
    fees = pd.read_json('/output/chunk6/data/context/fees.json')
except Exception as e:
    print(f"Error loading files: {e}")
    exit()

# 1. Filter for Merchant and Year
target_merchant = 'Belles_cookbook_store'
df_belles = payments[(payments['merchant'] == target_merchant) & (payments['year'] == 2023)].copy()

if df_belles.empty:
    print("No transactions found for Belles_cookbook_store in 2023.")
    exit()

# 2. Get Merchant Metadata
try:
    m_info = merchant_data[merchant_data['merchant'] == target_merchant].iloc[0]
    mcc = int(m_info['merchant_category_code'])
    account_type = m_info['account_type']
    capture_delay = m_info['capture_delay']
except IndexError:
    print(f"Merchant {target_merchant} not found in merchant_data.json")
    exit()

# 3. Calculate Merchant Stats (Volume & Fraud)
total_volume = df_belles['eur_amount'].sum()
monthly_volume = total_volume / 12.0

total_tx = len(df_belles)
fraud_tx = df_belles['has_fraudulent_dispute'].sum()
fraud_rate = fraud_tx / total_tx if total_tx > 0 else 0.0

# 4. Identify Schemes
schemes = ['NexPay', 'GlobalCard', 'SwiftCharge', 'TransactPlus']
fees_list = fees.to_dict('records')

results = {}

# 5. Simulation Loop
for scheme in schemes:
    total_scheme_fee = 0.0
    
    for _, tx in df_belles.iterrows():
        # Build Context for this transaction
        ctx = {
            'target_scheme': scheme, # Simulate this scheme
            'account_type': account_type,
            'mcc': mcc,
            'capture_delay': capture_delay,
            'monthly_volume': monthly_volume,
            'fraud_rate': fraud_rate,
            'is_credit': bool(tx['is_credit']),
            'aci': tx['aci'],
            'issuing_country': tx['issuing_country'],
            'acquirer_country': tx['acquirer_country'],
            'eur_amount': float(tx['eur_amount'])
        }
        
        # Find matching rules
        matches = []
        for rule in fees_list:
            if match_fee_rule(ctx, rule):
                matches.append(rule)
        
        if not matches:
            continue
            
        # Select most specific rule
        # Heuristic: Count number of non-null constraints
        def specificity(r):
            score = 0
            if r['account_type']: score += 1
            if r['merchant_category_code']: score += 1
            if r['capture_delay']: score += 1
            if r['monthly_volume']: score += 1
            if r['monthly_fraud_level']: score += 1
            if r['is_credit'] is not None: score += 1
            if r['aci']: score += 1
            if r['intracountry'] is not None: score += 1
            return score

        best_rule = sorted(matches, key=specificity, reverse=True)[0]
        
        fee = calculate_fee(ctx['eur_amount'], best_rule)
        total_scheme_fee += fee

    results[scheme] = total_scheme_fee

# 6. Find Max
max_scheme = max(results, key=results.get)
print(max_scheme)

# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2757
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: sufficient
# Plan steps: 2
# Code length: 9342 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

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
        except:
            return 0.0
    return 0.0

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

def get_month_from_doy(doy):
    """Returns month (1-12) from day of year for 2023 (non-leap)."""
    # Days in months for 2023
    days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    cum_days = [0] + list(np.cumsum(days))
    
    for i in range(12):
        if doy <= cum_days[i+1]:
            return i + 1
    return 12

def match_fee_rule(ctx, rule):
    """
    Checks if a fee rule applies to a transaction context.
    ctx: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme
    if rule['card_scheme'] != ctx['card_scheme']:
        return False

    # 2. Account Type (Rule list must contain merchant's type, or be empty)
    if rule['account_type'] and ctx['account_type'] not in rule['account_type']:
        return False

    # 3. Merchant Category Code (Rule list must contain merchant's MCC, or be empty)
    if rule['merchant_category_code'] and ctx['mcc'] not in rule['merchant_category_code']:
        return False

    # 4. Capture Delay
    if rule['capture_delay']:
        rd = str(rule['capture_delay'])
        md = str(ctx['capture_delay'])
        
        if rd == md:
            pass # Exact match
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
                return False # md might be 'manual' or 'immediate' which fails float conversion
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
            # Fallback for exact string match if not caught above
            if rd != md: return False

    # 5. Monthly Volume
    if rule['monthly_volume']:
        min_v, max_v = parse_volume_range(rule['monthly_volume'])
        if not (min_v <= ctx['monthly_volume'] <= max_v):
            return False

    # 6. Monthly Fraud Level
    if rule['monthly_fraud_level']:
        min_f, max_f = parse_fraud_range(rule['monthly_fraud_level'])
        if not (min_f <= ctx['monthly_fraud_level'] <= max_f):
            return False

    # 7. Is Credit
    if rule['is_credit'] is not None:
        if rule['is_credit'] != ctx['is_credit']:
            return False

    # 8. ACI
    if rule['aci'] and ctx['aci'] not in rule['aci']:
        return False

    # 9. Intracountry
    if rule['intracountry'] is not None:
        is_intra = (ctx['issuing_country'] == ctx['acquirer_country'])
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    return True

def calculate_fee_amount(amount, rule):
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000)

# --- Main Execution ---

# 1. Load Data
try:
    payments = pd.read_csv('/output/chunk6/data/context/payments.csv')
    merchant_data = pd.read_json('/output/chunk6/data/context/merchant_data.json')
    fees = pd.read_json('/output/chunk6/data/context/fees.json')
except Exception as e:
    print(f"Error loading files: {e}")
    exit()

# 2. Filter for Merchant and Year
target_merchant = 'Belles_cookbook_store'
df_belles = payments[(payments['merchant'] == target_merchant) & (payments['year'] == 2023)].copy()

if df_belles.empty:
    print("No transactions found for Belles_cookbook_store in 2023.")
    exit()

# 3. Get Merchant Metadata
try:
    m_info = merchant_data[merchant_data['merchant'] == target_merchant].iloc[0]
    mcc = int(m_info['merchant_category_code'])
    account_type = m_info['account_type']
    capture_delay = str(m_info['capture_delay'])
except IndexError:
    print(f"Merchant {target_merchant} not found in merchant_data.json")
    exit()

# 4. Calculate Monthly Stats (Volume & Fraud)
# Add month column
df_belles['month'] = df_belles['day_of_year'].apply(get_month_from_doy)

monthly_stats = {}
for month in range(1, 13):
    month_txs = df_belles[df_belles['month'] == month]
    if month_txs.empty:
        monthly_stats[month] = {'vol': 0.0, 'fraud_rate': 0.0}
        continue
    
    vol = month_txs['eur_amount'].sum()
    # Fraud defined as ratio of fraudulent volume over total volume (Manual Sec 7)
    fraud_vol = month_txs[month_txs['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    fraud_rate = fraud_vol / vol if vol > 0 else 0.0
    
    monthly_stats[month] = {'vol': vol, 'fraud_rate': fraud_rate}

# 5. Simulation Loop
schemes = ['NexPay', 'GlobalCard', 'SwiftCharge', 'TransactPlus']
fees_list = fees.to_dict('records')

results = {}

for scheme in schemes:
    total_scheme_fee = 0.0
    
    for _, tx in df_belles.iterrows():
        month = tx['month']
        stats = monthly_stats[month]
        
        # Build Context for this transaction
        ctx = {
            'card_scheme': scheme, # Simulate this scheme
            'account_type': account_type,
            'mcc': mcc,
            'capture_delay': capture_delay,
            'monthly_volume': stats['vol'],
            'monthly_fraud_level': stats['fraud_rate'],
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
            # If no rule matches, assume 0 fee or handle error. 
            # In this dataset, there should usually be a match.
            continue
            
        # Select most specific rule
        # Specificity score: count of non-null/non-empty constraint fields
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

        # Sort by specificity (descending), then ID (descending) to break ties deterministically if needed
        best_rule = sorted(matches, key=lambda x: (specificity(x), x['ID']), reverse=True)[0]
        
        fee = calculate_fee_amount(ctx['eur_amount'], best_rule)
        total_scheme_fee += fee

    results[scheme] = total_scheme_fee

# 6. Find Max
# print(results) # Debugging
max_scheme = max(results, key=results.get)
print(max_scheme)

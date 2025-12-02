# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1749
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: sufficient
# Plan steps: 2
# Code length: 8208 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np
import os

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value)

def parse_range(range_str):
    """Parses strings like '100k-1m', '<3', '>5', '7.7%-8.3%' into (min, max)."""
    if not range_str or not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip().lower()
    
    # Helper to parse k/m suffixes
    def parse_val(v):
        v = v.replace('%', '') 
        mult = 1
        if 'k' in v:
            mult = 1000
            v = v.replace('k', '')
        elif 'm' in v:
            mult = 1000000
            v = v.replace('m', '')
        return float(v) * mult

    try:
        if '-' in s:
            parts = s.split('-')
            # Check if it's a percentage range
            is_pct = '%' in range_str
            v1 = parse_val(parts[0])
            v2 = parse_val(parts[1])
            if is_pct:
                v1 /= 100
                v2 /= 100
            return v1, v2
        elif s.startswith('<'):
            val = parse_val(s[1:])
            if '%' in range_str: val /= 100
            return float('-inf'), val
        elif s.startswith('>'):
            val = parse_val(s[1:])
            if '%' in range_str: val /= 100
            return val, float('inf')
        elif s == 'immediate': 
            return 0.0, 0.0 
        elif s == 'manual': 
            return 999.0, 999.0
        else:
            val = parse_val(s)
            if '%' in range_str: val /= 100
            return val, val
    except:
        return None, None

def map_delay_to_float(val):
    """Maps merchant capture_delay values to numeric for comparison."""
    s = str(val).lower()
    if s == 'immediate': return 0.0
    if s == 'manual': return 999.0
    try:
        return float(s)
    except:
        return -1.0 

def is_match(value, rule_value, match_type='exact'):
    """
    Generic matcher for fee rules.
    match_type: 'exact', 'in_list', 'range_vol', 'range_fraud', 'bool_int', 'delay'
    """
    # Wildcard check (None or empty list matches all)
    if rule_value is None or rule_value == [] or rule_value == "None":
        return True
        
    if match_type == 'exact':
        return str(value).lower() == str(rule_value).lower()
        
    elif match_type == 'in_list':
        if not isinstance(rule_value, list):
            return str(value) == str(rule_value)
        # Check if value is in list (handling potential type mismatches)
        return value in rule_value
        
    elif match_type == 'range_vol':
        min_v, max_v = parse_range(rule_value)
        if min_v is None: return True
        return min_v <= value <= max_v
        
    elif match_type == 'range_fraud':
        min_v, max_v = parse_range(rule_value)
        if min_v is None: return True
        # value is already a float ratio (e.g. 0.08), range parsed to float ratio
        return min_v <= value <= max_v

    elif match_type == 'bool_int':
        # rule_value is 0.0 or 1.0, value is bool
        expected = (rule_value == 1.0)
        return value == expected
    
    elif match_type == 'delay':
        # value is float (mapped from merchant), rule_value is string/range
        min_v, max_v = parse_range(rule_value)
        if min_v is None: return True
        # Special handling: if rule is 'immediate' (0,0) and val is 0 -> match
        # If rule is '<3' (-inf, 3) and val is 0 -> match
        return min_v <= value <= max_v
        
    return False

# ==========================================
# MAIN SCRIPT
# ==========================================

# 1. Load Data
payments_path = '/output/chunk2/data/context/payments.csv'
merchant_name = 'Martinis_Fine_Steakhouse'
target_year = 2023

# Load Transactions
df_all = pd.read_csv(payments_path)
df = df_all[(df_all['merchant'] == merchant_name) & (df_all['year'] == target_year)].copy()

# Load Context Files
with open('/output/chunk2/data/context/merchant_data.json', 'r') as f:
    merchant_data = json.load(f)

with open('/output/chunk2/data/context/fees.json', 'r') as f:
    fees_data = json.load(f)

# 2. Get Merchant Attributes
merchant_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
if not merchant_info:
    raise ValueError(f"Merchant {merchant_name} not found")

m_account_type = merchant_info.get('account_type')
m_mcc = merchant_info.get('merchant_category_code')
m_capture_delay_str = merchant_info.get('capture_delay')
m_capture_delay_val = map_delay_to_float(m_capture_delay_str)

# 3. Calculate Monthly Stats
# Map day_of_year to month
df['month'] = pd.to_datetime(df['day_of_year'] - 1, unit='D', origin=f'{target_year}-01-01').dt.month

monthly_stats = {}
for month in range(1, 13):
    month_txs = df[df['month'] == month]
    if len(month_txs) == 0:
        monthly_stats[month] = {'vol': 0.0, 'fraud_rate': 0.0}
        continue
        
    total_vol = month_txs['eur_amount'].sum()
    # Fraud volume: sum of amounts where has_fraudulent_dispute is True
    fraud_vol = month_txs[month_txs['has_fraudulent_dispute']]['eur_amount'].sum()
    
    # Fraud Rate = Fraud Volume / Total Volume
    fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    monthly_stats[month] = {
        'vol': total_vol,
        'fraud_rate': fraud_rate
    }

# 4. Calculate Fees
total_fees = 0.0
matched_count = 0

for idx, tx in df.iterrows():
    # Transaction attributes
    tx_scheme = tx['card_scheme']
    tx_credit = tx['is_credit'] # bool
    tx_aci = tx['aci']
    tx_amount = tx['eur_amount']
    tx_month = tx['month']
    
    # Intracountry check
    is_intra = (tx['issuing_country'] == tx['acquirer_country'])
    
    # Monthly stats
    m_stats = monthly_stats.get(tx_month, {'vol': 0.0, 'fraud_rate': 0.0})
    curr_vol = m_stats['vol']
    curr_fraud = m_stats['fraud_rate']
    
    # Find first matching rule
    matched_rule = None
    for rule in fees_data:
        # 1. Card Scheme (Exact)
        if not is_match(tx_scheme, rule['card_scheme'], 'exact'): continue
        
        # 2. Account Type (List contains Merchant Type)
        if not is_match(m_account_type, rule['account_type'], 'in_list'): continue
            
        # 3. MCC (List contains Merchant MCC)
        if not is_match(m_mcc, rule['merchant_category_code'], 'in_list'): continue
            
        # 4. Is Credit (Exact Bool)
        if rule['is_credit'] is not None and rule['is_credit'] != tx_credit: continue
            
        # 5. ACI (List contains Tx ACI)
        if not is_match(tx_aci, rule['aci'], 'in_list'): continue
            
        # 6. Capture Delay (Range/Value match against Merchant Delay)
        if not is_match(m_capture_delay_val, rule['capture_delay'], 'delay'): continue
            
        # 7. Monthly Volume (Range match against Monthly Vol)
        if not is_match(curr_vol, rule['monthly_volume'], 'range_vol'): continue
            
        # 8. Monthly Fraud Level (Range match against Monthly Fraud Rate)
        if not is_match(curr_fraud, rule['monthly_fraud_level'], 'range_fraud'): continue
            
        # 9. Intracountry (Bool/Int match against Tx Intra)
        if not is_match(is_intra, rule['intracountry'], 'bool_int'): continue
            
        # Match found!
        matched_rule = rule
        break
    
    if matched_rule:
        fixed = matched_rule['fixed_amount']
        rate = matched_rule['rate']
        # Fee formula: fixed + rate * amount / 10000
        fee = fixed + (rate * tx_amount / 10000)
        total_fees += fee
        matched_count += 1

# Output the final result
print(f"{total_fees:.2f}")

# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1749
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8886 characters (FULL CODE)
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

def parse_range(range_str):
    """Parses strings like '100k-1m', '<3', '>5', '7.7%-8.3%' into (min, max)."""
    if not range_str or not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip().lower()
    
    # Helper to parse k/m suffixes
    def parse_val(v):
        v = v.replace('%', '') # keep as number, caller handles scale if needed
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
            return parse_val(parts[0]), parse_val(parts[1])
        elif s.startswith('<'):
            return float('-inf'), parse_val(s[1:])
        elif s.startswith('>'):
            return parse_val(s[1:]), float('inf')
        elif s == 'immediate': # Special case for capture_delay
            return 0.0, 0.0 
        elif s == 'manual': # Special case
            return 999.0, 999.0
        else:
            val = parse_val(s)
            return val, val
    except:
        return None, None

def map_delay(val):
    """Maps merchant capture_delay values to numeric for comparison."""
    s = str(val).lower()
    if s == 'immediate': return 0.0
    if s == 'manual': return 999.0
    try:
        return float(s)
    except:
        return -1.0 # Unknown

def is_match(value, rule_value, match_type='exact'):
    """
    Generic matcher for fee rules.
    match_type: 'exact', 'in_list', 'range_vol', 'range_fraud', 'bool_int'
    """
    # Wildcard check (None or empty list matches all)
    if rule_value is None or rule_value == [] or rule_value == "None":
        return True
        
    if match_type == 'exact':
        return str(value).lower() == str(rule_value).lower()
        
    elif match_type == 'in_list':
        # rule_value is list, value is item
        if not isinstance(rule_value, list):
            return str(value) == str(rule_value)
        return value in rule_value
        
    elif match_type == 'range_vol':
        # rule_value is string range (e.g. '100k-1m'), value is float
        min_v, max_v = parse_range(rule_value)
        if min_v is None: return True
        return min_v <= value <= max_v
        
    elif match_type == 'range_fraud':
        # rule_value is string range (e.g. '0%-0.5%'), value is float (0.004)
        is_percent_rule = '%' in str(rule_value)
        min_v, max_v = parse_range(rule_value)
        
        comp_value = value
        if is_percent_rule:
            comp_value = value * 100 # Convert 0.005 to 0.5 to match rule scale
            
        if min_v is None: return True
        return min_v <= comp_value <= max_v

    elif match_type == 'bool_int':
        # rule_value is 0.0 or 1.0, value is bool
        # 1.0 == True, 0.0 == False
        expected = (rule_value == 1.0)
        return value == expected
        
    return False

# ==========================================
# MAIN SCRIPT
# ==========================================

# 1. Load Data
print("Loading data...")
payments_path = '/output/chunk2/data/context/payments.csv'
pkl_path = 'filtered_transactions.pkl'
merchant_name = 'Martinis_Fine_Steakhouse'
target_year = 2023

# Load Transactions
if os.path.exists(pkl_path):
    df = pd.read_pickle(pkl_path)
    print(f"Loaded {len(df)} transactions from pickle.")
else:
    print("Pickle not found, loading from CSV...")
    df_all = pd.read_csv(payments_path)
    df = df_all[(df_all['merchant'] == merchant_name) & (df_all['year'] == target_year)].copy()
    print(f"Loaded {len(df)} transactions from CSV.")

# Load Context Files
with open('/output/chunk2/data/context/merchant_data.json', 'r') as f:
    merchant_data = json.load(f)

with open('/output/chunk2/data/context/fees.json', 'r') as f:
    fees_data = json.load(f)

# 2. Get Merchant Attributes
merchant_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)

if not merchant_info:
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

m_account_type = merchant_info.get('account_type')
m_mcc = merchant_info.get('merchant_category_code')
m_capture_delay_str = merchant_info.get('capture_delay')
m_capture_delay_val = map_delay(m_capture_delay_str)

print(f"Merchant Profile: Type={m_account_type}, MCC={m_mcc}, Delay={m_capture_delay_str} ({m_capture_delay_val})")

# 3. Calculate Monthly Stats
# Map day_of_year to month (2023)
df['month'] = pd.to_datetime(df['day_of_year'] - 1, unit='D', origin=f'{target_year}-01-01').dt.month

# Calculate Monthly Volume and Fraud Rate
monthly_stats = {}
for month in range(1, 13):
    month_txs = df[df['month'] == month]
    if len(month_txs) == 0:
        monthly_stats[month] = {'vol': 0.0, 'fraud_rate': 0.0}
        continue
        
    total_vol = month_txs['eur_amount'].sum()
    fraud_vol = month_txs[month_txs['has_fraudulent_dispute']]['eur_amount'].sum()
    
    # Fraud Rate = Fraud Volume / Total Volume (as per manual)
    fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    monthly_stats[month] = {
        'vol': total_vol,
        'fraud_rate': fraud_rate
    }

# 4. Calculate Fees
total_fees = 0.0
transaction_count = 0
matched_count = 0

print("Calculating fees per transaction...")

for idx, tx in df.iterrows():
    # Transaction attributes
    tx_scheme = tx['card_scheme']
    tx_credit = tx['is_credit'] # bool
    tx_aci = tx['aci']
    tx_amount = tx['eur_amount']
    tx_month = tx['month']
    
    # Intracountry check (issuing == acquirer)
    is_intra = (tx['issuing_country'] == tx['acquirer_country'])
    
    # Monthly stats for this transaction
    m_stats = monthly_stats.get(tx_month, {'vol': 0.0, 'fraud_rate': 0.0})
    curr_vol = m_stats['vol']
    curr_fraud = m_stats['fraud_rate']
    
    # Find first matching rule
    matched_rule = None
    for rule in fees_data:
        # 1. Card Scheme (Exact)
        if not is_match(tx_scheme, rule['card_scheme'], 'exact'): continue
        
        # 2. Account Type (List contains Merchant Type)
        if rule['account_type'] and m_account_type not in rule['account_type']: continue
            
        # 3. MCC (List contains Merchant MCC)
        if rule['merchant_category_code'] and m_mcc not in rule['merchant_category_code']: continue
            
        # 4. Is Credit (Exact Bool)
        if rule['is_credit'] is not None and rule['is_credit'] != tx_credit: continue
            
        # 5. ACI (List contains Tx ACI)
        if rule['aci'] and tx_aci not in rule['aci']: continue
            
        # 6. Capture Delay (Range/Value match)
        # Use range_vol logic since we mapped delay to float
        if not is_match(m_capture_delay_val, rule['capture_delay'], 'range_vol'): continue
            
        # 7. Monthly Volume (Range)
        if not is_match(curr_vol, rule['monthly_volume'], 'range_vol'): continue
            
        # 8. Monthly Fraud Level (Range)
        if not is_match(curr_fraud, rule['monthly_fraud_level'], 'range_fraud'): continue
            
        # 9. Intracountry (Bool/Int)
        if rule['intracountry'] is not None:
            rule_intra = (rule['intracountry'] == 1.0)
            if rule_intra != is_intra: continue
            
        # Match found!
        matched_rule = rule
        break
    
    if matched_rule:
        # Calculate fee: fixed + rate * amount / 10000
        fixed = matched_rule['fixed_amount']
        rate = matched_rule['rate']
        fee = fixed + (rate * tx_amount / 10000)
        total_fees += fee
        matched_count += 1
    else:
        # If no rule matches, we assume 0 or log it. 
        # In a real scenario, this would be an error.
        pass
    
    transaction_count += 1

print(f"Processed {transaction_count} transactions.")
print(f"Matched {matched_count} transactions to fee rules.")
print(f"Total Fees: {total_fees:.2f}")

# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2484
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: sufficient
# Plan steps: 2
# Code length: 7498 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np
import datetime

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

def parse_fraud_range(range_str):
    """
    Parses a fraud level range string (e.g., '7.7%-8.3%', '>5%', '<3%') 
    into a tuple (min_val, max_val).
    """
    if not range_str or not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip().replace('%', '')
    
    if '-' in s:
        try:
            parts = s.split('-')
            # Convert percentage string to ratio (e.g. 7.7 -> 0.077)
            return float(parts[0])/100, float(parts[1])/100
        except:
            return None, None
    elif s.startswith('>'):
        try:
            val = float(s[1:]) / 100
            return val, float('inf')
        except:
            return None, None
    elif s.startswith('<'):
        try:
            val = float(s[1:]) / 100
            return float('-inf'), val
        except:
            return None, None
    return None, None

def is_in_range(value, range_str):
    """Checks if a float value falls within a parsed range string."""
    if range_str is None:
        return True # Wildcard matches all
    
    low, high = parse_fraud_range(range_str)
    if low is not None and high is not None:
        # Use a small epsilon for float comparison if needed, but direct comparison is usually fine for these ranges
        return low <= value <= high
    return False

def match_fee_rule(tx_context, rule):
    """
    Matches a transaction context against a fee rule.
    tx_context must contain: 
      - card_scheme, aci, is_credit (from tx)
      - account_type, merchant_category_code (from merchant data)
      - monthly_fraud_rate (calculated stat)
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (Rule has list, Tx has single)
    # If rule['account_type'] is empty list or None, it applies to all
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. MCC (Rule has list, Tx has single)
    # If rule['merchant_category_code'] is empty list or None, it applies to all
    if rule.get('merchant_category_code'):
        if tx_context['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Exact match, handle boolean/null)
    # If rule['is_credit'] is None, it applies to both
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False
            
    # 5. ACI (Rule has list, Tx has single)
    # If rule['aci'] is empty list or None, it applies to all
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False
            
    # 6. Monthly Fraud Level (Rule has range string, Tx has float)
    if rule.get('monthly_fraud_level'):
        if not is_in_range(tx_context['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False
            
    return True

# ---------------------------------------------------------
# MAIN ANALYSIS
# ---------------------------------------------------------

# File Paths
fees_path = '/output/chunk2/data/context/fees.json'
payments_path = '/output/chunk2/data/context/payments.csv'
merchant_path = '/output/chunk2/data/context/merchant_data.json'

# 1. Load Data
try:
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)
    df = pd.read_csv(payments_path)
except Exception as e:
    print(f"Error loading data: {e}")
    exit()

# 2. Extract Target Fee Rule (ID=398)
target_rule = next((r for r in fees_data if r['ID'] == 398), None)
if not target_rule:
    print("Error: Fee rule ID=398 not found.")
    exit()

original_rate = target_rule['rate']
new_rate = 99

# 3. Get Merchant Metadata
merchant_name = 'Crossfit_Hanna'
merchant_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
if not merchant_info:
    print(f"Error: Merchant {merchant_name} not found.")
    exit()

# 4. Filter Transactions (Merchant + Year 2023)
df_mh = df[(df['merchant'] == merchant_name) & (df['year'] == 2023)].copy()

# 5. Calculate Monthly Fraud Rates
# Convert day_of_year to Month (1-12)
# 2023 is not a leap year
df_mh['month'] = pd.to_datetime(df_mh['year'] * 1000 + df_mh['day_of_year'], format='%Y%j').dt.month

monthly_stats = {}
for month in range(1, 13):
    month_txs = df_mh[df_mh['month'] == month]
    if len(month_txs) == 0:
        monthly_stats[month] = {'fraud_rate': 0.0, 'volume': 0.0}
        continue
    
    total_vol = month_txs['eur_amount'].sum()
    fraud_vol = month_txs[month_txs['has_fraudulent_dispute']]['eur_amount'].sum()
    
    # Fraud Rate = Fraud Volume / Total Volume
    fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    monthly_stats[month] = {
        'fraud_rate': fraud_rate,
        'volume': total_vol
    }

# 6. Identify Matching Transactions and Calculate Delta
matching_amount_sum = 0.0
matching_count = 0

# Optimization: Pre-filter by static fields if possible to speed up iteration
# Rule 398 static fields: card_scheme, is_credit
static_scheme = target_rule.get('card_scheme')
static_credit = target_rule.get('is_credit')

# Iterate
for idx, row in df_mh.iterrows():
    # Quick check on static fields to avoid full context build if not needed
    if static_scheme and row['card_scheme'] != static_scheme:
        continue
    if static_credit is not None and row['is_credit'] != static_credit:
        continue

    month = row['month']
    current_fraud_rate = monthly_stats[month]['fraud_rate']
    
    # Construct context for matching
    tx_context = {
        'card_scheme': row['card_scheme'],
        'account_type': merchant_info['account_type'],
        'merchant_category_code': merchant_info['merchant_category_code'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'monthly_fraud_rate': current_fraud_rate
    }
    
    # Check if transaction matches Rule 398
    if match_fee_rule(tx_context, target_rule):
        matching_amount_sum += row['eur_amount']
        matching_count += 1

# 7. Calculate Final Delta
# Formula: Delta = (New Rate - Old Rate) * Amount / 10000
# Note: Rate is usually in basis points or similar, formula given in manual:
# fee = fixed_amount + rate * transaction_value / 10000
# Delta = (fixed + new_rate * val / 10000) - (fixed + old_rate * val / 10000)
# Delta = (new_rate - old_rate) * val / 10000

delta = (new_rate - original_rate) * matching_amount_sum / 10000

print(f"{delta:.14f}")

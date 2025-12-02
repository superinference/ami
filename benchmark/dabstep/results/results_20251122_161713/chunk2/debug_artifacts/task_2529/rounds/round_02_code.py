# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2529
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8128 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- HELPER FUNCTIONS ---
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

def parse_range(range_str):
    """Parses strings like '100k-1m', '<3', '>5', '7.7%-8.3%' into (min, max)."""
    if range_str is None:
        return (-float('inf'), float('inf'))
    
    s = str(range_str).lower().strip()
    
    # Handle percentages
    is_pct = '%' in s
    if is_pct:
        s = s.replace('%', '')
        scale = 0.01
    else:
        scale = 1.0
        
    # Handle k/m suffixes for volume
    def parse_val(v):
        v = v.strip()
        mult = 1
        if 'k' in v:
            mult = 1000
            v = v.replace('k', '')
        elif 'm' in v:
            mult = 1000000
            v = v.replace('m', '')
        return float(v) * mult * scale

    if '-' in s:
        parts = s.split('-')
        try:
            return (parse_val(parts[0]), parse_val(parts[1]))
        except:
            return (-float('inf'), float('inf'))
    elif s.startswith('<'):
        try:
            return (-float('inf'), parse_val(s[1:]))
        except:
            return (-float('inf'), float('inf'))
    elif s.startswith('>'):
        try:
            return (parse_val(s[1:]), float('inf'))
        except:
            return (-float('inf'), float('inf'))
    else:
        # Exact value or unparseable
        try:
            val = parse_val(s)
            return (val, val)
        except:
            return (-float('inf'), float('inf'))

def match_fee_rule(tx_context, rule):
    """
    Checks if a fee rule applies to a transaction context.
    tx_context must contain: card_scheme, account_type, mcc, is_credit, aci, 
    capture_delay, intracountry, monthly_volume, monthly_fraud_rate
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (Rule has list, Merchant has string)
    # Wildcard: [] or None matches all
    if rule['account_type'] and tx_context['account_type'] not in rule['account_type']:
        return False
        
    # 3. MCC (Rule has list, Merchant has int)
    # Wildcard: [] or None matches all
    if rule['merchant_category_code'] and tx_context['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 4. Is Credit (Exact match or None)
    if rule['is_credit'] is not None and rule['is_credit'] != tx_context['is_credit']:
        return False
        
    # 5. ACI (Rule has list, Transaction has string)
    # Wildcard: [] or None matches all
    if rule['aci'] and tx_context['aci'] not in rule['aci']:
        return False
        
    # 6. Capture Delay
    if rule['capture_delay'] is not None:
        rd = str(rule['capture_delay'])
        md = str(tx_context['capture_delay'])
        
        # Try numeric comparison first (e.g. merchant "1" vs rule "<3")
        try:
            md_num = float(md)
            min_v, max_v = parse_range(rd)
            if not (min_v <= md_num <= max_v):
                return False
        except ValueError:
            # Fallback to string comparison if not numeric (e.g. "manual")
            if rd != md:
                return False

    # 7. Intracountry (Rule is 0.0/1.0/None, Context is Bool)
    if rule['intracountry'] is not None:
        rule_intra = bool(float(rule['intracountry']))
        if rule_intra != tx_context['intracountry']:
            return False

    # 8. Monthly Volume (Range check)
    if rule['monthly_volume']:
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= tx_context['monthly_volume'] <= max_v):
            return False

    # 9. Monthly Fraud Level (Range check)
    if rule['monthly_fraud_level']:
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        if not (min_f <= tx_context['monthly_fraud_rate'] <= max_f):
            return False
            
    return True

def calculate_fee(amount, rule):
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000)

def get_month(day_of_year):
    """Maps day_of_year (1-365) to month (1-12) for non-leap year."""
    days_in_months = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    cum_days = [0] + list(np.cumsum(days_in_months))
    for i in range(12):
        if day_of_year <= cum_days[i+1]:
            return i + 1
    return 12

# --- MAIN SCRIPT ---

# 1. Load Data
payments_path = '/output/chunk2/data/context/payments.csv'
fees_path = '/output/chunk2/data/context/fees.json'
merchant_path = '/output/chunk2/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Merchant and Year
merchant_name = 'Belles_cookbook_store'
target_year = 2023
df_belles = df_payments[
    (df_payments['merchant'] == merchant_name) & 
    (df_payments['year'] == target_year)
].copy()

# 3. Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
if not merchant_info:
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

original_mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']
new_mcc = 5411

# 4. Calculate Monthly Stats (Volume & Fraud)
# Add month column
df_belles['month'] = df_belles['day_of_year'].apply(get_month)

monthly_stats = {}
for month in range(1, 13):
    m_df = df_belles[df_belles['month'] == month]
    if len(m_df) == 0:
        monthly_stats[month] = {'vol': 0.0, 'fraud_rate': 0.0}
        continue
    
    total_vol = m_df['eur_amount'].sum()
    # Fraud rate = Fraud Volume / Total Volume (as per manual)
    fraud_vol = m_df[m_df['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    monthly_stats[month] = {'vol': total_vol, 'fraud_rate': fraud_rate}

# 5. Calculate Fees (Old vs New)
total_fee_old = 0.0
total_fee_new = 0.0

# Iterate through every transaction
for _, tx in df_belles.iterrows():
    month = tx['month']
    stats = monthly_stats[month]
    
    # Determine Intracountry (Issuer == Acquirer)
    is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
    
    # Base Context
    context = {
        'card_scheme': tx['card_scheme'],
        'account_type': account_type,
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'capture_delay': capture_delay,
        'intracountry': is_intracountry,
        'monthly_volume': stats['vol'],
        'monthly_fraud_rate': stats['fraud_rate'],
        'eur_amount': tx['eur_amount']
    }
    
    # --- Calculate Old Fee ---
    context['mcc'] = original_mcc
    fee_old = 0.0
    # Find first matching rule
    for rule in fees_data:
        if match_fee_rule(context, rule):
            fee_old = calculate_fee(context['eur_amount'], rule)
            break
    total_fee_old += fee_old
    
    # --- Calculate New Fee ---
    context['mcc'] = new_mcc
    fee_new = 0.0
    # Find first matching rule
    for rule in fees_data:
        if match_fee_rule(context, rule):
            fee_new = calculate_fee(context['eur_amount'], rule)
            break
    total_fee_new += fee_new

# 6. Calculate Delta
delta = total_fee_new - total_fee_old

# Print result with high precision
print(f"{delta:.14f}")

# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2532
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 8198 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except ValueError:
                return 0.0
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
    """Parses strings like '100k-1m', '<3', '>5', '8.3%' into min/max tuple."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    def parse_val(val_s):
        val_s = val_s.strip()
        mult = 1
        if val_s.endswith('%'):
            val_s = val_s[:-1]
            mult = 0.01
        elif val_s.endswith('k'):
            val_s = val_s[:-1]
            mult = 1000
        elif val_s.endswith('m'):
            val_s = val_s[:-1]
            mult = 1000000
        return float(val_s) * mult

    if '-' in s:
        parts = s.split('-')
        try:
            return parse_val(parts[0]), parse_val(parts[1])
        except:
            return None, None
    elif s.startswith('>'):
        try:
            return parse_val(s[1:]), float('inf')
        except:
            return None, None
    elif s.startswith('<'):
        try:
            return float('-inf'), parse_val(s[1:])
        except:
            return None, None
    else:
        try:
            v = parse_val(s)
            return v, v
        except:
            return None, None

def check_capture_delay(tx_delay, rule_delay):
    """Matches merchant capture delay (str) against rule delay (str)."""
    if rule_delay is None:
        return True
    
    t = str(tx_delay).lower()
    r = str(rule_delay).lower()
    
    if r == t:
        return True
    
    if any(x in r for x in ['<', '>', '-']):
        t_val = None
        if t == 'immediate':
            t_val = 0.0
        elif t == 'manual':
            t_val = 999.0
        else:
            try:
                t_val = float(t)
            except ValueError:
                return False
            
        min_v, max_v = parse_range(r)
        if min_v is None: return False
        
        return min_v <= t_val <= max_v
            
    return False

def match_fee_rule(tx_ctx, rule):
    """Checks if a fee rule applies to a transaction context."""
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (Rule has list, tx has string)
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (Rule has list, tx has int)
    if rule.get('merchant_category_code'):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. ACI (Rule has list, tx has string)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 5. Is Credit (Rule has bool or None)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 6. Intracountry (Rule has bool or None)
    if rule.get('intracountry') is not None:
        r_intra = rule['intracountry']
        # Handle 0.0/1.0 as booleans
        if r_intra == 1.0: r_intra = True
        elif r_intra == 0.0: r_intra = False
        
        if r_intra != tx_ctx['intracountry']:
            return False
            
    # 7. Capture Delay
    if rule.get('capture_delay'):
        if not check_capture_delay(tx_ctx['capture_delay'], rule['capture_delay']):
            return False
            
    # 8. Monthly Volume
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        vol = tx_ctx['monthly_volume']
        if min_v is not None and not (min_v <= vol <= max_v):
            return False
            
    # 9. Monthly Fraud Level
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        fraud = tx_ctx['monthly_fraud_level']
        if min_f is not None and not (min_f <= fraud <= max_f):
            return False
            
    return True

def calculate_fee(amount, rule):
    """Calculates fee amount based on rule."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    return fixed + (rate * amount / 10000.0)

# ==========================================
# MAIN LOGIC
# ==========================================

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_path = '/output/chunk6/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Merchant and Year
target_merchant = 'Belles_cookbook_store'
target_year = 2023

df = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
].copy()

# 3. Get Merchant Metadata
merchant_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found")

account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']
original_mcc = merchant_info['merchant_category_code']
new_mcc = 7523

# 4. Pre-calculate Monthly Stats
# Map day_of_year to month (1-12)
bins = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 366]
labels = range(1, 13)
df['month'] = pd.cut(df['day_of_year'], bins=bins, labels=labels).astype(int)

monthly_stats = {}
for month in range(1, 13):
    month_df = df[df['month'] == month]
    if month_df.empty:
        monthly_stats[month] = {'vol': 0.0, 'fraud_rate': 0.0}
        continue
    
    total_vol = month_df['eur_amount'].sum()
    
    # Fraud volume (sum of amounts where has_fraudulent_dispute is True)
    fraud_vol = month_df[month_df['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    
    # Fraud rate = fraud_vol / total_vol
    fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    monthly_stats[month] = {
        'vol': total_vol,
        'fraud_rate': fraud_rate
    }

# 5. Calculate Fees
total_fee_original = 0.0
total_fee_new = 0.0

for row in df.itertuples():
    month = row.month
    stats = monthly_stats[month]
    
    # Intracountry check: Issuer == Acquirer
    is_intracountry = (row.issuing_country == row.acquirer_country)
    
    tx_ctx = {
        'card_scheme': row.card_scheme,
        'account_type': account_type,
        'capture_delay': capture_delay,
        'is_credit': row.is_credit,
        'aci': row.aci,
        'intracountry': is_intracountry,
        'monthly_volume': stats['vol'],
        'monthly_fraud_level': stats['fraud_rate'],
        # MCC will be set below
    }
    
    # --- Original Fee ---
    tx_ctx['mcc'] = original_mcc
    for rule in fees_data:
        if match_fee_rule(tx_ctx, rule):
            total_fee_original += calculate_fee(row.eur_amount, rule)
            break 
    
    # --- New Fee ---
    tx_ctx['mcc'] = new_mcc
    for rule in fees_data:
        if match_fee_rule(tx_ctx, rule):
            total_fee_new += calculate_fee(row.eur_amount, rule)
            break 

# 6. Calculate Delta
delta = total_fee_new - total_fee_original

# 7. Output
print(f"{delta:.14f}")

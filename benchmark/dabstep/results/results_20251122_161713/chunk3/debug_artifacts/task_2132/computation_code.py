import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        v = v.replace('>', '').replace('<', '').replace('=', '') # Strip operators for simple parsing
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except:
                return 0.0
        if 'k' in v:
            try:
                return float(v.replace('k', '')) * 1000
            except:
                return 0.0
        if 'm' in v:
            try:
                return float(v.replace('m', '')) * 1000000
            except:
                return 0.0
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range_check(value, rule_str):
    """
    Check if a numeric value fits a rule string (e.g., '>5', '100k-1m', '8.3%').
    """
    if rule_str is None:
        return True
    
    s = str(rule_str).strip().lower()
    
    try:
        if '-' in s:
            parts = s.split('-')
            low = coerce_to_float(parts[0])
            high = coerce_to_float(parts[1])
            return low <= value <= high
        
        if s.startswith('>='):
            limit = coerce_to_float(s[2:])
            return value >= limit
        if s.startswith('>'):
            limit = coerce_to_float(s[1:])
            return value > limit
        if s.startswith('<='):
            limit = coerce_to_float(s[2:])
            return value <= limit
        if s.startswith('<'):
            limit = coerce_to_float(s[1:])
            return value < limit
            
        # Exact match (numeric)
        return value == coerce_to_float(s)
    except:
        return False

def check_capture_delay(merchant_delay, rule_delay):
    """
    Check if merchant capture delay matches rule.
    """
    if rule_delay is None:
        return True
    
    m_val = str(merchant_delay).lower().strip()
    r_val = str(rule_delay).lower().strip()
    
    if m_val == r_val:
        return True
    
    # If rule is numeric range/comparison
    if any(c in r_val for c in ['<', '>', '-']):
        try:
            # 'immediate' is effectively 0 days
            if m_val == 'immediate':
                m_num = 0.0
            elif m_val == 'manual':
                # Manual is not numeric, so it fails numeric range checks unless specified
                return False 
            else:
                m_num = float(m_val)
            
            return parse_range_check(m_num, r_val)
        except ValueError:
            return False
            
    return False

def match_fee_rule(tx_ctx, rule):
    """
    Check if a transaction context matches a fee rule.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List match or Wildcard)
    if rule.get('account_type'): # If not empty/None
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match or Wildcard)
    if rule.get('merchant_category_code'):
        if tx_ctx['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Exact match or Wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 5. ACI (List match or Wildcard)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Exact match or Wildcard)
    if rule.get('intracountry') is not None:
        # In fees.json, intracountry is 0.0 (False) or 1.0 (True)
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['intracountry']:
            return False
            
    # 7. Capture Delay (Complex match)
    if not check_capture_delay(tx_ctx['capture_delay'], rule.get('capture_delay')):
        return False
        
    # 8. Monthly Volume (Range match)
    if not parse_range_check(tx_ctx['monthly_volume'], rule.get('monthly_volume')):
        return False
        
    # 9. Monthly Fraud Level (Range match)
    if not parse_range_check(tx_ctx['monthly_fraud_level'], rule.get('monthly_fraud_level')):
        return False
        
    return True

# ---------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------

# File Paths
payments_path = '/output/chunk3/data/context/payments.csv'
fees_path = '/output/chunk3/data/context/fees.json'
merchant_path = '/output/chunk3/data/context/merchant_data.json'

# 1. Load Data
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Target Merchant and Timeframe
target_merchant = 'Golfclub_Baron_Friso'
# March 2023: Day 60 to 90 (inclusive)
df_march = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == 2023) &
    (df_payments['day_of_year'] >= 60) &
    (df_payments['day_of_year'] <= 90)
].copy()

# 3. Calculate Monthly Stats for Matching
# Volume: Sum of eur_amount
monthly_volume = df_march['eur_amount'].sum()

# Fraud Level: Ratio of fraud disputes to total transactions
tx_count = len(df_march)
fraud_count = df_march['has_fraudulent_dispute'].sum()
monthly_fraud_level = (fraud_count / tx_count) if tx_count > 0 else 0.0

# 4. Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

# 5. Get Target Fee Rule (ID=65)
target_rule_id = 65
target_rule = next((r for r in fees_data if r['ID'] == target_rule_id), None)

if not target_rule:
    print("0.00000000000000")
    exit()

old_rate = target_rule['rate']
new_rate = 99

# 6. Identify Affected Transactions and Calculate Delta
affected_volume = 0.0

# Pre-calculate static context parts to speed up loop
static_ctx = {
    'account_type': merchant_info['account_type'],
    'merchant_category_code': merchant_info['merchant_category_code'],
    'capture_delay': merchant_info['capture_delay'],
    'monthly_volume': monthly_volume,
    'monthly_fraud_level': monthly_fraud_level
}

for _, row in df_march.iterrows():
    # Build Context
    # Intracountry: Issuing == Acquirer
    is_intracountry = (row['issuing_country'] == row['acquirer_country'])
    
    tx_ctx = static_ctx.copy()
    tx_ctx.update({
        'card_scheme': row['card_scheme'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': is_intracountry
    })
    
    if match_fee_rule(tx_ctx, target_rule):
        affected_volume += row['eur_amount']

# 7. Calculate Delta
# Fee formula part affected: rate * amount / 10000
# Delta = (New Rate - Old Rate) * Volume / 10000
delta = (new_rate - old_rate) * affected_volume / 10000

print(f"{delta:.14f}")
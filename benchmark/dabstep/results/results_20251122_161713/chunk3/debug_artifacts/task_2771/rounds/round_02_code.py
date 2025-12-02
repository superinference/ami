# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2771
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8258 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        v = v.lstrip('><≤≥') 
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

def parse_volume_range(rule_vol_str, actual_vol):
    """Parses volume strings like '100k-1m', '>10m' and checks against actual_vol."""
    if rule_vol_str is None:
        return True
    
    s = str(rule_vol_str).lower().strip()
    
    def parse_val(val_s):
        val_s = val_s.strip()
        mult = 1.0
        if val_s.endswith('k'):
            mult = 1000.0
            val_s = val_s[:-1]
        elif val_s.endswith('m'):
            mult = 1000000.0
            val_s = val_s[:-1]
        return float(val_s) * mult

    try:
        if '-' in s:
            parts = s.split('-')
            low = parse_val(parts[0])
            high = parse_val(parts[1])
            return low <= actual_vol <= high
        elif s.startswith('>'):
            val = parse_val(s[1:])
            return actual_vol > val
        elif s.startswith('<'):
            val = parse_val(s[1:])
            return actual_vol < val
        else:
            return actual_vol == parse_val(s)
    except:
        return False

def parse_fraud_range(rule_fraud_str, actual_fraud_rate):
    """Parses fraud strings like '7.7%-8.3%', '>8.3%' and checks against actual_fraud_rate."""
    if rule_fraud_str is None:
        return True
    
    s = str(rule_fraud_str).strip()
    
    def parse_pct(val_s):
        val_s = val_s.strip().replace('%', '')
        return float(val_s) / 100.0

    try:
        if '-' in s:
            parts = s.split('-')
            low = parse_pct(parts[0])
            high = parse_pct(parts[1])
            return low <= actual_fraud_rate <= high
        elif s.startswith('>'):
            val = parse_pct(s[1:])
            return actual_fraud_rate > val
        elif s.startswith('<'):
            val = parse_pct(s[1:])
            return actual_fraud_rate < val
        else:
            return actual_fraud_rate == parse_pct(s)
    except:
        return False

def match_fee_rule(tx_ctx, rule):
    """Checks if a transaction context matches a fee rule."""
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List)
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List)
    if rule.get('merchant_category_code'):
        if tx_ctx['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Bool)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 5. ACI (List)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Bool)
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['intracountry']:
            return False
            
    # 7. Capture Delay
    if rule.get('capture_delay'):
        r_delay = str(rule['capture_delay']).lower()
        m_delay = str(tx_ctx['capture_delay']).lower()
        
        if r_delay == m_delay:
            pass
        elif m_delay == 'manual' or r_delay == 'manual':
            return False
        else:
            # Handle numeric/immediate
            try:
                days = 0.0 if m_delay == 'immediate' else float(m_delay)
                
                if r_delay == 'immediate':
                    return False # Already checked equality above
                elif '-' in r_delay:
                    low, high = map(float, r_delay.split('-'))
                    if not (low <= days <= high): return False
                elif r_delay.startswith('>'):
                    val = float(r_delay[1:])
                    if not (days > val): return False
                elif r_delay.startswith('<'):
                    val = float(r_delay[1:])
                    if not (days < val): return False
            except:
                return False

    # 8. Monthly Volume
    if not parse_volume_range(rule.get('monthly_volume'), tx_ctx['monthly_volume']):
        return False
        
    # 9. Monthly Fraud Level
    if not parse_fraud_range(rule.get('monthly_fraud_level'), tx_ctx['monthly_fraud_level']):
        return False
        
    return True

def calculate_fee(amount, rule):
    fixed = float(rule.get('fixed_amount', 0))
    rate = float(rule.get('rate', 0))
    return fixed + (rate * amount / 10000.0)

# ---------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------

# Load Data
payments_path = '/output/chunk3/data/context/payments.csv'
fees_path = '/output/chunk3/data/context/fees.json'
merchant_path = '/output/chunk3/data/context/merchant_data.json'

df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 1. Filter for Rafa_AI and 2023
merchant_name = 'Rafa_AI'
df_merchant = df[(df['merchant'] == merchant_name) & (df['year'] == 2023)].copy()

# 2. Calculate Merchant Stats (Volume and Fraud Rate)
# Manual: "Fraud is defined as the ratio of fraudulent volume over total volume."
total_volume = df_merchant['eur_amount'].sum()
fraud_volume = df_merchant[df_merchant['has_fraudulent_dispute'] == True]['eur_amount'].sum()

# Estimate Monthly Volume (Total / 12)
avg_monthly_volume = total_volume / 12.0

# Fraud Level (Ratio)
current_fraud_level = fraud_volume / total_volume if total_volume > 0 else 0.0

# 3. Get Merchant Metadata
merchant_info = next((item for item in merchant_data if item["merchant"] == merchant_name), None)
m_account_type = merchant_info['account_type']
m_mcc = merchant_info['merchant_category_code']
m_capture_delay = merchant_info['capture_delay']

# 4. Identify Fraudulent Transactions to "Move"
fraud_txs = df_merchant[df_merchant['has_fraudulent_dispute'] == True].copy()
fraud_txs['intracountry'] = fraud_txs['issuing_country'] == fraud_txs['acquirer_country']

# 5. Simulate ACIs
possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
results = {}

for target_aci in possible_acis:
    total_fee = 0.0
    
    for _, tx in fraud_txs.iterrows():
        # Build Context with TARGET ACI
        ctx = {
            'card_scheme': tx['card_scheme'],
            'account_type': m_account_type,
            'merchant_category_code': m_mcc,
            'is_credit': bool(tx['is_credit']),
            'aci': target_aci, # Simulation change
            'intracountry': bool(tx['intracountry']),
            'monthly_volume': avg_monthly_volume,
            'monthly_fraud_level': current_fraud_level,
            'capture_delay': m_capture_delay
        }
        
        # Find Fee Rule (First match)
        matched_rule = None
        for rule in fees:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_fee += fee
        else:
            # High penalty if no rule matches (invalid ACI for this config)
            total_fee += 9999.0 
            
    results[target_aci] = total_fee

# 6. Find Best ACI
best_aci = min(results, key=results.get)
print(best_aci)

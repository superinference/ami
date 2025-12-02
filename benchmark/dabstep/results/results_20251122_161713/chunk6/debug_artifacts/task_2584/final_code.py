import pandas as pd
import json
import numpy as np

# Helper functions for robust data processing
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
        try:
            return float(v)
        except:
            return 0.0
    return float(value)

def parse_range_check(value, range_str):
    """
    Checks if a numeric value fits into a range string (e.g. '100k-1m', '>5', '7.7%-8.3%').
    Returns True if match, False otherwise.
    """
    if range_str is None:
        return True
    
    # Normalize range string
    s = str(range_str).strip().lower()
    
    # Handle percentages in range string
    is_percent = '%' in s
    
    # Helper to parse single number from string like '100k', '8.3%'
    def parse_num(n_str):
        n_str = n_str.replace('%', '').replace('€', '').replace('$', '').replace(',', '')
        mult = 1.0
        if 'k' in n_str:
            mult = 1000.0
            n_str = n_str.replace('k', '')
        elif 'm' in n_str:
            mult = 1000000.0
            n_str = n_str.replace('m', '')
        
        try:
            val = float(n_str) * mult
            if is_percent:
                val = val / 100.0
            return val
        except:
            return 0.0

    # Range "min-max"
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            min_val = parse_num(parts[0])
            max_val = parse_num(parts[1])
            # Inclusive boundaries
            return min_val <= value <= max_val
            
    # Inequality ">X" or "<X"
    if s.startswith('>'):
        limit = parse_num(s[1:])
        return value > limit
    if s.startswith('<'):
        limit = parse_num(s[1:])
        return value < limit
        
    # Exact match (unlikely for ranges, but possible)
    return value == parse_num(s)

def parse_capture_delay_match(merchant_delay, rule_delay):
    """
    Matches merchant capture delay (e.g. 'immediate', '1') against rule (e.g. '<3', 'immediate').
    """
    if rule_delay is None:
        return True
    
    m_str = str(merchant_delay).lower()
    r_str = str(rule_delay).lower()
    
    # Exact string match
    if m_str == r_str:
        return True
        
    # Convert merchant delay to number for range comparison
    # 'immediate' -> 0, 'manual' -> 999
    m_val = 0.0
    if m_str == 'immediate':
        m_val = 0.0
    elif m_str == 'manual':
        m_val = 999.0
    else:
        try:
            m_val = float(m_str)
        except:
            return False # Cannot compare if not numeric
            
    # Check rule ranges
    if '-' in r_str: # '3-5'
        parts = r_str.split('-')
        try:
            min_d = float(parts[0])
            max_d = float(parts[1])
            return min_d <= m_val <= max_d
        except:
            return False
    
    if r_str.startswith('>'):
        try:
            limit = float(r_str[1:])
            return m_val > limit
        except:
            return False
            
    if r_str.startswith('<'):
        try:
            limit = float(r_str[1:])
            return m_val < limit
        except:
            return False
            
    return False

def match_fee_rule(tx_ctx, rule):
    """
    Checks if a fee rule applies to a transaction context.
    """
    # 1. Card Scheme
    if rule.get('card_scheme') != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List in rule)
    if rule.get('account_type') and tx_ctx['account_type'] not in rule['account_type']:
        return False
        
    # 3. MCC (List in rule)
    if rule.get('merchant_category_code') and tx_ctx['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 4. Capture Delay
    if not parse_capture_delay_match(tx_ctx['capture_delay'], rule.get('capture_delay')):
        return False
        
    # 5. Monthly Volume
    if not parse_range_check(tx_ctx['monthly_volume'], rule.get('monthly_volume')):
        return False
        
    # 6. Monthly Fraud Level
    if not parse_range_check(tx_ctx['monthly_fraud_level'], rule.get('monthly_fraud_level')):
        return False
        
    # 7. Is Credit
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 8. ACI (List in rule)
    if rule.get('aci') and tx_ctx['aci'] not in rule['aci']:
        return False
        
    # 9. Intracountry
    if rule.get('intracountry') is not None:
        # Rule expects boolean (True/False) or 1.0/0.0
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['intracountry']:
            return False
            
    return True

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'
fees_path = '/output/chunk6/data/context/fees.json'

df_payments = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees = json.load(f)

# 2. Filter for Martinis_Fine_Steakhouse in January
merchant_name = 'Martinis_Fine_Steakhouse'
jan_mask = (df_payments['merchant'] == merchant_name) & (df_payments['day_of_year'] >= 1) & (df_payments['day_of_year'] <= 31)
jan_txs = df_payments[jan_mask].copy()

if len(jan_txs) == 0:
    print("No transactions found for merchant in January.")
    exit()

# 3. Get Merchant Metadata
m_info = next((item for item in merchant_data if item["merchant"] == merchant_name), None)
if not m_info:
    print("Merchant not found in merchant_data.")
    exit()

account_type = m_info.get('account_type')
mcc = m_info.get('merchant_category_code')
capture_delay = m_info.get('capture_delay')

# 4. Calculate Monthly Metrics
monthly_volume = jan_txs['eur_amount'].sum()
fraud_volume = jan_txs[jan_txs['has_fraudulent_dispute'] == True]['eur_amount'].sum()
monthly_fraud_level = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 5. Identify Available Schemes
schemes = set(f['card_scheme'] for f in fees if f.get('card_scheme'))

# 6. Calculate Fees for Each Scheme
scheme_costs = {}

for scheme in schemes:
    total_scheme_fee = 0.0
    possible = True
    
    for _, tx in jan_txs.iterrows():
        # Build context for this transaction, assuming it was processed by 'scheme'
        tx_ctx = {
            'card_scheme': scheme,
            'account_type': account_type,
            'mcc': mcc,
            'capture_delay': capture_delay,
            'monthly_volume': monthly_volume,
            'monthly_fraud_level': monthly_fraud_level,
            'is_credit': bool(tx['is_credit']),
            'aci': tx['aci'],
            'intracountry': (tx['issuing_country'] == tx['acquirer_country'])
        }
        
        # Find matching rule
        matched_rule = None
        # Iterate through fees to find match
        for rule in fees:
            if match_fee_rule(tx_ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            # Calculate fee
            # fee = fixed + (rate * amount / 10000)
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * tx['eur_amount'] / 10000.0)
            total_scheme_fee += fee
        else:
            # If a transaction cannot be processed by a scheme (no rule), 
            # then this scheme is not a valid option for ALL traffic.
            possible = False
            break
    
    if possible:
        scheme_costs[scheme] = total_scheme_fee

# 7. Find Minimum
if not scheme_costs:
    print("No valid schemes found.")
else:
    best_scheme = min(scheme_costs, key=scheme_costs.get)
    print(best_scheme)
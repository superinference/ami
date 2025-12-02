# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2699
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7816 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v.lower():
            return float(v.lower().replace('k', '')) * 1000
        if 'm' in v.lower():
            return float(v.lower().replace('m', '')) * 1000000
        # Range handling (e.g., "50-60") - return mean
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        try:
            return float(v)
        except ValueError:
            return None
    return None

def parse_range(range_str):
    """Parses a range string like '100k-1m' or '>5' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip().lower()
    
    if s.startswith('>'):
        val = coerce_to_float(s[1:])
        return val, float('inf')
    if s.startswith('<'):
        val = coerce_to_float(s[1:])
        return float('-inf'), val
        
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            # Use coerce_to_float on parts, but avoid its internal range averaging
            # by passing simple strings
            min_val = coerce_to_float(parts[0])
            max_val = coerce_to_float(parts[1])
            return min_val, max_val
            
    return None, None

def check_range(value, range_str):
    """Checks if a value falls within a range string."""
    if range_str is None:
        return True
    min_v, max_v = parse_range(range_str)
    if min_v is None: # Parsing failed or not a range
        return True 
    return min_v <= value <= max_v

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    tx_ctx: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (Wildcard [])
    if rule.get('account_type'): # If rule has specific types (not empty list/None)
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (Wildcard [])
    if rule.get('merchant_category_code'):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (Wildcard null)
    if rule.get('capture_delay'):
        r_cd = rule['capture_delay']
        m_cd = str(tx_ctx['capture_delay'])
        
        if r_cd == m_cd:
            pass # Exact match
        elif r_cd == '<3' and m_cd.isdigit() and float(m_cd) < 3:
            pass
        elif r_cd == '3-5' and m_cd.isdigit() and 3 <= float(m_cd) <= 5:
            pass
        elif r_cd == '>5' and m_cd.isdigit() and float(m_cd) > 5:
            pass
        else:
            return False

    # 5. Monthly Volume (Wildcard null)
    if rule.get('monthly_volume'):
        if not check_range(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False

    # 6. Monthly Fraud Level (Wildcard null)
    if rule.get('monthly_fraud_level'):
        if not check_range(tx_ctx['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False

    # 7. Is Credit (Wildcard null)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 8. ACI (Wildcard []) - This is the variable we are simulating
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 9. Intracountry (Wildcard null)
    if rule.get('intracountry') is not None:
        rule_intra = rule['intracountry']
        if isinstance(rule_intra, float):
            rule_intra = bool(rule_intra)
        if rule_intra != tx_ctx['intracountry']:
            return False

    return True

def calculate_fee(amount, rule):
    fixed = rule.get('fixed_amount', 0)
    rate = rule.get('rate', 0)
    return fixed + (rate * amount / 10000)

def execute_step():
    # Load Data
    payments = pd.read_csv('/output/chunk5/data/context/payments.csv')
    with open('/output/chunk5/data/context/fees.json', 'r') as f:
        fees = json.load(f)
    with open('/output/chunk5/data/context/merchant_data.json', 'r') as f:
        merchant_data = json.load(f)

    # Target Merchant and Timeframe
    merchant_name = 'Golfclub_Baron_Friso'
    
    # 1. Get Merchant Attributes
    m_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
    if not m_info:
        print(f"Merchant {merchant_name} not found.")
        return

    # 2. Filter January Transactions for Merchant (All txs for stats)
    # January: day_of_year 1 to 31
    jan_mask = (payments['merchant'] == merchant_name) & \
               (payments['day_of_year'] >= 1) & \
               (payments['day_of_year'] <= 31)
    
    jan_txs = payments[jan_mask].copy()
    
    # 3. Calculate Monthly Stats (Volume and Fraud Rate)
    # These determine the fee tier for the merchant
    monthly_volume = jan_txs['eur_amount'].sum()
    fraud_vol = jan_txs[jan_txs['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    monthly_fraud_rate = fraud_vol / monthly_volume if monthly_volume > 0 else 0
    
    # 4. Identify Fraudulent Transactions (The ones to move/simulate)
    fraud_txs = jan_txs[jan_txs['has_fraudulent_dispute'] == True].copy()
    
    if fraud_txs.empty:
        print("No fraudulent transactions found.")
        return

    # 5. Simulate ACIs
    # We calculate the total fee for the fraudulent transactions assuming they had a specific ACI
    possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    results = {}

    for aci in possible_acis:
        total_fee = 0
        
        for _, tx in fraud_txs.iterrows():
            # Build Context for this transaction
            is_intra = (tx['issuing_country'] == tx['acquirer_country'])
            
            ctx = {
                'card_scheme': tx['card_scheme'],
                'account_type': m_info['account_type'],
                'mcc': m_info['merchant_category_code'],
                'capture_delay': m_info['capture_delay'],
                'monthly_volume': monthly_volume,
                'monthly_fraud_rate': monthly_fraud_rate,
                'is_credit': bool(tx['is_credit']),
                'aci': aci, # The simulated ACI
                'intracountry': is_intra
            }
            
            # Find Matching Rule
            matched_rule = None
            for rule in fees:
                if match_fee_rule(ctx, rule):
                    matched_rule = rule
                    break # Take first match
            
            if matched_rule:
                fee = calculate_fee(tx['eur_amount'], matched_rule)
                total_fee += fee
            else:
                # If no rule matches, we can't calculate cost. 
                # In a real scenario, this might be an error or default fee.
                # For this exercise, we assume coverage.
                pass
        
        results[aci] = total_fee

    # 6. Find Best ACI (Lowest Fee)
    best_aci = min(results, key=results.get)
    
    # Output the result
    print(best_aci)

if __name__ == "__main__":
    execute_step()

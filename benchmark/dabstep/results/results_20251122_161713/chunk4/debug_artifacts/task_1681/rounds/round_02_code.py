# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1681
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8763 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ==========================================
# Helper Functions
# ==========================================

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
        except ValueError:
            return 0.0
    return float(value)

def parse_value_with_units(val_str):
    """Parse string with units (k, m, %) into float."""
    if isinstance(val_str, (int, float)):
        return float(val_str)
    s = str(val_str).lower().strip()
    factor = 1.0
    if s.endswith('%'):
        factor = 0.01
        s = s[:-1]
    elif s.endswith('k'):
        factor = 1000.0
        s = s[:-1]
    elif s.endswith('m'):
        factor = 1000000.0
        s = s[:-1]
    
    # Remove operators for value parsing
    s = s.replace('>', '').replace('<', '').replace('=', '')
    
    try:
        return float(s) * factor
    except ValueError:
        return 0.0

def check_range_condition(value, rule_condition):
    """Check if a numeric value satisfies a rule condition string (e.g., '>5', '100k-1m')."""
    if rule_condition is None:
        return True
    
    s = str(rule_condition).strip()
    
    # Handle range "min-max"
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            try:
                low = parse_value_with_units(parts[0])
                high = parse_value_with_units(parts[1])
                return low <= value <= high
            except:
                pass
                
    # Handle operators
    limit = parse_value_with_units(s)
    if s.startswith('>='):
        return value >= limit
    elif s.startswith('>'):
        return value > limit
    elif s.startswith('<='):
        return value <= limit
    elif s.startswith('<'):
        return value < limit
    
    # Exact match (approximate for floats)
    return abs(value - limit) < 1e-9

def check_capture_delay(merchant_delay, rule_delay):
    """Match merchant capture delay against rule."""
    if rule_delay is None:
        return True
    
    # Map string descriptors to numeric days for comparison
    # immediate = 0, manual = 999 (assumed large)
    def parse_delay(d):
        d_str = str(d).lower()
        if 'immediate' in d_str: return 0.0
        if 'manual' in d_str: return 999.0
        return float(d_str)

    try:
        merch_val = parse_delay(merchant_delay)
        # If rule is a specific string like 'immediate' or 'manual'
        if isinstance(rule_delay, str) and rule_delay.lower() in ['immediate', 'manual']:
            return str(merchant_delay).lower() == rule_delay.lower()
        
        # Otherwise treat rule as a numeric condition (e.g., '>5', '<3')
        return check_range_condition(merch_val, rule_delay)
    except:
        # Fallback to string equality
        return str(merchant_delay) == str(rule_delay)

def match_fee_rule(tx_context, rule):
    """
    Check if a fee rule applies to a transaction context.
    tx_context must contain:
    - card_scheme, account_type, mcc, is_credit, aci, intracountry (transaction/merchant attributes)
    - monthly_volume, monthly_fraud_level (calculated stats)
    - capture_delay (merchant attribute)
    """
    
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match)
    # Rule has list of allowed types. If empty/null, allows all.
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Boolean match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False
            
    # 5. ACI (List match)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Boolean match)
    # Rule might use 0.0/1.0 or False/True
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False
            
    # 7. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not check_range_condition(tx_context['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 8. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not check_range_condition(tx_context['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False
            
    # 9. Capture Delay (Condition match)
    if rule.get('capture_delay'):
        if not check_capture_delay(tx_context['capture_delay'], rule['capture_delay']):
            return False
            
    return True

# ==========================================
# Main Execution
# ==========================================

def main():
    # File paths
    payments_path = '/output/chunk4/data/context/payments.csv'
    fees_path = '/output/chunk4/data/context/fees.json'
    merchant_data_path = '/output/chunk4/data/context/merchant_data.json'
    
    # 1. Load Data
    df_payments = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_data_path, 'r') as f:
        merchant_data = json.load(f)
        
    # Target parameters
    target_merchant = 'Belles_cookbook_store'
    target_year = 2023
    target_day = 10
    
    # 2. Get Merchant Attributes
    merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print(f"Merchant {target_merchant} not found in merchant_data.json")
        return

    mcc = merchant_info['merchant_category_code']
    account_type = merchant_info['account_type']
    capture_delay = merchant_info['capture_delay']
    
    # 3. Calculate Monthly Stats (January 2023)
    # Day 10 is in January (Days 1-31)
    df_jan = df_payments[
        (df_payments['merchant'] == target_merchant) & 
        (df_payments['year'] == target_year) & 
        (df_payments['day_of_year'] <= 31)
    ]
    
    monthly_volume = df_jan['eur_amount'].sum()
    
    # Fraud Level: Ratio of Fraud Volume / Total Volume (as per Manual Section 7)
    fraud_volume = df_jan[df_jan['has_fraudulent_dispute']]['eur_amount'].sum()
    monthly_fraud_level = (fraud_volume / monthly_volume) if monthly_volume > 0 else 0.0
    
    # 4. Filter Target Transactions (Day 10)
    df_target = df_payments[
        (df_payments['merchant'] == target_merchant) & 
        (df_payments['year'] == target_year) & 
        (df_payments['day_of_year'] == target_day)
    ]
    
    # 5. Find Applicable Fees
    applicable_fee_ids = set()
    
    for _, tx in df_target.iterrows():
        # Determine intracountry status
        # "True if the transaction is domestic, defined by the fact that the issuer country and the acquiring country are the same."
        is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
        
        # Build context for this transaction
        tx_context = {
            'card_scheme': tx['card_scheme'],
            'account_type': account_type,
            'mcc': mcc,
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'intracountry': is_intracountry,
            'monthly_volume': monthly_volume,
            'monthly_fraud_level': monthly_fraud_level,
            'capture_delay': capture_delay
        }
        
        # Check against all rules
        for rule in fees_data:
            if match_fee_rule(tx_context, rule):
                applicable_fee_ids.add(rule['ID'])
    
    # 6. Output Result
    # Return sorted list of unique IDs
    result = sorted(list(applicable_fee_ids))
    print(", ".join(map(str, result)))

if __name__ == "__main__":
    main()

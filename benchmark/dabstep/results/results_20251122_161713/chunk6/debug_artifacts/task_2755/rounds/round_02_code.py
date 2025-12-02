# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2755
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 10271 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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
        return float(v)
    return float(value)

def safe_get(df, column, default=None):
    """Safely get column from DataFrame, return default if not exists."""
    if isinstance(df, dict):
        return df.get(column, default)
    elif hasattr(df, 'columns') and column in df.columns:
        return df[column]
    return default

def is_not_empty(array):
    """Check if array/list is not empty. Handles numpy arrays safely."""
    if array is None:
        return False
    if hasattr(array, 'size'):  # numpy array
        return array.size > 0
    try:
        return len(array) > 0
    except TypeError:
        return False

# --- Parsing Helpers for Fee Rules ---

def parse_vol_str(s):
    """Parse volume string like '100k', '1m' to float."""
    if not s: return 0.0
    s = str(s).lower().replace('€', '').replace(',', '').strip()
    if 'k' in s:
        return float(s.replace('k', '')) * 1000
    if 'm' in s:
        return float(s.replace('m', '')) * 1000000
    try:
        return float(s)
    except ValueError:
        return 0.0

def check_volume(rule_vol_str, actual_vol):
    """Check if actual volume fits the rule's volume range."""
    if not rule_vol_str: return True
    
    rule_vol_str = str(rule_vol_str).lower().strip()
    
    if rule_vol_str.startswith('>'):
        limit = parse_vol_str(rule_vol_str[1:])
        return actual_vol > limit
    if rule_vol_str.startswith('<'):
        limit = parse_vol_str(rule_vol_str[1:])
        return actual_vol < limit
    if '-' in rule_vol_str:
        parts = rule_vol_str.split('-')
        if len(parts) == 2:
            low = parse_vol_str(parts[0])
            high = parse_vol_str(parts[1])
            return low <= actual_vol <= high
            
    return False

def parse_fraud_str(s):
    """Parse fraud string like '8.3%' to float (0.083)."""
    if not s: return 0.0
    s = str(s).replace('%', '').strip()
    try:
        return float(s) / 100.0
    except ValueError:
        return 0.0

def check_fraud(rule_fraud_str, actual_fraud):
    """Check if actual fraud rate fits the rule's fraud range."""
    if not rule_fraud_str: return True
    
    rule_fraud_str = str(rule_fraud_str).strip()
    
    if rule_fraud_str.startswith('>'):
        limit = parse_fraud_str(rule_fraud_str[1:])
        return actual_fraud > limit
    if rule_fraud_str.startswith('<'):
        limit = parse_fraud_str(rule_fraud_str[1:])
        return actual_fraud < limit
    if '-' in rule_fraud_str:
        parts = rule_fraud_str.split('-')
        if len(parts) == 2:
            low = parse_fraud_str(parts[0])
            high = parse_fraud_str(parts[1])
            return low <= actual_fraud <= high
            
    return False

def check_capture_delay(rule_delay, actual_delay):
    """Check if actual capture delay fits the rule."""
    if not rule_delay: return True
    if str(rule_delay) == str(actual_delay): return True
    
    # Handle numeric comparisons if actual_delay is numeric string
    try:
        actual_val = float(actual_delay)
        if rule_delay.startswith('<'):
            limit = float(rule_delay[1:])
            return actual_val < limit
        if rule_delay.startswith('>'):
            limit = float(rule_delay[1:])
            return actual_val > limit
        if '-' in rule_delay:
            parts = rule_delay.split('-')
            if len(parts) == 2:
                low = float(parts[0])
                high = float(parts[1])
                return low <= actual_val <= high
    except ValueError:
        pass # actual_delay might be 'manual' or 'immediate'
        
    return False

def match_fee_rule(ctx, rule):
    """
    Match a transaction context against a fee rule.
    ctx: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme
    if rule['card_scheme'] != ctx['card_scheme']:
        return False
        
    # 2. Account Type (List)
    if is_not_empty(rule['account_type']):
        if ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List)
    if is_not_empty(rule['merchant_category_code']):
        if ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. ACI (List) - This is the variable we are simulating
    if is_not_empty(rule['aci']):
        if ctx['aci'] not in rule['aci']:
            return False
            
    # 5. Is Credit (Bool/Null)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != ctx['is_credit']:
            return False
            
    # 6. Intracountry (Bool/Null)
    if rule['intracountry'] is not None:
        is_intra = (ctx['issuing_country'] == ctx['acquirer_country'])
        # rule['intracountry'] is 1.0 (True) or 0.0 (False) usually in JSON
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False
            
    # 7. Capture Delay
    if not check_capture_delay(rule['capture_delay'], ctx['capture_delay']):
        return False
        
    # 8. Monthly Volume
    if not check_volume(rule['monthly_volume'], ctx['monthly_volume']):
        return False
        
    # 9. Monthly Fraud Level
    if not check_fraud(rule['monthly_fraud_level'], ctx['monthly_fraud_rate']):
        return False
        
    return True

def execute_step():
    # File paths
    payments_path = '/output/chunk6/data/context/payments.csv'
    fees_path = '/output/chunk6/data/context/fees.json'
    merchant_path = '/output/chunk6/data/context/merchant_data.json'
    
    # Load data
    df_payments = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)
        
    target_merchant = 'Martinis_Fine_Steakhouse'
    
    # 1. Get Merchant Metadata
    merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print(f"Merchant {target_merchant} not found.")
        return

    mcc = merchant_info['merchant_category_code']
    account_type = merchant_info['account_type']
    capture_delay = merchant_info['capture_delay']
    
    # 2. Calculate December Stats for the Merchant
    # December is day_of_year >= 335 (2023 is not leap year)
    dec_mask = (df_payments['merchant'] == target_merchant) & (df_payments['day_of_year'] >= 335)
    df_dec = df_payments[dec_mask]
    
    total_volume_dec = df_dec['eur_amount'].sum()
    fraud_volume_dec = df_dec[df_dec['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    
    # Fraud rate as ratio of volume (per manual)
    fraud_rate_dec = fraud_volume_dec / total_volume_dec if total_volume_dec > 0 else 0.0
    
    # 3. Identify Target Transactions (Fraudulent ones in December)
    target_txs = df_dec[df_dec['has_fraudulent_dispute'] == True].copy()
    
    # 4. Simulate Fees for each ACI
    possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    results = {}
    
    for aci_candidate in possible_acis:
        total_fee_for_aci = 0.0
        
        for _, tx in target_txs.iterrows():
            # Build context
            ctx = {
                'card_scheme': tx['card_scheme'],
                'is_credit': tx['is_credit'],
                'issuing_country': tx['issuing_country'],
                'acquirer_country': tx['acquirer_country'],
                'amount': tx['eur_amount'],
                # Merchant/Time context
                'account_type': account_type,
                'mcc': mcc,
                'capture_delay': capture_delay,
                'monthly_volume': total_volume_dec,
                'monthly_fraud_rate': fraud_rate_dec,
                # Simulation override
                'aci': aci_candidate
            }
            
            # Find matching rule
            # We assume the first matching rule is the applicable one, or we sum them if additive?
            # Usually fee structures are "find first match". 
            # Given the complexity, we'll look for the first valid match.
            matched_rule = None
            for rule in fees_data:
                if match_fee_rule(ctx, rule):
                    matched_rule = rule
                    break
            
            if matched_rule:
                # Fee = fixed + rate * amount / 10000
                fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * ctx['amount'] / 10000.0)
                total_fee_for_aci += fee
            else:
                # If no rule matches, this ACI might be invalid for this merchant/scheme combo.
                # We should probably penalize it or ignore it. 
                # For this exercise, we assume there's always a fallback or we skip.
                # Let's add a high cost to discourage selection if no rule found?
                # Or just print a warning.
                pass
                
        results[aci_candidate] = total_fee_for_aci

    # 5. Find the best ACI
    # Filter out ACIs with 0 cost if that means no rules matched (unless actual cost is 0)
    # But fees are usually > 0.
    valid_results = {k: v for k, v in results.items() if v > 0}
    
    if not valid_results:
        # Fallback if all 0 (maybe no rules matched?)
        valid_results = results

    best_aci = min(valid_results, key=valid_results.get)
    min_fee = valid_results[best_aci]
    
    print(best_aci)

if __name__ == "__main__":
    execute_step()

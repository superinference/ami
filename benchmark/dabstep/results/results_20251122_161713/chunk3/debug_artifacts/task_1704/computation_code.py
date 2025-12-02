import pandas as pd
import json
import numpy as np
import re

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

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

def parse_range_string(range_str, value_type='numeric'):
    """
    Parses range strings like '100k-1m', '>5', '<3', '7.7%-8.3%'.
    Returns (min_val, max_val).
    """
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip().lower()
    
    # Handle percentages
    is_percent = '%' in s
    if is_percent:
        s = s.replace('%', '')
        
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
        try:
            val = float(v)
            if is_percent:
                val = val / 100.0
            return val * mult
        except ValueError:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif '>' in s:
        val = parse_val(s.replace('>', ''))
        return val, float('inf')
    elif '<' in s:
        val = parse_val(s.replace('<', ''))
        return float('-inf'), val
    elif s == 'immediate':
        return 0, 0 # Treat as numeric 0 for comparison if needed, or handle specifically
    elif s == 'manual':
        return 999, 999 # Treat as high number
    else:
        # Exact value
        val = parse_val(s)
        return val, val

def check_range_match(value, rule_str, value_type='numeric'):
    """Checks if a numeric value falls within a rule string range."""
    if rule_str is None:
        return True
    
    # Special handling for capture_delay strings
    if value_type == 'capture_delay':
        # If rule is specific string like 'immediate' or 'manual'
        if rule_str in ['immediate', 'manual']:
            return value == rule_str
        
        # If rule is numeric range (e.g. '3-5', '>5') and value is numeric string
        try:
            val_num = float(value)
            min_v, max_v = parse_range_string(rule_str, value_type)
            # Handle open ranges carefully
            if '>' in rule_str:
                return val_num > min_v
            if '<' in rule_str:
                return val_num < max_v
            return min_v <= val_num <= max_v
        except ValueError:
            # Value is 'immediate' or 'manual' but rule is numeric range
            return False

    # Standard numeric handling (volume, fraud)
    min_v, max_v = parse_range_string(rule_str, value_type)
    return min_v <= value <= max_v

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_ctx: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != tx_ctx['card_scheme']:
        return False

    # 2. Account Type (List match - Wildcard if empty)
    if rule['account_type']:
        if tx_ctx['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match - Wildcard if empty)
    if rule['merchant_category_code']:
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match - Wildcard if None)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 5. ACI (List match - Wildcard if empty/None)
    if rule['aci']:
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match - Wildcard if None)
    if rule['intracountry'] is not None:
        is_intra = (tx_ctx['issuing_country'] == tx_ctx['acquirer_country'])
        # rule['intracountry'] is 1.0 (True) or 0.0 (False)
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    # 7. Capture Delay (String/Range match - Wildcard if None)
    if rule['capture_delay'] is not None:
        if not check_range_match(tx_ctx['capture_delay'], rule['capture_delay'], 'capture_delay'):
            return False

    # 8. Monthly Volume (Range match - Wildcard if None)
    if rule['monthly_volume'] is not None:
        if not check_range_match(tx_ctx['monthly_volume'], rule['monthly_volume'], 'volume'):
            return False

    # 9. Monthly Fraud Level (Range match - Wildcard if None)
    if rule['monthly_fraud_level'] is not None:
        if not check_range_match(tx_ctx['monthly_fraud_rate'], rule['monthly_fraud_level'], 'fraud'):
            return False

    return True

def execute_step():
    # File paths
    payments_path = '/output/chunk3/data/context/payments.csv'
    merchant_path = '/output/chunk3/data/context/merchant_data.json'
    fees_path = '/output/chunk3/data/context/fees.json'

    # 1. Load Data
    try:
        df_payments = pd.read_csv(payments_path)
        with open(merchant_path, 'r') as f:
            merchant_data = json.load(f)
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    target_merchant = "Martinis_Fine_Steakhouse"
    target_day = 365
    target_year = 2023

    # 2. Get Merchant Details
    merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print(f"Merchant {target_merchant} not found in merchant_data.json")
        return

    print(f"Merchant Info: {merchant_info}")
    
    # 3. Calculate Monthly Stats for December 2023
    # December is roughly day 335 to 365 (31+28+31+30+31+30+31+31+30+31+30 = 334)
    # So Dec 1 is Day 335.
    dec_start = 335
    dec_end = 365
    
    df_dec = df_payments[
        (df_payments['merchant'] == target_merchant) &
        (df_payments['year'] == target_year) &
        (df_payments['day_of_year'] >= dec_start) &
        (df_payments['day_of_year'] <= dec_end)
    ]
    
    monthly_volume = df_dec['eur_amount'].sum()
    
    # Fraud rate calculation (Volume based per manual section 7)
    fraud_volume = df_dec[df_dec['has_fraudulent_dispute']]['eur_amount'].sum()
    monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0
    
    print(f"December Stats - Volume: €{monthly_volume:,.2f}, Fraud Rate: {monthly_fraud_rate:.4%}")

    # 4. Get Transactions for Day 365
    df_target = df_payments[
        (df_payments['merchant'] == target_merchant) &
        (df_payments['year'] == target_year) &
        (df_payments['day_of_year'] == target_day)
    ]
    
    print(f"Found {len(df_target)} transactions for Day {target_day}")

    # 5. Find Applicable Fee IDs
    applicable_ids = set()
    
    for _, tx in df_target.iterrows():
        # Create context for this transaction
        tx_ctx = {
            'card_scheme': tx['card_scheme'],
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'issuing_country': tx['issuing_country'],
            'acquirer_country': tx['acquirer_country'],
            'account_type': merchant_info['account_type'],
            'mcc': merchant_info['merchant_category_code'],
            'capture_delay': merchant_info['capture_delay'],
            'monthly_volume': monthly_volume,
            'monthly_fraud_rate': monthly_fraud_rate
        }
        
        # Check against all rules
        for rule in fees_data:
            if match_fee_rule(tx_ctx, rule):
                applicable_ids.add(rule['ID'])

    # 6. Output Result
    sorted_ids = sorted(list(applicable_ids))
    print("\nApplicable Fee IDs:")
    print(sorted_ids)
    
    # Format for final answer
    print("\nFINAL ANSWER:")
    print(", ".join(map(str, sorted_ids)))

if __name__ == "__main__":
    execute_step()
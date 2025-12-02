# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1513
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9922 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

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

def parse_range_string(range_str):
    """
    Parses strings like '100k-1m', '>10m', '<3', '0.0%-0.5%' into (min, max).
    Used for matching fee rules.
    """
    if not isinstance(range_str, str):
        # If not a string (e.g. None), it shouldn't be parsed as a range constraint
        return -float('inf'), float('inf')
    
    s = range_str.strip().lower().replace(',', '')
    is_percent = '%' in s
    s = s.replace('%', '')
    
    # Handle k/m suffixes for volume
    def parse_val(v):
        if 'k' in v: return float(v.replace('k', '')) * 1000
        if 'm' in v: return float(v.replace('m', '')) * 1000000
        try:
            return float(v)
        except ValueError:
            return 0.0

    try:
        if '-' in s:
            parts = s.split('-')
            low = parse_val(parts[0])
            high = parse_val(parts[1])
            if is_percent:
                low /= 100
                high /= 100
            return low, high
        elif '>' in s:
            val = parse_val(s.replace('>', '').replace('=', ''))
            if is_percent: val /= 100
            return val, float('inf')
        elif '<' in s:
            val = parse_val(s.replace('<', '').replace('=', ''))
            if is_percent: val /= 100
            return 0.0, val # Assuming positive quantities
        else:
            # Exact match treated as range [val, val]
            val = parse_val(s)
            if is_percent: val /= 100
            return val, val
    except:
        return -float('inf'), float('inf')

def match_fee_rule(profile, rule):
    """
    Checks if a transaction profile matches a fee rule.
    Handles wildcards (None/Empty) and specific logic for ranges.
    """
    # 1. Card Scheme (Must match)
    if rule.get('card_scheme') and rule['card_scheme'] != profile['card_scheme']:
        return False
        
    # 2. Account Type (List in rule: Transaction value must be IN list)
    if is_not_empty(rule.get('account_type')):
        if profile['account_type'] not in rule['account_type']:
            return False
            
    # 3. MCC (List in rule: Transaction value must be IN list)
    if is_not_empty(rule.get('merchant_category_code')):
        if profile['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. ACI (List in rule: Transaction value must be IN list)
    if is_not_empty(rule.get('aci')):
        if profile['aci'] not in rule['aci']:
            return False
            
    # 5. Is Credit (Bool: Must match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != profile['is_credit']:
            return False
            
    # 6. Intracountry (Bool: Must match)
    if rule.get('intracountry') is not None:
        if rule['intracountry'] != profile['intracountry']:
            return False
            
    # 7. Capture Delay (String/Range)
    if rule.get('capture_delay'):
        rule_delay = str(rule['capture_delay'])
        prof_delay = str(profile['capture_delay'])
        
        if rule_delay == prof_delay:
            pass # Exact match (e.g. 'manual' == 'manual')
        elif rule_delay in ['manual', 'immediate']:
            if rule_delay != prof_delay: return False
        else:
            # Rule is likely a range (e.g. '>5') and profile is numeric string (e.g. '7')
            try:
                if prof_delay in ['manual', 'immediate']:
                    return False # Range rules don't match categorical delays
                
                val = float(prof_delay)
                min_v, max_v = parse_range_string(rule_delay)
                if not (min_v <= val <= max_v):
                    return False
            except:
                return False

    # 8. Monthly Volume (Range String)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range_string(rule['monthly_volume'])
        if not (min_v <= profile['monthly_volume'] <= max_v):
            return False

    # 9. Monthly Fraud Level (Range String)
    if rule.get('monthly_fraud_level'):
        min_v, max_v = parse_range_string(rule['monthly_fraud_level'])
        # Fraud level in profile is ratio (e.g. 0.083), rule is parsed to ratio
        if not (min_v <= profile['monthly_fraud_level'] <= max_v):
            return False

    return True

# ═══════════════════════════════════════════════════════════
# MAIN ANALYSIS
# ═══════════════════════════════════════════════════════════

def main():
    # 1. Load Data
    try:
        payments = pd.read_csv('/output/chunk4/data/context/payments.csv')
        with open('/output/chunk4/data/context/merchant_data.json') as f:
            merchant_data = json.load(f)
        with open('/output/chunk4/data/context/fees.json') as f:
            fees = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Determine "Average Scenario" Parameters
    # Calculate modes for transaction attributes
    merchant_mode = payments['merchant'].mode()[0]
    is_credit_mode = payments['is_credit'].mode()[0]
    aci_mode = payments['aci'].mode()[0]
    
    # Calculate Intracountry Mode (issuing == acquirer)
    intracountry_series = payments['issuing_country'] == payments['acquirer_country']
    intracountry_mode = intracountry_series.mode()[0]
    
    # 3. Get Merchant Specifics (for the mode merchant)
    merchant_info = next((item for item in merchant_data if item["merchant"] == merchant_mode), None)
    if not merchant_info:
        print(f"Error: Merchant {merchant_mode} not found in merchant_data.json")
        return

    mcc = merchant_info['merchant_category_code']
    account_type = merchant_info['account_type']
    capture_delay = merchant_info['capture_delay']

    # 4. Calculate Merchant Volume and Fraud Stats
    # Filter payments for this merchant
    merchant_txs = payments[payments['merchant'] == merchant_mode]
    
    # Monthly Volume (Total 2023 Volume / 12)
    total_volume = merchant_txs['eur_amount'].sum()
    monthly_volume = total_volume / 12.0
    
    # Fraud Level (Fraud Volume / Total Volume)
    fraud_volume = merchant_txs[merchant_txs['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    monthly_fraud_level = fraud_volume / total_volume if total_volume > 0 else 0.0

    # 5. Define Profile for Fee Matching
    # Question asks for transaction value of 1000 EUR
    transaction_amount = 1000.0
    
    profile = {
        'merchant': merchant_mode,
        'mcc': mcc,
        'account_type': account_type,
        'capture_delay': capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level,
        'is_credit': is_credit_mode,
        'aci': aci_mode,
        'intracountry': intracountry_mode,
        # card_scheme will be iterated
    }

    print("Average Scenario Profile:")
    print(f"Merchant: {profile['merchant']}")
    print(f"MCC: {profile['mcc']}")
    print(f"Account Type: {profile['account_type']}")
    print(f"Capture Delay: {profile['capture_delay']}")
    print(f"Monthly Volume: {profile['monthly_volume']:.2f} EUR")
    print(f"Monthly Fraud Level: {profile['monthly_fraud_level']:.4%}")
    print(f"Is Credit: {profile['is_credit']}")
    print(f"ACI: {profile['aci']}")
    print(f"Intracountry: {profile['intracountry']}")
    print("-" * 30)

    # 6. Calculate Fees for each Scheme
    schemes = ['NexPay', 'GlobalCard', 'SwiftCharge', 'TransactPlus']
    scheme_fees = {}

    for scheme in schemes:
        profile['card_scheme'] = scheme
        
        # Find matching rules
        matching_rules = []
        for rule in fees:
            if match_fee_rule(profile, rule):
                matching_rules.append(rule)
        
        if not matching_rules:
            print(f"No fee rule found for {scheme}")
            continue
            
        # Calculate fee for all matching rules and take the minimum (best price)
        fees_calculated = []
        for rule in matching_rules:
            # Fee formula from manual: fee = fixed_amount + rate * transaction_value / 10000
            fixed = rule['fixed_amount']
            rate = rule['rate']
            fee = fixed + (rate * transaction_amount / 10000.0)
            fees_calculated.append(fee)
        
        min_fee = min(fees_calculated)
        scheme_fees[scheme] = min_fee
        print(f"Scheme: {scheme}, Fee: {min_fee:.4f} EUR")

    # 7. Determine Cheapest
    if scheme_fees:
        cheapest_scheme = min(scheme_fees, key=scheme_fees.get)
        print("-" * 30)
        print(f"Cheapest Scheme: {cheapest_scheme}")
    else:
        print("Could not determine cheapest scheme.")

if __name__ == "__main__":
    main()

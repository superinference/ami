# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1507
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8193 characters (FULL CODE)
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

def check_volume_range(value, range_str):
    """Check if a value fits within a volume range string (e.g., '100k-1m')."""
    if not range_str:
        return True
    try:
        s = str(range_str).lower().replace(',', '').replace('€', '')
        
        def parse_val(v):
            mul = 1
            if 'k' in v:
                mul = 1000
                v = v.replace('k', '')
            elif 'm' in v:
                mul = 1000000
                v = v.replace('m', '')
            return float(v) * mul

        if '-' in s:
            low, high = s.split('-')
            return parse_val(low) <= value <= parse_val(high)
        elif '>' in s:
            return value > parse_val(s.replace('>', ''))
        elif '<' in s:
            return value < parse_val(s.replace('<', ''))
        return False
    except:
        return False

def check_fraud_range(value, range_str):
    """Check if a value fits within a fraud percentage range string (e.g., '0%-0.5%')."""
    if not range_str:
        return True
    try:
        s = str(range_str).replace('%', '')
        
        def parse_val(v):
            return float(v) / 100.0

        if '-' in s:
            low, high = s.split('-')
            return parse_val(low) <= value <= parse_val(high)
        elif '>' in s:
            return value > parse_val(s.replace('>', ''))
        elif '<' in s:
            return value < parse_val(s.replace('<', ''))
        return False
    except:
        return False

def check_capture_delay(merchant_delay, rule_delay):
    """Check if merchant capture delay matches the rule."""
    if rule_delay is None:
        return True
    
    # Exact match (handles 'manual', 'immediate', '1', '7')
    if str(rule_delay) == str(merchant_delay):
        return True
        
    # If merchant delay is not numeric (e.g. 'manual'), it cannot match a numeric range
    # unless the rule is exactly 'manual' which is handled above.
    if not str(merchant_delay).replace('.', '', 1).isdigit():
        return False
        
    # Merchant delay is numeric, check rule range
    try:
        m_val = float(merchant_delay)
        r_str = str(rule_delay)
        if '-' in r_str:
            low, high = r_str.split('-')
            return float(low) <= m_val <= float(high)
        elif '<' in r_str:
            return m_val < float(r_str.replace('<', ''))
        elif '>' in r_str:
            return m_val > float(r_str.replace('>', ''))
    except:
        return False
        
    return False

def main():
    # 1. Load Data
    try:
        payments = pd.read_csv('/output/chunk2/data/context/payments.csv')
        with open('/output/chunk2/data/context/merchant_data.json') as f:
            merchant_data = json.load(f)
        with open('/output/chunk2/data/context/fees.json') as f:
            fees = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Determine Average Scenario Parameters (Modes)
    # Merchant
    mode_merchant = payments['merchant'].mode()[0]
    
    # Is Credit
    mode_is_credit = payments['is_credit'].mode()[0]
    
    # ACI
    mode_aci = payments['aci'].mode()[0]
    
    # Intracountry (Derived)
    payments['intracountry_derived'] = payments['issuing_country'] == payments['acquirer_country']
    mode_intracountry = payments['intracountry_derived'].mode()[0]

    # 3. Get Merchant Specifics from merchant_data.json
    merchant_info = next((item for item in merchant_data if item["merchant"] == mode_merchant), None)
    if not merchant_info:
        print(f"Error: Merchant {mode_merchant} not found in merchant_data.json")
        return

    mcc = merchant_info['merchant_category_code']
    account_type = merchant_info['account_type']
    capture_delay = merchant_info['capture_delay']

    # 4. Calculate Monthly Volume and Fraud Level for this Merchant
    # Filter payments for the specific merchant
    merchant_txs = payments[payments['merchant'] == mode_merchant]
    
    # Calculate Average Monthly Volume
    # Assuming dataset covers 1 full year (2023) as per stats
    total_volume = merchant_txs['eur_amount'].sum()
    avg_monthly_volume = total_volume / 12.0
    
    # Calculate Fraud Rate (Fraud Volume / Total Volume)
    fraud_volume = merchant_txs[merchant_txs['has_fraudulent_dispute']]['eur_amount'].sum()
    fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

    # 5. Find Applicable Fees for each Scheme
    schemes = ['NexPay', 'GlobalCard', 'SwiftCharge', 'TransactPlus']
    results = {}
    transaction_value = 50.0

    for scheme in schemes:
        matching_rule = None
        
        # Iterate through fees to find the first matching rule for this scheme
        # Note: In a real engine, we might look for the *most specific* rule, 
        # but here we assume the list is ordered or rules are mutually exclusive enough.
        for rule in fees:
            # 1. Check Scheme
            if rule['card_scheme'] != scheme:
                continue
                
            # 2. Check Account Type (Wildcard allowed)
            if rule['account_type'] and account_type not in rule['account_type']:
                continue
                
            # 3. Check MCC (Wildcard allowed)
            if rule['merchant_category_code'] and mcc not in rule['merchant_category_code']:
                continue
                
            # 4. Check Is Credit (Wildcard allowed)
            # JSON uses boolean or null
            if rule['is_credit'] is not None and rule['is_credit'] != mode_is_credit:
                continue
                
            # 5. Check ACI (Wildcard allowed)
            if rule['aci'] and mode_aci not in rule['aci']:
                continue
                
            # 6. Check Intracountry (Wildcard allowed)
            # JSON uses 0.0 (False), 1.0 (True), or null
            if rule['intracountry'] is not None:
                rule_intra = bool(rule['intracountry'])
                if rule_intra != mode_intracountry:
                    continue
            
            # 7. Check Capture Delay (Wildcard allowed)
            if not check_capture_delay(capture_delay, rule['capture_delay']):
                continue

            # 8. Check Monthly Volume (Wildcard allowed)
            if not check_volume_range(avg_monthly_volume, rule['monthly_volume']):
                continue

            # 9. Check Monthly Fraud Level (Wildcard allowed)
            if not check_fraud_range(fraud_rate, rule['monthly_fraud_level']):
                continue
            
            # Match found!
            matching_rule = rule
            break
        
        if matching_rule:
            # Calculate Fee: fixed + (rate * amount / 10000)
            fee = matching_rule['fixed_amount'] + (matching_rule['rate'] * transaction_value / 10000.0)
            results[scheme] = fee
        else:
            # If no rule matches, we can't calculate a fee. 
            # In reality, there might be a default, but we'll skip.
            pass

    # 6. Determine Cheapest Scheme
    if results:
        cheapest_scheme = min(results, key=results.get)
        # Output just the scheme name as requested by the question style
        print(cheapest_scheme)
    else:
        print("No applicable schemes found")

if __name__ == "__main__":
    main()

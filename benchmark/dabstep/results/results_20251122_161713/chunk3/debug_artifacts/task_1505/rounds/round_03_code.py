# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1505
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 9982 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

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

def parse_volume_range(vol_str):
    """Parses volume strings like '100k-1m' into (min, max)."""
    if not vol_str:
        return 0.0, float('inf')
    
    s = vol_str.lower().replace(',', '').strip()
    
    def parse_val(val_s):
        multiplier = 1.0
        if 'k' in val_s:
            multiplier = 1000.0
            val_s = val_s.replace('k', '')
        elif 'm' in val_s:
            multiplier = 1000000.0
            val_s = val_s.replace('m', '')
        try:
            return float(val_s) * multiplier
        except ValueError:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif '>' in s:
        return parse_val(s.replace('>', '')), float('inf')
    elif '<' in s:
        return 0.0, parse_val(s.replace('<', ''))
    
    # Fallback
    try:
        val = parse_val(s)
        return val, val 
    except:
        return 0.0, float('inf')

def parse_fraud_range(fraud_str):
    """Parses fraud strings like '>8.3%' or '0%-1%' into (min, max)."""
    if not fraud_str:
        return 0.0, float('inf')
    
    s = fraud_str.replace('%', '').strip()
    
    if '-' in s:
        parts = s.split('-')
        try:
            return float(parts[0])/100.0, float(parts[1])/100.0
        except ValueError:
            return 0.0, float('inf')
    elif '>' in s:
        try:
            return float(s.replace('>', ''))/100.0, float('inf')
        except ValueError:
            return 0.0, float('inf')
    elif '<' in s:
        try:
            return 0.0, float(s.replace('<', ''))/100.0
        except ValueError:
            return 0.0, float('inf')
    
    return 0.0, float('inf')

def match_capture_delay(actual, rule_val):
    """Matches merchant capture delay against rule."""
    if rule_val is None:
        return True
    
    actual_str = str(actual).lower()
    rule_str = str(rule_val).lower()
    
    # Exact string match (e.g., 'manual', 'immediate')
    if actual_str == rule_str:
        return True
        
    # If actual is 'manual' or 'immediate', it usually doesn't match numeric ranges 
    if actual_str in ['manual', 'immediate']:
        return False
        
    # Numeric comparison
    try:
        act_days = float(actual_str)
    except ValueError:
        return False 
        
    if '-' in rule_str:
        parts = rule_str.split('-')
        try:
            return float(parts[0]) <= act_days <= float(parts[1])
        except ValueError:
            return False
    elif '>' in rule_str:
        try:
            return act_days > float(rule_str.replace('>', ''))
        except ValueError:
            return False
    elif '<' in rule_str:
        try:
            return act_days < float(rule_str.replace('<', ''))
        except ValueError:
            return False
        
    return False

def match_fee_rule(scenario, rule):
    """Checks if a fee rule applies to the scenario."""
    
    # 1. Account Type (List)
    if is_not_empty(rule['account_type']):
        if scenario['account_type'] not in rule['account_type']:
            return False
            
    # 2. MCC (List)
    if is_not_empty(rule['merchant_category_code']):
        if scenario['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 3. is_credit (Bool/Null)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != scenario['is_credit']:
            return False
            
    # 4. ACI (List)
    if is_not_empty(rule['aci']):
        if scenario['aci'] not in rule['aci']:
            return False
            
    # 5. Intracountry (Bool/Null - represented as 0.0/1.0 in json)
    if rule['intracountry'] is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != scenario['intracountry']:
            return False

    # 6. Capture Delay (String/Range)
    if rule['capture_delay'] is not None:
        if not match_capture_delay(scenario['capture_delay'], rule['capture_delay']):
            return False

    # 7. Monthly Volume (Range)
    if rule['monthly_volume'] is not None:
        min_v, max_v = parse_volume_range(rule['monthly_volume'])
        if not (min_v <= scenario['monthly_volume'] <= max_v):
            return False

    # 8. Fraud Level (Range)
    if rule['monthly_fraud_level'] is not None:
        min_f, max_f = parse_fraud_range(rule['monthly_fraud_level'])
        # Use a small epsilon for float comparison if needed
        # Check if fraud_rate is within range
        if not (min_f <= scenario['fraud_rate'] <= max_f):
            return False
            
    return True

# ---------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------

def main():
    # 1. Load Data
    try:
        payments = pd.read_csv('/output/chunk3/data/context/payments.csv')
        with open('/output/chunk3/data/context/merchant_data.json') as f:
            merchant_data = json.load(f)
        with open('/output/chunk3/data/context/fees.json') as f:
            fees = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Determine Global Modes (Transaction Parameters)
    # is_credit
    mode_is_credit = payments['is_credit'].mode()[0]
    
    # aci
    mode_aci = payments['aci'].mode()[0]
    
    # intracountry (Derived)
    if 'issuing_country' in payments.columns and 'acquirer_country' in payments.columns:
        payments['is_intracountry'] = payments['issuing_country'] == payments['acquirer_country']
        mode_intracountry = payments['is_intracountry'].mode()[0]
    else:
        mode_intracountry = False 

    # 3. Determine Merchant Parameters (Most Frequent Merchant)
    top_merchant_name = payments['merchant'].mode()[0]
    
    # Get static merchant data
    merchant_info = next((item for item in merchant_data if item["merchant"] == top_merchant_name), None)
    if not merchant_info:
        print(f"Merchant {top_merchant_name} not found in merchant_data.json")
        return

    m_account_type = merchant_info['account_type']
    m_mcc = merchant_info['merchant_category_code']
    m_capture_delay = merchant_info['capture_delay']

    # Calculate dynamic merchant stats (Volume & Fraud)
    merchant_txs = payments[payments['merchant'] == top_merchant_name].copy()
    
    # Average Monthly Volume
    # Convert year+day_of_year to month to handle partial years correctly
    merchant_txs['month_id'] = pd.to_datetime(merchant_txs['year'] * 1000 + merchant_txs['day_of_year'], format='%Y%j').dt.to_period('M')
    
    # Sum volume per month, then average
    monthly_volumes = merchant_txs.groupby('month_id')['eur_amount'].sum()
    avg_monthly_volume = monthly_volumes.mean()
    
    # Fraud Rate (Total Fraud Volume / Total Volume)
    total_vol = merchant_txs['eur_amount'].sum()
    fraud_vol = merchant_txs[merchant_txs['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0

    # 4. Construct the "Average Scenario"
    scenario = {
        'amount': 10.0, # Given in question
        'is_credit': bool(mode_is_credit),
        'aci': mode_aci,
        'intracountry': bool(mode_intracountry),
        'account_type': m_account_type,
        'mcc': m_mcc,
        'capture_delay': m_capture_delay,
        'monthly_volume': avg_monthly_volume,
        'fraud_rate': fraud_rate
    }

    # 5. Calculate Fees for Each Scheme
    schemes = ['NexPay', 'GlobalCard', 'SwiftCharge', 'TransactPlus']
    fees_by_scheme = {}

    for scheme in schemes:
        # Filter rules for this scheme
        scheme_rules = [r for r in fees if r['card_scheme'] == scheme]
        
        # Find first matching rule
        matched_rule = None
        for rule in scheme_rules:
            if match_fee_rule(scenario, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            fixed = coerce_to_float(matched_rule['fixed_amount'])
            rate = coerce_to_float(matched_rule['rate'])
            # Fee formula: fixed + (rate * amount / 10000)
            fee = fixed + (rate * scenario['amount'] / 10000)
            fees_by_scheme[scheme] = fee
        else:
            fees_by_scheme[scheme] = float('inf')

    # 6. Determine Winner
    if not fees_by_scheme:
        print("No applicable schemes found.")
        return

    # Find scheme with minimum fee
    cheapest_scheme = min(fees_by_scheme, key=fees_by_scheme.get)
    
    # Output ONLY the answer as requested
    print(cheapest_scheme)

if __name__ == "__main__":
    main()

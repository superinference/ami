# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1515
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7256 characters (FULL CODE)
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
    """Check if a numeric value falls within a volume range string (e.g., '100k-1m')."""
    if not range_str: return True
    v = float(value)
    s = range_str.lower().replace(',', '').strip()
    
    def parse_val(x):
        m = 1.0
        if 'k' in x:
            m = 1000.0
            x = x.replace('k', '')
        elif 'm' in x:
            m = 1000000.0
            x = x.replace('m', '')
        return float(x) * m

    if '-' in s:
        parts = s.split('-')
        low = parse_val(parts[0])
        high = parse_val(parts[1])
        return low <= v <= high
    elif '>' in s:
        val = parse_val(s.replace('>', ''))
        return v > val
    elif '<' in s:
        val = parse_val(s.replace('<', ''))
        return v < val
    return False

def check_fraud_range(value, range_str):
    """Check if a numeric value falls within a fraud percentage range string (e.g., '0.0%-0.5%')."""
    if not range_str: return True
    # value is float 0.0 to 1.0 (e.g. 0.008 for 0.8%)
    s = range_str.replace('%', '').strip()
    
    def parse_val(x):
        return float(x) / 100.0

    if '-' in s:
        parts = s.split('-')
        low = parse_val(parts[0])
        high = parse_val(parts[1])
        return low <= value <= high
    elif '>' in s:
        val = parse_val(s.replace('>', ''))
        return value > val
    elif '<' in s:
        val = parse_val(s.replace('<', ''))
        return value < val
    return False

def solve():
    # Load data
    try:
        payments = pd.read_csv('/output/chunk6/data/context/payments.csv')
        with open('/output/chunk6/data/context/fees.json', 'r') as f:
            fees = json.load(f)
        with open('/output/chunk6/data/context/merchant_data.json', 'r') as f:
            merchant_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 1. Determine Average Scenario Parameters (Global Modes)
    # We define the "average scenario" by the most frequent values in the dataset
    if 'is_credit' not in payments.columns or 'aci' not in payments.columns or 'merchant' not in payments.columns:
        print("Required columns missing in payments.csv")
        return

    is_credit_mode = payments['is_credit'].mode()[0]
    aci_mode = payments['aci'].mode()[0]
    merchant_mode = payments['merchant'].mode()[0]
    
    # Calculate intracountry (True if issuing == acquirer)
    payments['is_intracountry'] = payments['issuing_country'] == payments['acquirer_country']
    intracountry_mode = payments['is_intracountry'].mode()[0]
    
    # 2. Get Merchant Specifics for the most common merchant
    merchant_info = next((item for item in merchant_data if item["merchant"] == merchant_mode), None)
    if not merchant_info:
        print(f"Error: Merchant {merchant_mode} not found in merchant_data.json")
        return

    mcc = merchant_info['merchant_category_code']
    account_type = merchant_info['account_type']
    capture_delay_merchant = merchant_info['capture_delay']
    
    # 3. Calculate Merchant Volume and Fraud Stats
    # Filter payments for this merchant
    merchant_txs = payments[payments['merchant'] == merchant_mode]
    
    # Monthly Volume: Total Volume / 12 (assuming dataset covers full year 2023)
    total_volume = merchant_txs['eur_amount'].sum()
    avg_monthly_volume = total_volume / 12.0
    
    # Fraud Rate: Fraudulent Volume / Total Volume
    fraud_txs = merchant_txs[merchant_txs['has_fraudulent_dispute'] == True]
    fraud_volume = fraud_txs['eur_amount'].sum()
    fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0
    
    # 4. Find Applicable Fees for each Scheme
    tx_value = 5000.0
    schemes = set(f['card_scheme'] for f in fees)
    scheme_costs = {}

    for scheme in schemes:
        matches = []
        for rule in fees:
            # Filter by Scheme
            if rule['card_scheme'] != scheme:
                continue
            
            # Filter by Account Type (list match)
            if rule['account_type'] and account_type not in rule['account_type']:
                continue
            
            # Filter by MCC (list match)
            if rule['merchant_category_code'] and mcc not in rule['merchant_category_code']:
                continue
            
            # Filter by is_credit (exact match or null)
            if rule['is_credit'] is not None and rule['is_credit'] != is_credit_mode:
                continue
            
            # Filter by ACI (list match)
            if rule['aci'] and aci_mode not in rule['aci']:
                continue
            
            # Filter by Intracountry (exact match or null)
            if rule['intracountry'] is not None:
                # JSON 0.0/1.0/null -> convert to bool for comparison
                if bool(rule['intracountry']) != intracountry_mode:
                    continue
            
            # Filter by Capture Delay (exact match or null)
            # Note: 'manual' is a distinct category in the manual, so we use strict matching
            if rule['capture_delay'] is not None:
                if rule['capture_delay'] != capture_delay_merchant:
                    continue
            
            # Filter by Monthly Volume (range match or null)
            if rule['monthly_volume']:
                if not check_volume_range(avg_monthly_volume, rule['monthly_volume']):
                    continue
            
            # Filter by Monthly Fraud Level (range match or null)
            if rule['monthly_fraud_level']:
                if not check_fraud_range(fraud_rate, rule['monthly_fraud_level']):
                    continue
            
            matches.append(rule)
        
        if matches:
            # If multiple rules match, we assume the first valid one applies (or they are consistent)
            rule = matches[0]
            # Fee Formula: fixed_amount + (rate * amount / 10000)
            fee = rule['fixed_amount'] + (rule['rate'] * tx_value / 10000.0)
            scheme_costs[scheme] = fee
        else:
            # Scheme not applicable for this scenario
            pass

    # 5. Determine Cheapest Scheme
    if not scheme_costs:
        print("No applicable schemes found")
    else:
        min_scheme = min(scheme_costs, key=scheme_costs.get)
        print(min_scheme)

if __name__ == "__main__":
    solve()

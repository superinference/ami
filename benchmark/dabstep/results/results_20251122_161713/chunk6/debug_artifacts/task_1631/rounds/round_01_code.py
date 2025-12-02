# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1631
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: sufficient
# Plan steps: 1
# Code length: 7764 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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

def safe_index(array, idx, default=None):
    """Safely get array element at index, return default if out of bounds."""
    try:
        if 0 <= idx < len(array):
            return array[idx]
        return default
    except (IndexError, TypeError, AttributeError):
        return default


import pandas as pd
import numpy as np
import json

def solve():
    # Load datasets
    fees_df = pd.read_json('/output/chunk6/data/context/fees.json')
    merchant_data = pd.read_json('/output/chunk6/data/context/merchant_data.json')
    payments = pd.read_csv('/output/chunk6/data/context/payments.csv')

    # 1. Identify Account Type F Merchants
    # Filter merchant_data for account_type == 'F'
    type_f_merchants_df = merchant_data[merchant_data['account_type'] == 'F']
    type_f_merchants = set(type_f_merchants_df['merchant'])

    # 2. Calculate Merchant Stats (Monthly Volume, Fraud Rate)
    # Group payments by merchant to calculate total volume and fraud volume
    # Assuming payments.csv covers 1 year (2023)
    merchant_stats = {}
    
    # Pre-calculate totals per merchant
    merchant_totals = payments.groupby('merchant').agg(
        total_volume=('eur_amount', 'sum'),
        fraud_volume=('eur_amount', lambda x: x[payments.loc[x.index, 'has_fraudulent_dispute']].sum())
    )
    
    # Join with merchant static data
    for merchant_name in type_f_merchants:
        if merchant_name not in merchant_totals.index:
            continue
            
        row = merchant_totals.loc[merchant_name]
        static_info = merchant_data[merchant_data['merchant'] == merchant_name].iloc[0]
        
        monthly_vol = row['total_volume'] / 12.0
        fraud_rate = row['fraud_volume'] / row['total_volume'] if row['total_volume'] > 0 else 0.0
        
        merchant_stats[merchant_name] = {
            'account_type': static_info['account_type'],
            'mcc': static_info['merchant_category_code'],
            'capture_delay': static_info['capture_delay'],
            'monthly_volume': monthly_vol,
            'fraud_rate': fraud_rate
        }

    # 3. Filter Transactions
    # We want transactions for Account Type F merchants AND Card Scheme SwiftCharge
    target_txs = payments[
        (payments['merchant'].isin(type_f_merchants)) & 
        (payments['card_scheme'] == 'SwiftCharge')
    ].copy()

    # 4. Define Matching Logic Helpers
    def parse_vol(v_str):
        if not v_str: return None
        v_str = v_str.lower().replace('k', '000').replace('m', '000000')
        if '-' in v_str:
            l, h = v_str.split('-')
            return float(l), float(h)
        if v_str.startswith('>'): return float(v_str[1:]), float('inf')
        if v_str.startswith('<'): return 0.0, float(v_str[1:])
        return None

    def parse_fraud(f_str):
        if not f_str: return None
        s = f_str.replace('%', '')
        if '-' in s:
            l, h = s.split('-')
            return float(l)/100, float(h)/100
        if s.startswith('>'): return float(s[1:])/100, float('inf')
        if s.startswith('<'): return 0.0, float(s[1:])/100
        return None

    def check_capture(m_delay, rule_delay):
        if not rule_delay: return True
        if rule_delay == 'manual': return m_delay == 'manual'
        if rule_delay == 'immediate': return m_delay == 'immediate'
        # Numeric check
        if str(m_delay).isdigit():
            val = int(m_delay)
            if '-' in rule_delay:
                l, h = map(int, rule_delay.split('-'))
                return l <= val <= h
            if rule_delay.startswith('>'): return val > int(rule_delay[1:])
            if rule_delay.startswith('<'): return val < int(rule_delay[1:])
        return False

    # Prepare fees rules
    # Filter for SwiftCharge
    fees_rules = fees_df[fees_df['card_scheme'] == 'SwiftCharge'].to_dict('records')
    
    # Sort rules by specificity (number of non-null conditions) to ensure most specific rule applies first
    criteria = ['account_type', 'merchant_category_code', 'aci', 'is_credit', 'intracountry', 'capture_delay', 'monthly_volume', 'monthly_fraud_level']
    fees_rules.sort(key=lambda x: sum(1 for k in criteria if x[k] not in [None, []]), reverse=True)

    calculated_fees = []

    for _, tx in target_txs.iterrows():
        m_name = tx['merchant']
        if m_name not in merchant_stats:
            continue
            
        m_info = merchant_stats[m_name]
        
        # Transaction attributes
        tx_aci = tx['aci']
        tx_credit = tx['is_credit']
        tx_intra = (tx['issuing_country'] == tx['acquirer_country'])
        
        # Find matching rule
        matched_rule = None
        
        for rule in fees_rules:
            # Account Type
            if rule['account_type'] and m_info['account_type'] not in rule['account_type']: continue
            # MCC
            if rule['merchant_category_code'] and m_info['mcc'] not in rule['merchant_category_code']: continue
            # ACI
            if rule['aci'] and tx_aci not in rule['aci']: continue
            # Is Credit
            if rule['is_credit'] is not None and rule['is_credit'] != tx_credit: continue
            # Intracountry
            if rule['intracountry'] is not None:
                if bool(rule['intracountry']) != tx_intra: continue
            # Capture Delay
            if not check_capture(m_info['capture_delay'], rule['capture_delay']): continue
            # Volume
            if rule['monthly_volume']:
                min_v, max_v = parse_vol(rule['monthly_volume'])
                if not (min_v <= m_info['monthly_volume'] <= max_v): continue
            # Fraud
            if rule['monthly_fraud_level']:
                min_f, max_f = parse_fraud(rule['monthly_fraud_level'])
                if not (min_f <= m_info['fraud_rate'] <= max_f): continue
            
            matched_rule = rule
            break # Stop at first (most specific) match
        
        if matched_rule:
            # Calculate fee for 500 EUR
            # fee = fixed + rate/10000 * amount
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * 500.0 / 10000.0)
            calculated_fees.append(fee)

    # Average
    if calculated_fees:
        avg = np.mean(calculated_fees)
        print(f"{avg:.6f}")
    else:
        print("No matching transactions found.")

if __name__ == "__main__":
    solve()

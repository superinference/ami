# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1299
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 10088 characters (FULL CODE)
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

def safe_index(array, idx, default=None):
    """Safely get array element at index, return default if out of bounds."""
    try:
        if 0 <= idx < len(array):
            return array[idx]
        return default
    except (IndexError, TypeError, AttributeError):
        return default

def calculate_fee(amount, rule):
    """
    Calculates fee based on formula: fee = fixed_amount + (rate * amount / 10000)
    """
    fixed = float(rule.get('fixed_amount', 0))
    rate = float(rule.get('rate', 0))
    return fixed + (rate * amount / 10000)

def parse_range_value(val_str, is_percentage=False):
    """
    Parses a range string like '100k-1m', '>5', '7.7%-8.3%' into (min, max).
    Returns (min, max) tuple.
    """
    if not isinstance(val_str, str):
        return (float('-inf'), float('inf'))
    
    val_str = val_str.lower().replace(',', '').replace('€', '')
    
    # Handle multipliers
    def parse_num(s):
        mul = 1
        if 'k' in s:
            mul = 1000
            s = s.replace('k', '')
        elif 'm' in s:
            mul = 1000000
            s = s.replace('m', '')
        
        if '%' in s:
            s = s.replace('%', '')
            return float(s) / 100 * mul
        return float(s) * mul

    try:
        if '-' in val_str:
            parts = val_str.split('-')
            return (parse_num(parts[0]), parse_num(parts[1]))
        elif '>' in val_str:
            return (parse_num(val_str.replace('>', '')), float('inf'))
        elif '<' in val_str:
            return (float('-inf'), parse_num(val_str.replace('<', '')))
        else:
            # Exact match treated as range [val, val]
            val = parse_num(val_str)
            return (val, val)
    except:
        return (float('-inf'), float('inf'))

def check_capture_delay(merchant_delay, rule_delay):
    """
    Checks if merchant capture delay matches rule.
    Merchant: '1', '2', '7', 'immediate', 'manual'
    Rule: '3-5', '>5', '<3', 'immediate', 'manual', None
    """
    if rule_delay is None:
        return True
    
    m_delay = str(merchant_delay).lower()
    r_delay = str(rule_delay).lower()
    
    # Direct string match (immediate, manual)
    if m_delay == r_delay:
        return True
    
    # If merchant is numeric (1, 2, 7)
    if m_delay.isdigit():
        m_val = float(m_delay)
        
        if '-' in r_delay:
            low, high = map(float, r_delay.split('-'))
            return low <= m_val <= high
        elif '>' in r_delay:
            limit = float(r_delay.replace('>', ''))
            return m_val > limit
        elif '<' in r_delay:
            limit = float(r_delay.replace('<', ''))
            return m_val < limit
            
    return False

def solve():
    # 1. Load Data
    payments_path = '/output/chunk3/data/context/payments.csv'
    fees_path = '/output/chunk3/data/context/fees.json'
    merchant_path = '/output/chunk3/data/context/merchant_data.json'
    
    df_payments = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)
        
    # 2. Preprocess Merchant Data
    merchant_lookup = {m['merchant']: m for m in merchant_data}
    
    # 3. Calculate Monthly Stats (Volume & Fraud) per Merchant
    # Convert day_of_year to month (Year 2023)
    df_payments['date'] = pd.to_datetime(df_payments['year'] * 1000 + df_payments['day_of_year'], format='%Y%j')
    df_payments['month'] = df_payments['date'].dt.month
    
    # Monthly Volume (Sum of eur_amount)
    monthly_vol = df_payments.groupby(['merchant', 'month'])['eur_amount'].sum().reset_index()
    monthly_vol.rename(columns={'eur_amount': 'monthly_volume'}, inplace=True)
    
    # Monthly Fraud Volume (Sum of eur_amount where has_fraudulent_dispute is True)
    # Manual: "fraud levels measured as ratio between monthly total volume and monthly volume notified as fraud"
    fraud_txs = df_payments[df_payments['has_fraudulent_dispute'] == True]
    monthly_fraud_vol = fraud_txs.groupby(['merchant', 'month'])['eur_amount'].sum().reset_index()
    monthly_fraud_vol.rename(columns={'eur_amount': 'fraud_volume'}, inplace=True)
    
    # Merge stats
    merchant_stats = pd.merge(monthly_vol, monthly_fraud_vol, on=['merchant', 'month'], how='left')
    merchant_stats['fraud_volume'] = merchant_stats['fraud_volume'].fillna(0)
    merchant_stats['fraud_rate'] = merchant_stats['fraud_volume'] / merchant_stats['monthly_volume']
    
    # Create lookup: (merchant, month) -> stats
    stats_lookup = {}
    for _, row in merchant_stats.iterrows():
        stats_lookup[(row['merchant'], row['month'])] = {
            'volume': row['monthly_volume'],
            'fraud_rate': row['fraud_rate']
        }
        
    # 4. Filter Target Transactions (SwiftCharge + Credit)
    # We want to calculate the average fee for a hypothetical 1234 EUR transaction
    # weighted by the distribution of ACTUAL SwiftCharge Credit transactions.
    target_txs = df_payments[
        (df_payments['card_scheme'] == 'SwiftCharge') & 
        (df_payments['is_credit'] == True)
    ].copy()
    
    print(f"Found {len(target_txs)} SwiftCharge Credit transactions to analyze.")
    
    # 5. Filter Relevant Fees
    # Keep rules that are for SwiftCharge and (Credit OR Wildcard)
    relevant_fees = [
        f for f in fees_data 
        if f['card_scheme'] == 'SwiftCharge' 
        and (f['is_credit'] is True or f['is_credit'] is None)
    ]
    # Sort by ID to ensure consistent matching order
    relevant_fees.sort(key=lambda x: x['ID'])
    
    # 6. Calculate Fees
    calculated_fees = []
    hypothetical_amount = 1234.0
    
    for _, tx in target_txs.iterrows():
        merchant_name = tx['merchant']
        month = tx['month']
        
        # Get Merchant Info
        m_info = merchant_lookup.get(merchant_name)
        if not m_info:
            continue
            
        # Get Monthly Stats
        m_stats = stats_lookup.get((merchant_name, month))
        if not m_stats:
            continue
            
        # Transaction Attributes
        tx_aci = tx['aci']
        is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
        
        # Find Matching Rule
        matched_rule = None
        for rule in relevant_fees:
            # 1. Account Type
            if rule['account_type'] and m_info['account_type'] not in rule['account_type']:
                continue
            
            # 2. Merchant Category Code
            if rule['merchant_category_code'] and m_info['merchant_category_code'] not in rule['merchant_category_code']:
                continue
                
            # 3. ACI
            if rule['aci'] and tx_aci not in rule['aci']:
                continue
                
            # 4. Intracountry
            # JSON uses 0.0/1.0 for boolean sometimes, or null. Python loads as float or None.
            # Or boolean true/false.
            # Let's handle both.
            r_intra = rule['intracountry']
            if r_intra is not None:
                # Convert rule value to bool
                r_intra_bool = bool(r_intra)
                if r_intra_bool != is_intracountry:
                    continue
            
            # 5. Capture Delay
            if not check_capture_delay(m_info['capture_delay'], rule['capture_delay']):
                continue
                
            # 6. Monthly Volume
            if rule['monthly_volume']:
                min_v, max_v = parse_range_value(rule['monthly_volume'])
                if not (min_v <= m_stats['volume'] <= max_v):
                    continue
                    
            # 7. Monthly Fraud Level
            if rule['monthly_fraud_level']:
                min_f, max_f = parse_range_value(rule['monthly_fraud_level'], is_percentage=True)
                # Fraud rate is 0.083 for 8.3%
                if not (min_f <= m_stats['fraud_rate'] <= max_f):
                    continue
            
            # Match found
            matched_rule = rule
            break
        
        if matched_rule:
            fee = calculate_fee(hypothetical_amount, matched_rule)
            calculated_fees.append(fee)
        else:
            # print(f"Warning: No rule found for tx {tx['psp_reference']}")
            pass
            
    # 7. Average
    if calculated_fees:
        avg_fee = sum(calculated_fees) / len(calculated_fees)
        print(f"Total fees calculated: {len(calculated_fees)}")
        print(f"Average Fee: {avg_fee:.14f}")
    else:
        print("No applicable fees found.")

if __name__ == "__main__":
    solve()

# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2547
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9205 characters (FULL CODE)
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

# --- Custom Logic for Fee Calculation ---

def parse_value_custom(val_str):
    """Parses a value string with units (k, m, %) into a float."""
    val_str = str(val_str).strip().lower()
    scale = 1.0
    if '%' in val_str:
        scale = 0.01
        val_str = val_str.replace('%', '')
    
    if 'k' in val_str:
        scale *= 1000
        val_str = val_str.replace('k', '')
    elif 'm' in val_str:
        scale *= 1000000
        val_str = val_str.replace('m', '')
        
    try:
        return float(val_str) * scale
    except ValueError:
        return 0.0

def check_range(range_str, value):
    """Checks if a value falls within a range string (e.g., '100k-1m', '>5%', '<3')."""
    if range_str is None:
        return True
    s = str(range_str).strip()
    if s == '':
        return True
    
    # Handle operators
    if s.startswith(('>', '<', '≥', '≤')):
        operator = s[0]
        if len(s) > 1 and s[1] == '=': 
            operator += '='
            num_str = s[2:]
        else:
            num_str = s[1:]
        
        limit = parse_value_custom(num_str)
        
        if operator == '>': return value > limit
        if operator == '<': return value < limit
        if operator == '≥' or operator == '>=': return value >= limit
        if operator == '≤' or operator == '<=': return value <= limit
        
    # Handle range "min-max"
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            try:
                min_val = parse_value_custom(parts[0])
                max_val = parse_value_custom(parts[1])
                return min_val <= value <= max_val
            except:
                return False
    
    # Exact match
    try:
        return value == parse_value_custom(s)
    except:
        return False

def check_capture_delay(rule_delay, merchant_delay):
    """Checks if merchant capture delay matches the rule."""
    if rule_delay is None:
        return True
    
    md = str(merchant_delay).lower()
    rd = str(rule_delay).lower()
    
    if rd == md:
        return True
        
    # Handle numeric comparisons for days
    if md.isdigit():
        days = int(md)
        if rd == '<3':
            return days < 3
        if rd == '>5':
            return days > 5
        if rd == '3-5':
            return 3 <= days <= 5
    
    return False

def main():
    # 1. Load Data
    payments_path = '/output/chunk3/data/context/payments.csv'
    fees_path = '/output/chunk3/data/context/fees.json'
    merchant_data_path = '/output/chunk3/data/context/merchant_data.json'

    df_payments = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees_rules = json.load(f)
    with open(merchant_data_path, 'r') as f:
        merchant_data = json.load(f)

    target_merchant = "Martinis_Fine_Steakhouse"
    target_year = 2023

    # 2. Filter Transactions
    df = df_payments[
        (df_payments['merchant'] == target_merchant) & 
        (df_payments['year'] == target_year)
    ].copy()

    if df.empty:
        print("No transactions found.")
        return

    # 3. Get Merchant Metadata
    merchant_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
    if not merchant_info:
        print("Merchant info not found.")
        return

    original_mcc = merchant_info['merchant_category_code']
    account_type = merchant_info['account_type']
    capture_delay_val = merchant_info['capture_delay']

    # 4. Pre-calculate Monthly Stats (Volume and Fraud Rate)
    # Create date/month column
    df['date'] = pd.to_datetime(df['year'] * 1000 + df['day_of_year'], format='%Y%j')
    df['month'] = df['date'].dt.month

    monthly_stats = {}
    for month in df['month'].unique():
        month_df = df[df['month'] == month]
        total_vol = month_df['eur_amount'].sum()
        fraud_vol = month_df[month_df['has_fraudulent_dispute'] == True]['eur_amount'].sum()
        # Fraud rate = Fraud Volume / Total Volume
        fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
        monthly_stats[month] = {
            'volume': total_vol,
            'fraud_rate': fraud_rate
        }

    # 5. Define Fee Calculation Function
    def calculate_total_fees_for_mcc(mcc_code):
        total_fee = 0.0
        
        # Iterate through each transaction
        for _, row in df.iterrows():
            # Transaction attributes
            r_scheme = row['card_scheme']
            r_credit = row['is_credit'] # boolean in csv
            r_aci = row['aci']
            r_intra = (row['issuing_country'] == row['acquirer_country'])
            r_amount = row['eur_amount']
            
            # Monthly stats for this transaction
            m_stats = monthly_stats[row['month']]
            m_vol = m_stats['volume']
            m_fraud = m_stats['fraud_rate']
            
            matched_rule = None
            
            # Find the first matching rule
            for rule in fees_rules:
                # 1. Card Scheme
                if rule['card_scheme'] != r_scheme:
                    continue
                
                # 2. Account Type (list, empty=wildcard)
                if is_not_empty(rule['account_type']) and account_type not in rule['account_type']:
                    continue
                    
                # 3. Capture Delay
                if not check_capture_delay(rule['capture_delay'], capture_delay_val):
                    continue
                
                # 4. Monthly Fraud Level
                if not check_range(rule['monthly_fraud_level'], m_fraud):
                    continue
                    
                # 5. Monthly Volume
                if not check_range(rule['monthly_volume'], m_vol):
                    continue
                    
                # 6. MCC (list, empty=wildcard)
                if is_not_empty(rule['merchant_category_code']) and mcc_code not in rule['merchant_category_code']:
                    continue
                    
                # 7. Is Credit (bool, null=wildcard)
                if rule['is_credit'] is not None:
                    if rule['is_credit'] != r_credit:
                        continue
                        
                # 8. ACI (list, empty=wildcard)
                if is_not_empty(rule['aci']) and r_aci not in rule['aci']:
                    continue
                    
                # 9. Intracountry (bool/float, null=wildcard)
                if rule['intracountry'] is not None:
                    # Convert 0.0/1.0 to bool
                    rule_intra = bool(rule['intracountry'])
                    if rule_intra != r_intra:
                        continue
                
                # Match found
                matched_rule = rule
                break
            
            if matched_rule:
                # Fee = Fixed + (Rate * Amount / 10000)
                fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * r_amount / 10000.0)
                total_fee += fee
            else:
                # No rule found (should not happen in synthetic data usually)
                pass
                
        return total_fee

    # 6. Calculate Fees for Both Scenarios
    fees_original = calculate_total_fees_for_mcc(original_mcc)
    fees_new = calculate_total_fees_for_mcc(7523)

    # 7. Calculate Delta
    delta = fees_new - fees_original
    
    # 8. Output
    print(f"{delta:.14f}")

if __name__ == "__main__":
    main()

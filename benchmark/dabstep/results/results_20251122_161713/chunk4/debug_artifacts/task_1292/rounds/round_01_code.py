# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1292
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: sufficient
# Plan steps: 1
# Code length: 7788 characters (FULL CODE)
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
import json
import numpy as np

def calculate_transactplus_avg_fee():
    # File paths
    payments_path = '/output/chunk4/data/context/payments.csv'
    fees_path = '/output/chunk4/data/context/fees.json'
    merchant_data_path = '/output/chunk4/data/context/merchant_data.json'

    # 1. Load Data
    try:
        df_payments = pd.read_csv(payments_path)
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
        with open(merchant_data_path, 'r') as f:
            merchant_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Pre-calculate Merchant Stats (Volume & Fraud)
    # Needed for fee rules involving monthly_volume or monthly_fraud_level
    # Assuming the dataset represents a full year (based on day_of_year max ~365)
    merchant_stats = {}
    for merchant, group in df_payments.groupby('merchant'):
        total_vol = group['eur_amount'].sum()
        fraud_vol = group[group['has_fraudulent_dispute']]['eur_amount'].sum()
        
        # Monthly averages
        monthly_vol = total_vol / 12.0
        # Fraud rate as percentage (e.g., 8.3 for 8.3%)
        fraud_rate = (fraud_vol / total_vol * 100.0) if total_vol > 0 else 0.0
        
        merchant_stats[merchant] = {
            'monthly_volume': monthly_vol,
            'fraud_rate': fraud_rate
        }

    # 3. Prepare Merchant Attributes Lookup
    merchant_attrs = {m['merchant']: m for m in merchant_data}

    # 4. Filter Transactions
    # Criteria: Card Scheme = TransactPlus, Is Credit = True
    target_txs = df_payments[
        (df_payments['card_scheme'] == 'TransactPlus') & 
        (df_payments['is_credit'] == True)
    ].copy()

    if target_txs.empty:
        print("No TransactPlus credit transactions found.")
        return

    # 5. Define Matching Helpers
    def parse_check(value, rule_str):
        """
        Checks if a value matches a rule string (range, inequality, or exact).
        value: numeric or string from data
        rule_str: string from fees.json (e.g., "100k-1m", ">5", "manual")
        """
        if rule_str is None:
            return True
            
        s_rule = str(rule_str)
        s_val = str(value)
        
        # Handle percentages
        if '%' in s_rule:
            s_rule = s_rule.replace('%', '')
            # value is expected to be passed as percentage already
        
        # Handle k/m suffixes
        if 'k' in s_rule or 'm' in s_rule:
            s_rule = s_rule.replace('k', '000').replace('m', '000000')
            
        try:
            # Numeric comparisons
            if '-' in s_rule:
                low, high = map(float, s_rule.split('-'))
                return low <= float(value) <= high
            elif '>' in s_rule:
                limit = float(s_rule.replace('>', ''))
                return float(value) > limit
            elif '<' in s_rule:
                limit = float(s_rule.replace('<', ''))
                return float(value) < limit
            else:
                # Try numeric exact match
                return float(value) == float(s_rule)
        except ValueError:
            # String exact match (e.g., "manual", "immediate")
            return s_val == s_rule

    def get_fee_rule(tx, rules):
        m_name = tx['merchant']
        m_attr = merchant_attrs.get(m_name, {})
        m_stat = merchant_stats.get(m_name, {})
        
        # Transaction-specifics
        tx_aci = tx['aci']
        is_intra = (tx['issuing_country'] == tx['acquirer_country'])
        
        for rule in rules:
            # 1. Scheme & Credit (Already filtered, but double check)
            if rule.get('is_credit') is not None and rule['is_credit'] != True:
                continue
                
            # 2. Account Type
            if rule.get('account_type') and m_attr.get('account_type') not in rule['account_type']:
                continue
                
            # 3. MCC
            if rule.get('merchant_category_code') and m_attr.get('merchant_category_code') not in rule['merchant_category_code']:
                continue
                
            # 4. ACI
            if rule.get('aci') and tx_aci not in rule['aci']:
                continue
                
            # 5. Intracountry
            # rule['intracountry'] can be 0.0 (False), 1.0 (True), or None
            if rule.get('intracountry') is not None:
                rule_intra = bool(rule['intracountry'])
                if rule_intra != is_intra:
                    continue
            
            # 6. Capture Delay
            if rule.get('capture_delay'):
                if not parse_check(m_attr.get('capture_delay'), rule['capture_delay']):
                    continue
                    
            # 7. Monthly Volume
            if rule.get('monthly_volume'):
                if not parse_check(m_stat.get('monthly_volume'), rule['monthly_volume']):
                    continue
                    
            # 8. Monthly Fraud Level
            if rule.get('monthly_fraud_level'):
                if not parse_check(m_stat.get('fraud_rate'), rule['monthly_fraud_level']):
                    continue
            
            # Match found
            return rule
            
        return None

    # 6. Calculate Fees
    # Filter rules for TransactPlus to speed up
    tp_rules = [r for r in fees_data if r['card_scheme'] == 'TransactPlus']
    
    fees_values = []
    
    for _, tx in target_txs.iterrows():
        rule = get_fee_rule(tx, tp_rules)
        if rule:
            # Fee Formula: fixed + (rate * amount / 10000)
            # Amount is fixed at 1000 EUR per question
            amount = 1000.0
            fee = rule['fixed_amount'] + (rule['rate'] * amount / 10000.0)
            fees_values.append(fee)
            
    # 7. Output Result
    if fees_values:
        avg_fee = np.mean(fees_values)
        print(f"{avg_fee:.14f}")
    else:
        print("No applicable fee rules found for the transactions.")

if __name__ == "__main__":
    calculate_transactplus_avg_fee()

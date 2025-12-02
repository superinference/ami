# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1700
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: sufficient
# Plan steps: 1
# Code length: 7570 characters (FULL CODE)
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

def execute_step():
    # Paths
    payments_path = '/output/chunk2/data/context/payments.csv'
    fees_path = '/output/chunk2/data/context/fees.json'
    merchant_data_path = '/output/chunk2/data/context/merchant_data.json'

    # 1. Load Data
    df_payments = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_data_path, 'r') as f:
        merchant_list = json.load(f)

    # 2. Filter for Merchant and Date
    merchant_name = "Martinis_Fine_Steakhouse"
    target_year = 2023
    target_day = 12

    # Get Merchant Metadata
    merchant_meta = next((item for item in merchant_list if item["merchant"] == merchant_name), None)
    if not merchant_meta:
        print(f"Merchant {merchant_name} not found.")
        return

    mcc = merchant_meta['merchant_category_code']
    account_type = merchant_meta['account_type']
    capture_delay = merchant_meta['capture_delay']

    # 3. Calculate Monthly Stats (Volume and Fraud) for January
    # Day 12 is in January. Manual says "natural months".
    # We use days 1-31 for January.
    jan_mask = (df_payments['merchant'] == merchant_name) & \
               (df_payments['year'] == target_year) & \
               (df_payments['day_of_year'] >= 1) & \
               (df_payments['day_of_year'] <= 31)

    df_jan = df_payments[jan_mask]

    monthly_volume = df_jan['eur_amount'].sum()
    fraud_volume = df_jan[df_jan['has_fraudulent_dispute']]['eur_amount'].sum()
    monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

    # 4. Identify Unique Transactions for Day 12
    day_mask = (df_payments['merchant'] == merchant_name) & \
               (df_payments['year'] == target_year) & \
               (df_payments['day_of_year'] == target_day)

    df_day = df_payments[day_mask]

    # Attributes needed for fee matching
    unique_txs = df_day[['card_scheme', 'is_credit', 'aci', 'issuing_country', 'acquirer_country']].drop_duplicates()

    # 5. Fee Matching Logic
    def parse_range(val_str):
        if not val_str: return None
        val_str = str(val_str).strip()
        
        is_percent = '%' in val_str
        clean_str = val_str.replace('%', '')
        
        def parse_num(s):
            s = s.lower()
            mult = 1
            if 'k' in s:
                mult = 1000
                s = s.replace('k', '')
            elif 'm' in s:
                mult = 1000000
                s = s.replace('m', '')
            try:
                return float(s) * mult
            except:
                return 0.0

        if '-' in clean_str:
            low, high = clean_str.split('-')
            l = parse_num(low)
            h = parse_num(high)
            if is_percent: l /= 100; h /= 100
            return (l, h)
        elif '<' in clean_str:
            val = parse_num(clean_str.replace('<', ''))
            if is_percent: val /= 100
            return (float('-inf'), val)
        elif '>' in clean_str:
            val = parse_num(clean_str.replace('>', ''))
            if is_percent: val /= 100
            return (val, float('inf'))
        else:
            val = parse_num(clean_str)
            if is_percent: val /= 100
            return (val, val)

    def check_rule(rule, tx_row, merch_mcc, merch_acct, merch_delay, m_vol, m_fraud):
        # 1. Card Scheme
        if rule['card_scheme'] != tx_row['card_scheme']:
            return False
            
        # 2. Account Type (List) - Empty list means ALL
        if rule['account_type'] and merch_acct not in rule['account_type']:
            return False
            
        # 3. MCC (List) - Empty list means ALL
        if rule['merchant_category_code'] and merch_mcc not in rule['merchant_category_code']:
            return False
            
        # 4. Is Credit (Bool)
        if rule['is_credit'] is not None and rule['is_credit'] != tx_row['is_credit']:
            return False
            
        # 5. ACI (List) - Empty list means ALL
        if rule['aci'] and tx_row['aci'] not in rule['aci']:
            return False
            
        # 6. Intracountry (Bool/Float)
        is_intra = (tx_row['issuing_country'] == tx_row['acquirer_country'])
        if rule['intracountry'] is not None:
            rule_intra = bool(rule['intracountry'])
            if rule_intra != is_intra:
                return False
                
        # 7. Capture Delay
        if rule['capture_delay']:
            rd = rule['capture_delay']
            md = str(merch_delay)
            match = False
            if rd == md: match = True
            elif rd == 'immediate' and md == 'immediate': match = True
            elif rd == 'manual' and md == 'manual': match = True
            elif rd == '<3' and md.isdigit() and float(md) < 3: match = True
            elif rd == '>5' and md.isdigit() and float(md) > 5: match = True
            elif rd == '3-5' and md.isdigit() and 3 <= float(md) <= 5: match = True
            if not match: return False

        # 8. Monthly Volume
        if rule['monthly_volume']:
            low, high = parse_range(rule['monthly_volume'])
            if not (low <= m_vol <= high):
                return False
                
        # 9. Monthly Fraud Level
        if rule['monthly_fraud_level']:
            low, high = parse_range(rule['monthly_fraud_level'])
            if not (low <= m_fraud <= high):
                return False
                
        return True

    # 6. Iterate and Collect IDs
    applicable_fee_ids = set()

    for _, tx in unique_txs.iterrows():
        for rule in fees_data:
            if check_rule(rule, tx, mcc, account_type, capture_delay, monthly_volume, monthly_fraud_rate):
                applicable_fee_ids.add(rule['ID'])

    # 7. Output
    print(sorted(list(applicable_fee_ids)))

if __name__ == "__main__":
    execute_step()

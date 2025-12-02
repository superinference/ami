# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2490
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7774 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas, k/m suffixes to float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v.lower():
            return float(v.lower().replace('k', '')) * 1000
        if 'm' in v.lower():
            return float(v.lower().replace('m', '')) * 1000000
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value)

def parse_range(range_str):
    """Parses strings like '100k-1m', '<3', '>5', '7.7%-8.3%' into (min, max)."""
    if not range_str:
        return None, None
    
    s = str(range_str).strip()
    
    # Handle comparisons
    if s.startswith('<'):
        return 0, coerce_to_float(s[1:])
    if s.startswith('>'):
        return coerce_to_float(s[1:]), float('inf')
        
    # Handle ranges
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            return coerce_to_float(parts[0]), coerce_to_float(parts[1])
            
    # Handle exact numeric values disguised as strings
    try:
        val = coerce_to_float(s)
        return val, val
    except:
        return None, None

def check_numeric_match(value, rule_value_str):
    """Checks if a numeric value fits in the rule's range string."""
    if not rule_value_str:
        return True # Wildcard
        
    min_val, max_val = parse_range(rule_value_str)
    if min_val is None and max_val is None:
        # Fallback for non-range strings
        return str(value) == str(rule_value_str)
        
    return min_val <= value <= max_val

def check_categorical_match(value, rule_value):
    """Checks if value matches rule (list, single value, or None)."""
    if rule_value is None:
        return True
    if isinstance(rule_value, list):
        if not rule_value: # Empty list = wildcard
            return True
        return value in rule_value
    return value == rule_value

def check_capture_delay(merchant_val, rule_val):
    """Handles capture_delay matching (strings vs numeric ranges)."""
    if rule_val is None:
        return True
    
    # Exact string match (handles 'manual', 'immediate')
    if str(merchant_val) == str(rule_val):
        return True
        
    # If merchant value is numeric (e.g. '7'), check against rule range (e.g. '>5')
    try:
        m_float = float(merchant_val)
        return check_numeric_match(m_float, rule_val)
    except ValueError:
        # Merchant val is non-numeric (e.g. 'manual') but rule is numeric/range -> False
        return False

def main():
    # 1. Load Data
    payments_path = '/output/chunk6/data/context/payments.csv'
    fees_path = '/output/chunk6/data/context/fees.json'
    merchant_data_path = '/output/chunk6/data/context/merchant_data.json'
    
    print("Loading data...")
    df_payments = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_data_path, 'r') as f:
        merchant_data = json.load(f)
        
    # 2. Setup Context
    target_merchant = 'Crossfit_Hanna'
    target_fee_id = 792
    target_year = 2023
    new_rate = 99
    
    # 3. Get Merchant Metadata
    merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print(f"Error: Merchant {target_merchant} not found.")
        return

    # 4. Get Fee Rule
    fee_rule = next((f for f in fees_data if f['ID'] == target_fee_id), None)
    if not fee_rule:
        print(f"Error: Fee ID {target_fee_id} not found.")
        return
    
    old_rate = fee_rule['rate']
    print(f"Fee {target_fee_id} found. Old Rate: {old_rate}, New Rate: {new_rate}")
    
    # 5. Filter Transactions (Merchant + Year)
    # Optimization: Filter early to reduce processing
    df = df_payments[(df_payments['merchant'] == target_merchant) & (df_payments['year'] == target_year)].copy()
    print(f"Processing {len(df)} transactions for {target_merchant} in {target_year}...")
    
    # 6. Calculate Monthly Stats for the Merchant (Volume & Fraud)
    # Create a date column to extract month (2023 is non-leap)
    df['date'] = pd.to_datetime(df['year'] * 1000 + df['day_of_year'], format='%Y%j')
    df['month'] = df['date'].dt.month
    
    # Calculate monthly stats
    monthly_stats = {}
    for month in range(1, 13):
        month_txs = df[df['month'] == month]
        if month_txs.empty:
            monthly_stats[month] = {'vol': 0.0, 'fraud_rate': 0.0}
            continue
            
        total_vol = month_txs['eur_amount'].sum()
        # Fraud rate = Fraud Volume / Total Volume (per Manual Section 7)
        fraud_vol = month_txs[month_txs['has_fraudulent_dispute']]['eur_amount'].sum()
        fraud_rate = (fraud_vol / total_vol) if total_vol > 0 else 0.0
        
        monthly_stats[month] = {'vol': total_vol, 'fraud_rate': fraud_rate}

    # 7. Filter Matching Transactions
    affected_amount = 0.0
    match_count = 0
    
    for idx, row in df.iterrows():
        # --- Static Matches (Merchant/Fee attributes) ---
        
        # Card Scheme
        if not check_categorical_match(row['card_scheme'], fee_rule.get('card_scheme')):
            continue
            
        # Account Type (Merchant Attribute)
        if not check_categorical_match(merchant_info['account_type'], fee_rule.get('account_type')):
            continue
            
        # MCC (Merchant Attribute)
        if not check_categorical_match(merchant_info['merchant_category_code'], fee_rule.get('merchant_category_code')):
            continue
            
        # Capture Delay (Merchant Attribute vs Rule)
        if not check_capture_delay(merchant_info['capture_delay'], fee_rule.get('capture_delay')):
            continue
                 
        # --- Transaction Matches ---
        
        # Is Credit
        if fee_rule.get('is_credit') is not None:
            if row['is_credit'] != fee_rule['is_credit']:
                continue
                
        # ACI
        if not check_categorical_match(row['aci'], fee_rule.get('aci')):
            continue
            
        # Intracountry
        if fee_rule.get('intracountry') is not None:
            is_intra = (row['issuing_country'] == row['acquirer_country'])
            # Handle 0.0/1.0/True/False in JSON
            rule_intra_bool = bool(fee_rule['intracountry'])
            if is_intra != rule_intra_bool:
                continue
                
        # --- Dynamic Matches (Monthly Stats) ---
        month = row['month']
        stats = monthly_stats.get(month)
        
        # Monthly Volume
        if fee_rule.get('monthly_volume'):
            if not check_numeric_match(stats['vol'], fee_rule['monthly_volume']):
                continue
                
        # Monthly Fraud Level
        if fee_rule.get('monthly_fraud_level'):
            if not check_numeric_match(stats['fraud_rate'], fee_rule['monthly_fraud_level']):
                continue
                
        # If all passed:
        affected_amount += row['eur_amount']
        match_count += 1

    # 8. Calculate Delta
    # Fee Formula: fee = fixed + (rate * amount / 10000)
    # Delta = (New_Rate - Old_Rate) * Amount / 10000
    delta = (new_rate - old_rate) * affected_amount / 10000
    
    print(f"Matching Transactions: {match_count}")
    print(f"Affected Volume: {affected_amount:.2f}")
    print(f"Delta: {delta:.14f}")

if __name__ == "__main__":
    main()

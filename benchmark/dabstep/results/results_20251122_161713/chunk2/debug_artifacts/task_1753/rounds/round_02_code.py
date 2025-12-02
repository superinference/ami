# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1753
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8933 characters (FULL CODE)
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

# --- Custom Logic for Fee Matching ---

def parse_value_for_matching(val_str):
    """Parse a single value string into a float, handling k, m, %."""
    if isinstance(val_str, (int, float)):
        return float(val_str)
    
    s = str(val_str).strip().lower()
    factor = 1.0
    
    if '%' in s:
        factor = 0.01
        s = s.replace('%', '')
    elif 'k' in s:
        factor = 1000.0
        s = s.replace('k', '')
    elif 'm' in s:
        factor = 1000000.0
        s = s.replace('m', '')
        
    # Remove operators for value extraction
    s = s.replace('>', '').replace('<', '').replace('=', '')
    
    try:
        return float(s) * factor
    except ValueError:
        return 0.0

def parse_range_bounds(range_str):
    """Parse a range string (e.g., '100k-1m', '>5', '3-5') into (min, max)."""
    if not range_str:
        return (float('-inf'), float('inf'))
    
    s = str(range_str).strip()
    
    # Handle > X
    if s.startswith('>'):
        val = parse_value_for_matching(s)
        return (val, float('inf')) # Treat > as inclusive for safety or strictly greater? 
                                   # Usually fee tiers are continuous. Let's assume inclusive boundary for lower bound.
    
    # Handle < X
    if s.startswith('<'):
        val = parse_value_for_matching(s)
        return (float('-inf'), val)
        
    # Handle X-Y
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            return (parse_value_for_matching(parts[0]), parse_value_for_matching(parts[1]))
            
    # Handle exact value (rare for ranges)
    val = parse_value_for_matching(s)
    return (val, val)

def check_range_match(value, range_str):
    """Check if a numeric value falls within the parsed range string."""
    if range_str is None:
        return True
    min_v, max_v = parse_range_bounds(range_str)
    # Handle floating point precision issues with epsilon if needed, but direct comparison usually fine for these scales
    return min_v <= value <= max_v

def match_capture_delay(merchant_val, rule_val):
    """Match merchant capture delay (e.g. '1', 'immediate') against rule (e.g. '<3', 'immediate')."""
    if rule_val is None:
        return True
    
    # Exact string match (e.g., 'immediate' == 'immediate', 'manual' == 'manual')
    if str(merchant_val).lower() == str(rule_val).lower():
        return True
        
    # Numeric matching
    # Convert merchant value to days (float)
    m_days = None
    if str(merchant_val).lower() == 'immediate':
        m_days = 0.0
    else:
        try:
            m_days = float(merchant_val)
        except ValueError:
            pass # 'manual' or other non-numeric
            
    if m_days is not None:
        min_v, max_v = parse_range_bounds(rule_val)
        return min_v <= m_days <= max_v
        
    return False

def solve():
    # 1. Load Data
    payments_path = '/output/chunk2/data/context/payments.csv'
    fees_path = '/output/chunk2/data/context/fees.json'
    merchant_data_path = '/output/chunk2/data/context/merchant_data.json'
    
    df_payments = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees = json.load(f)
    with open(merchant_data_path, 'r') as f:
        merchant_data = json.load(f)
        
    # 2. Filter for Merchant and Date Range (March 2023: Day 60-90)
    merchant_name = "Belles_cookbook_store"
    df_march = df_payments[
        (df_payments['merchant'] == merchant_name) &
        (df_payments['year'] == 2023) &
        (df_payments['day_of_year'] >= 60) &
        (df_payments['day_of_year'] <= 90)
    ].copy()
    
    if df_march.empty:
        print("No transactions found for this merchant in March 2023.")
        return

    # 3. Calculate Monthly Stats (Volume and Fraud Rate)
    # Volume in EUR
    monthly_vol = df_march['eur_amount'].sum()
    
    # Fraud Rate (Ratio: 0.0 to 1.0)
    # Note: Rules like '8.3%' are parsed to 0.083 by our helper.
    fraud_count = df_march['has_fraudulent_dispute'].sum()
    tx_count = len(df_march)
    fraud_rate = fraud_count / tx_count if tx_count > 0 else 0.0
    
    # 4. Get Merchant Static Attributes
    m_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
    if not m_info:
        print(f"Merchant {merchant_name} not found in merchant_data.json")
        return
        
    m_account_type = m_info.get('account_type')
    m_mcc = m_info.get('merchant_category_code')
    m_capture_delay = m_info.get('capture_delay')
    
    # 5. Identify Applicable Fee IDs
    # We iterate through unique transaction profiles to find all applicable rules
    applicable_ids = set()
    
    # Relevant columns for fee rules that vary per transaction
    cols = ['card_scheme', 'is_credit', 'aci', 'issuing_country', 'acquirer_country']
    
    # Get unique combinations present in the data
    profiles = df_march[cols].drop_duplicates().to_dict('records')
    
    for profile in profiles:
        # Determine intracountry status for this profile
        # True if issuing_country == acquirer_country
        is_intra = (profile['issuing_country'] == profile['acquirer_country'])
        
        for rule in fees:
            # --- Check 1: Card Scheme (Exact Match) ---
            if rule['card_scheme'] != profile['card_scheme']:
                continue
                
            # --- Check 2: Account Type (Merchant Attribute vs Rule List) ---
            # Wildcard: None or Empty List matches all
            if is_not_empty(rule['account_type']):
                if m_account_type not in rule['account_type']:
                    continue
            
            # --- Check 3: MCC (Merchant Attribute vs Rule List) ---
            if is_not_empty(rule['merchant_category_code']):
                if m_mcc not in rule['merchant_category_code']:
                    continue
                    
            # --- Check 4: Is Credit (Transaction Attribute vs Rule Bool) ---
            if rule['is_credit'] is not None:
                if rule['is_credit'] != profile['is_credit']:
                    continue
            
            # --- Check 5: ACI (Transaction Attribute vs Rule List) ---
            if is_not_empty(rule['aci']):
                if profile['aci'] not in rule['aci']:
                    continue
            
            # --- Check 6: Intracountry (Derived Attribute vs Rule Bool/Float) ---
            if rule['intracountry'] is not None:
                # JSON might have 0.0, 1.0, or boolean
                rule_intra_bool = bool(float(rule['intracountry']))
                if rule_intra_bool != is_intra:
                    continue
            
            # --- Check 7: Capture Delay (Merchant Attribute vs Rule Range/String) ---
            if not match_capture_delay(m_capture_delay, rule['capture_delay']):
                continue
                
            # --- Check 8: Monthly Volume (Calculated Stat vs Rule Range) ---
            if not check_range_match(monthly_vol, rule['monthly_volume']):
                continue
                
            # --- Check 9: Monthly Fraud Level (Calculated Stat vs Rule Range) ---
            if not check_range_match(fraud_rate, rule['monthly_fraud_level']):
                continue
            
            # If all checks pass, the rule is applicable
            applicable_ids.add(rule['ID'])
            
    # 6. Output Result
    sorted_ids = sorted(list(applicable_ids))
    print(sorted_ids)

if __name__ == "__main__":
    solve()

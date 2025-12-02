# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2524
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 4961 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json

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
        try:
            return float(v)
        except ValueError:
            return None
    return None

def get_capture_delay_bucket(val):
    """Maps raw capture delay values from merchant data to fee rule buckets."""
    val_str = str(val).lower().strip()
    if val_str == 'manual':
        return 'manual'
    if val_str == 'immediate':
        return 'immediate'
    
    # Try numeric conversion for days
    num_val = coerce_to_float(val)
    if num_val is not None:
        if num_val < 3:
            return '<3'
        elif 3 <= num_val <= 5:
            return '3-5'
        elif num_val > 5:
            return '>5'
    return None

def is_not_empty(array):
    """Check if array/list is not empty."""
    if array is None:
        return False
    if hasattr(array, 'size'):  # numpy array
        return array.size > 0
    try:
        return len(array) > 0
    except TypeError:
        return False

# 1. Load Data
fees_path = '/output/chunk2/data/context/fees.json'
merchant_path = '/output/chunk2/data/context/merchant_data.json'
payments_path = '/output/chunk2/data/context/payments.csv'

with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
df_payments = pd.read_csv(payments_path)

# 2. Get Fee 17 Criteria
fee_17 = next((f for f in fees if f['ID'] == 17), None)

if not fee_17:
    print("Fee ID 17 not found.")
else:
    # Extract specific criteria for Fee 17
    # Fee 17 Criteria (from analysis):
    # - card_scheme: "SwiftCharge"
    # - is_credit: True
    # - aci: ["A"]
    # - capture_delay: ">5"
    # - account_type: [] (Any)
    # - merchant_category_code: [] (Any)
    
    req_scheme = fee_17.get('card_scheme')
    req_credit = fee_17.get('is_credit')
    req_aci = fee_17.get('aci')
    req_delay = fee_17.get('capture_delay')
    req_mcc = fee_17.get('merchant_category_code')
    req_account = fee_17.get('account_type')

    # 3. Identify Merchants matching the static criteria (capture_delay, account_type, mcc)
    # We filter the merchant_data.json list first
    valid_merchants = []
    
    for m in merchant_data:
        m_name = m['merchant']
        
        # Check Capture Delay
        # Map merchant's specific delay (e.g., "7") to bucket (e.g., ">5")
        m_delay_raw = m.get('capture_delay')
        m_delay_bucket = get_capture_delay_bucket(m_delay_raw)
        
        if req_delay and m_delay_bucket != req_delay:
            continue # Skip if delay bucket doesn't match
            
        # Check Account Type (if fee has restriction)
        if is_not_empty(req_account):
            if m.get('account_type') not in req_account:
                continue

        # Check MCC (if fee has restriction)
        if is_not_empty(req_mcc):
            if m.get('merchant_category_code') not in req_mcc:
                continue
                
        # If we get here, merchant matches static criteria
        valid_merchants.append(m_name)

    # 4. Filter Transactions for 2023 matching Fee 17 transaction criteria
    # Criteria: Year=2023, Merchant in valid_merchants, Scheme, Credit, ACI
    
    # Start with Year and Merchant filter
    cond_year = df_payments['year'] == 2023
    cond_merchant = df_payments['merchant'].isin(valid_merchants)
    
    # Scheme Filter
    if req_scheme:
        cond_scheme = df_payments['card_scheme'] == req_scheme
    else:
        cond_scheme = pd.Series([True] * len(df_payments))
        
    # Credit Filter
    if req_credit is not None:
        cond_credit = df_payments['is_credit'] == req_credit
    else:
        cond_credit = pd.Series([True] * len(df_payments))
        
    # ACI Filter
    if is_not_empty(req_aci):
        cond_aci = df_payments['aci'].isin(req_aci)
    else:
        cond_aci = pd.Series([True] * len(df_payments))
        
    # Apply all filters
    final_filter = cond_year & cond_merchant & cond_scheme & cond_credit & cond_aci
    affected_txs = df_payments[final_filter]
    
    # 5. Get unique merchants
    affected_merchants_list = affected_txs['merchant'].unique().tolist()
    affected_merchants_list.sort()
    
    # Output result
    print(", ".join(affected_merchants_list))

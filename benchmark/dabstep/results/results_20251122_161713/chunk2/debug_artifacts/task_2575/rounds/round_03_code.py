# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2575
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: sufficient
# Plan steps: 2
# Code length: 6293 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import json
import pandas as pd
import numpy as np

# ═══════════════════════════════════════════════════════════
# Helper functions for robust data processing
# ═══════════════════════════════════════════════════════════

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

# ═══════════════════════════════════════════════════════════
# Main Analysis Script
# ═══════════════════════════════════════════════════════════

def analyze_fee_impact():
    # 1. Load Data
    try:
        fees_path = '/output/chunk2/data/context/fees.json'
        merchant_path = '/output/chunk2/data/context/merchant_data.json'
        payments_path = '/output/chunk2/data/context/payments.csv'

        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
        
        with open(merchant_path, 'r') as f:
            merchant_data_list = json.load(f)
            # Create a lookup dictionary for merchant data: name -> details
            merchant_lookup = {
                m['merchant']: {
                    'account_type': m.get('account_type'),
                    'merchant_category_code': m.get('merchant_category_code')
                } for m in merchant_data_list
            }

        df_payments = pd.read_csv(payments_path)
        
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Get Fee 787 and its criteria
    fee_787 = next((fee for fee in fees_data if fee['ID'] == 787), None)
    if not fee_787:
        print("Fee ID 787 not found.")
        return

    # print(f"Analyzing Fee ID: {fee_787['ID']}")
    # print(f"Original Criteria: {json.dumps(fee_787, indent=2)}")

    # 3. Filter Payments for 2023 and Static Criteria (Scheme, ACI, Credit)
    # These criteria do not change between the 'Old' and 'New' scenarios
    
    # Filter Year
    df_2023 = df_payments[df_payments['year'] == 2023].copy()
    
    # Filter Card Scheme
    if fee_787.get('card_scheme'):
        df_2023 = df_2023[df_2023['card_scheme'] == fee_787['card_scheme']]
    
    # Filter is_credit (Handle boolean explicitly)
    if fee_787.get('is_credit') is not None:
        # Ensure column is boolean for comparison
        is_credit_param = fee_787['is_credit']
        df_2023 = df_2023[df_2023['is_credit'] == is_credit_param]

    # Filter ACI (List inclusion)
    # Fee rule 'aci' is a list of allowed values. Transaction 'aci' is a single value.
    if is_not_empty(fee_787.get('aci')):
        df_2023 = df_2023[df_2023['aci'].isin(fee_787['aci'])]

    # 4. Identify Affected Merchants
    # "Affected" means the fee application status changes (Applied -> Not Applied OR Not Applied -> Applied)
    
    affected_merchants = []
    
    # Get unique merchants who have transactions matching the static criteria
    candidate_merchants = df_2023['merchant'].unique()
    
    # print(f"\nChecking {len(candidate_merchants)} candidate merchants with matching transactions...")

    for merchant_name in candidate_merchants:
        merchant_info = merchant_lookup.get(merchant_name)
        if not merchant_info:
            continue
            
        # Check MCC Match (Static for both scenarios)
        # Fee MCC is a list, Merchant MCC is an int
        fee_mccs = fee_787.get('merchant_category_code')
        merchant_mcc = merchant_info['merchant_category_code']
        
        mcc_match = False
        if not is_not_empty(fee_mccs): # Wildcard matches all
            mcc_match = True
        elif merchant_mcc in fee_mccs:
            mcc_match = True
            
        if not mcc_match:
            continue # Doesn't match basic criteria, so never applied -> not affected
            
        # Check Account Type Match (The Variable)
        merchant_acct_type = merchant_info['account_type']
        
        # Scenario A: Original Rule
        # Matches if fee['account_type'] is empty OR merchant_acct_type is in it
        original_fee_acct_types = fee_787.get('account_type')
        matches_original = False
        if not is_not_empty(original_fee_acct_types):
            matches_original = True # Wildcard
        elif merchant_acct_type in original_fee_acct_types:
            matches_original = True
            
        # Scenario B: New Rule (Only applied to account type 'S')
        matches_new = (merchant_acct_type == 'S')
        
        # Determine if Affected
        # Affected if the application status is different
        if matches_original != matches_new:
            affected_merchants.append(merchant_name)
            # Debug info
            # print(f"  Merchant: {merchant_name} (Type: {merchant_acct_type}) -> Old: {matches_original}, New: {matches_new} -> AFFECTED")

    # 5. Output Results
    if affected_merchants:
        # Sort for consistent output
        affected_merchants.sort()
        # print("\nMerchants affected by the change:")
        print(", ".join(affected_merchants))
    else:
        print("No merchants would be affected by this change.")

if __name__ == "__main__":
    analyze_fee_impact()

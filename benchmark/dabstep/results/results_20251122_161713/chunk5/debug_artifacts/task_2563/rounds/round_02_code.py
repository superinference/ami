# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2563
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 5252 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import json
import pandas as pd

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

# Main Execution
try:
    # 1. Load Data
    fees_path = '/output/chunk5/data/context/fees.json'
    merchant_path = '/output/chunk5/data/context/merchant_data.json'
    payments_path = '/output/chunk5/data/context/payments.csv'

    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)
        
    df_payments = pd.read_csv(payments_path)

    # 2. Get Fee 384
    fee_384 = next((fee for fee in fees_data if fee['ID'] == 384), None)
    if not fee_384:
        print("Fee 384 not found.")
        exit()

    # 3. Map Merchant Data for easy lookup
    # Map merchant name -> {account_type, mcc}
    merchant_lookup = {m['merchant']: m for m in merchant_data}

    # 4. Filter Payments based on Fee 384's STATIC criteria
    # We filter for transactions that match the fee's characteristics (Scheme, Credit, ACI, Intracountry)
    # This gives us the pool of transactions where this fee *could* apply based on transaction attributes.
    
    df_filtered = df_payments.copy()

    # Filter by Card Scheme
    if fee_384.get('card_scheme'):
        df_filtered = df_filtered[df_filtered['card_scheme'] == fee_384['card_scheme']]

    # Filter by is_credit
    if fee_384.get('is_credit') is not None:
        df_filtered = df_filtered[df_filtered['is_credit'] == fee_384['is_credit']]

    # Filter by ACI
    if is_not_empty(fee_384.get('aci')):
        df_filtered = df_filtered[df_filtered['aci'].isin(fee_384['aci'])]

    # Filter by Intracountry
    if fee_384.get('intracountry') is not None:
        is_intra = fee_384['intracountry']
        # 1.0/True = Domestic (Issuing == Acquirer)
        # 0.0/False = International (Issuing != Acquirer)
        if is_intra: 
            df_filtered = df_filtered[df_filtered['issuing_country'] == df_filtered['acquirer_country']]
        else:
            df_filtered = df_filtered[df_filtered['issuing_country'] != df_filtered['acquirer_country']]

    # 5. Identify Merchants and Check "Affected" Status
    # Get unique merchants from the filtered transactions
    potential_merchants = df_filtered['merchant'].unique()
    
    affected_merchants = []

    for merchant_name in potential_merchants:
        m_info = merchant_lookup.get(merchant_name)
        if not m_info:
            continue
            
        # Check MCC match (Static criteria)
        fee_mcc = fee_384.get('merchant_category_code')
        merchant_mcc = m_info.get('merchant_category_code')
        
        if is_not_empty(fee_mcc):
            if merchant_mcc not in fee_mcc:
                continue # Merchant doesn't match MCC, so they never used this fee.

        # At this point, the merchant matches all transaction/merchant criteria EXCEPT account_type.
        
        # Check Account Type Change
        current_account_types = fee_384.get('account_type') # List or None
        merchant_account_type = m_info.get('account_type')
        
        # Does it match CURRENTLY?
        # Match if list is empty (wildcard) OR merchant type is in list
        matches_current = (not is_not_empty(current_account_types)) or (merchant_account_type in current_account_types)
        
        # Does it match PROPOSED (Only 'S')?
        matches_proposed = (merchant_account_type == 'S')
        
        # Is it affected? (Status changes)
        # Affected if they matched before but not now, OR didn't match before but do now.
        if matches_current != matches_proposed:
            affected_merchants.append(merchant_name)

    # 6. Output Result
    if affected_merchants:
        affected_merchants.sort()
        # The question asks "which merchants". Returning the list.
        print(", ".join(affected_merchants))
    else:
        print("No merchants would be affected.")

except Exception as e:
    print(f"Error: {e}")

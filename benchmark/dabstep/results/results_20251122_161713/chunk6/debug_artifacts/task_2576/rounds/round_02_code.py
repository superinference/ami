# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2576
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 5123 characters (FULL CODE)
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

# --- Helper Functions ---
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

# --- Load Data ---
fees_path = '/output/chunk6/data/context/fees.json'
merchants_path = '/output/chunk6/data/context/merchant_data.json'
payments_path = '/output/chunk6/data/context/payments.csv'

with open(fees_path, 'r') as f:
    fees_data = json.load(f)

with open(merchants_path, 'r') as f:
    merchant_data = json.load(f)

# Create merchant lookup for fast access to attributes
# Map: merchant_name -> {account_type, merchant_category_code}
merchant_lookup = {
    m['merchant']: {
        'account_type': m['account_type'],
        'merchant_category_code': m['merchant_category_code']
    } 
    for m in merchant_data
}

# --- Step 1: Get Fee Rule 787 ---
fee_rule = next((item for item in fees_data if item["ID"] == 787), None)

if not fee_rule:
    print("Fee rule with ID 787 not found.")
else:
    # --- Step 2: Filter Transactions (2023) ---
    df = pd.read_csv(payments_path)
    df = df[df['year'] == 2023]

    # Apply Transaction-Level Filters from Fee 787
    
    # Filter by Card Scheme
    if fee_rule.get('card_scheme'):
        df = df[df['card_scheme'] == fee_rule['card_scheme']]
    
    # Filter by Credit/Debit status (if specified)
    if fee_rule.get('is_credit') is not None:
        df = df[df['is_credit'] == fee_rule['is_credit']]
        
    # Filter by ACI (if specified in rule)
    if is_not_empty(fee_rule.get('aci')):
        df = df[df['aci'].isin(fee_rule['aci'])]

    # Get list of merchants who have matching transactions
    potential_merchants = df['merchant'].unique()
    
    affected_merchants = []

    # --- Step 3: Check Merchant-Level Criteria ---
    for merchant in potential_merchants:
        m_info = merchant_lookup.get(merchant)
        if not m_info:
            continue
            
        # Check 1: Merchant Category Code (MCC)
        # If the fee rule specifies MCCs, the merchant must match one of them
        if is_not_empty(fee_rule.get('merchant_category_code')):
            if m_info['merchant_category_code'] not in fee_rule['merchant_category_code']:
                continue # Merchant doesn't match original rule
        
        # Check 2: Original Account Type
        # If the fee rule ALREADY had account type restrictions, the merchant must match them first
        if is_not_empty(fee_rule.get('account_type')):
            if m_info['account_type'] not in fee_rule['account_type']:
                continue # Merchant doesn't match original rule
        
        # --- Step 4: Determine if Affected ---
        # The merchant matches the ORIGINAL rule.
        # The hypothetical change is: Fee 787 is ONLY applied to account type 'O'.
        # Merchants are "affected" if they matched the old rule but DO NOT match the new rule.
        # i.e., Their account type is NOT 'O'.
        
        if m_info['account_type'] != 'O':
            affected_merchants.append(merchant)

    # --- Output Results ---
    if not affected_merchants:
        print("No merchants would be affected.")
    else:
        # Sort alphabetically for clean output
        affected_merchants.sort()
        print(", ".join(affected_merchants))

import pandas as pd
import json

# --- Helper Functions ---
def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        return float(v)
    return float(value)

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

def safe_get(df, column, default=None):
    """Safely get column from DataFrame, return default if not exists."""
    if isinstance(df, dict):
        return df.get(column, default)
    elif hasattr(df, 'columns') and column in df.columns:
        return df[column]
    return default

# --- Load Data ---
fees_path = '/output/chunk6/data/context/fees.json'
merchants_path = '/output/chunk6/data/context/merchant_data.json'
payments_path = '/output/chunk6/data/context/payments.csv'

with open(fees_path, 'r') as f:
    fees_data = json.load(f)

with open(merchants_path, 'r') as f:
    merchant_data = json.load(f)

# Create merchant lookup
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
    # We need to find transactions that match the *invariant* parts of the rule
    # (Scheme, Credit/Debit, ACI, MCC, etc.)
    # The only thing changing is the Account Type logic.
    
    df = pd.read_csv(payments_path)
    df = df[df['year'] == 2023]

    # 2a. Filter by Card Scheme
    if fee_rule.get('card_scheme'):
        df = df[df['card_scheme'] == fee_rule['card_scheme']]
    
    # 2b. Filter by Credit/Debit
    if fee_rule.get('is_credit') is not None:
        df = df[df['is_credit'] == fee_rule['is_credit']]
        
    # 2c. Filter by ACI (if specified in rule)
    # Note: fee_rule['aci'] is a list of allowed values.
    if is_not_empty(fee_rule.get('aci')):
        df = df[df['aci'].isin(fee_rule['aci'])]

    # 2d. Filter by Merchant Category Code (if specified in rule)
    # This requires joining with merchant data or filtering later. 
    # Doing it later is safer as MCC is a merchant property, not just transaction.
    
    # Get potential merchants from the filtered transactions
    potential_merchants = df['merchant'].unique()
    
    affected_merchants = []

    # --- Step 3: Check Merchant-Level Criteria ---
    for merchant in potential_merchants:
        m_info = merchant_lookup.get(merchant)
        if not m_info:
            continue
            
        # Check MCC (Invariant part of the rule)
        if is_not_empty(fee_rule.get('merchant_category_code')):
            if m_info['merchant_category_code'] not in fee_rule['merchant_category_code']:
                continue # Merchant doesn't match the basic criteria of the fee
        
        # --- Step 4: Determine "Affected" Status ---
        # Original Rule Logic:
        # Applies if merchant's account_type is in fee_rule['account_type']
        # (If fee_rule['account_type'] is empty/null, it applies to ALL, but usually it's specific)
        
        original_account_types = fee_rule.get('account_type', [])
        # Handle wildcard: if list is empty, it applies to ALL types
        original_applies = False
        if not is_not_empty(original_account_types):
            original_applies = True
        else:
            if m_info['account_type'] in original_account_types:
                original_applies = True
                
        # New Rule Logic:
        # Applies ONLY if account_type is 'O'
        new_applies = (m_info['account_type'] == 'O')
        
        # A merchant is affected if the application status changes
        if original_applies != new_applies:
            affected_merchants.append(merchant)

    # --- Output Results ---
    if not affected_merchants:
        print("No merchants would be affected.")
    else:
        # Sort alphabetically for clean output
        affected_merchants.sort()
        print(", ".join(affected_merchants))
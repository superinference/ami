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
    """Check if array/list is not empty."""
    if array is None:
        return False
    if hasattr(array, 'size'):
        return array.size > 0
    try:
        return len(array) > 0
    except TypeError:
        return False

# --- Main Analysis ---

# 1. Load Data
fees_path = '/output/chunk2/data/context/fees.json'
merchant_path = '/output/chunk2/data/context/merchant_data.json'
payments_path = '/output/chunk2/data/context/payments.csv'

try:
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)
        
    df_payments = pd.read_csv(payments_path)

    # 2. Get Fee ID 64 Criteria
    fee_rule = next((item for item in fees_data if item["ID"] == 64), None)
    
    if not fee_rule:
        print("Error: Fee ID 64 not found.")
    else:
        # 3. Create Merchant Lookup Map (Name -> {MCC, Account Type})
        # We need this to check if a merchant matches the fee's MCC rule and the hypothetical Account Type rule
        merchant_map = {}
        for m in merchant_data:
            merchant_map[m['merchant']] = {
                'mcc': m['merchant_category_code'],
                'account_type': m['account_type']
            }

        # 4. Filter Transactions for 2023 matching Fee 64's criteria
        # Start with all 2023 transactions
        df_filtered = df_payments[df_payments['year'] == 2023].copy()

        # Apply Card Scheme filter
        if fee_rule.get('card_scheme'):
            df_filtered = df_filtered[df_filtered['card_scheme'] == fee_rule['card_scheme']]

        # Apply 'is_credit' filter (handle boolean/null)
        # In fees.json, is_credit can be True, False, or null (wildcard)
        if fee_rule.get('is_credit') is not None:
            df_filtered = df_filtered[df_filtered['is_credit'] == fee_rule['is_credit']]

        # Apply ACI filter (list in fee rule, single value in transaction)
        # If fee_rule['aci'] is not empty, transaction 'aci' must be in that list
        if is_not_empty(fee_rule.get('aci')):
            df_filtered = df_filtered[df_filtered['aci'].isin(fee_rule['aci'])]

        # Apply Merchant Category Code (MCC) filter
        # MCC is not in payments.csv, so we filter by merchants who have the valid MCCs
        if is_not_empty(fee_rule.get('merchant_category_code')):
            valid_mccs = set(fee_rule['merchant_category_code'])
            # Identify merchants whose MCC is in the valid list
            valid_mcc_merchants = [
                name for name, data in merchant_map.items() 
                if data['mcc'] in valid_mccs
            ]
            df_filtered = df_filtered[df_filtered['merchant'].isin(valid_mcc_merchants)]

        # 5. Identify Merchants currently subject to Fee 64
        # These are the merchants who processed transactions matching the rule
        current_merchants = df_filtered['merchant'].unique()

        # 6. Determine Affected Merchants
        # The hypothetical change: Fee 64 ONLY applies if account_type == 'D'.
        # Affected = Currently paying (in current_merchants) BUT account_type != 'D'.
        affected_merchants = []
        
        for merchant in current_merchants:
            # Safely get account type, default to None if missing
            acct_type = merchant_map.get(merchant, {}).get('account_type')
            
            # If account type is NOT 'D', they are affected (they lose the fee rule)
            if acct_type != 'D':
                affected_merchants.append(merchant)

        # 7. Output Results
        if affected_merchants:
            # Sort for consistent output
            affected_merchants.sort()
            print(", ".join(affected_merchants))
        else:
            print("No merchants would be affected.")

except Exception as e:
    print(f"An error occurred: {e}")
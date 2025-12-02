import pandas as pd
import json

# Helper functions for robust data processing
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

def safe_get(df, column, default=None):
    if isinstance(df, dict):
        return df.get(column, default)
    elif hasattr(df, 'columns') and column in df.columns:
        return df[column]
    return default

def is_not_empty(array):
    if array is None:
        return False
    if hasattr(array, 'size'):
        return array.size > 0
    try:
        return len(array) > 0
    except TypeError:
        return False

# --- Step 1: Load Data ---
try:
    fees_path = '/output/chunk3/data/context/fees.json'
    merchant_path = '/output/chunk3/data/context/merchant_data.json'
    payments_path = '/output/chunk3/data/context/payments.csv'

    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)
        
    df_payments = pd.read_csv(payments_path)

    # --- Step 2: Get Fee 64 Criteria ---
    fee_rule_64 = next((item for item in fees_data if item["ID"] == 64), None)
    
    if not fee_rule_64:
        print("Fee rule with ID 64 not found.")
    else:
        # Extract criteria
        rule_scheme = fee_rule_64.get('card_scheme')
        rule_is_credit = fee_rule_64.get('is_credit')
        rule_aci = fee_rule_64.get('aci') # List or None
        rule_mcc = fee_rule_64.get('merchant_category_code') # List or None
        rule_account_type_original = fee_rule_64.get('account_type') # Should be empty/wildcard currently

        # --- Step 3: Prepare Merchant Mappings ---
        # Map merchant name to Account Type and MCC
        merchant_account_map = {m['merchant']: m['account_type'] for m in merchant_data}
        merchant_mcc_map = {m['merchant']: m['merchant_category_code'] for m in merchant_data}

        # --- Step 4: Filter Transactions that CURRENTLY match Fee 64 ---
        # We need to find who is using it NOW to see who would lose it.
        
        # Filter by Card Scheme
        if rule_scheme:
            df_filtered = df_payments[df_payments['card_scheme'] == rule_scheme]
        else:
            df_filtered = df_payments.copy()

        # Filter by Credit Status
        if rule_is_credit is not None:
            df_filtered = df_filtered[df_filtered['is_credit'] == rule_is_credit]

        # Filter by ACI
        # Note: rule_aci is a list of allowed values (e.g., ['D']). Payment has single value.
        if is_not_empty(rule_aci):
            df_filtered = df_filtered[df_filtered['aci'].isin(rule_aci)]

        # Filter by Merchant Category Code (MCC)
        # We need to map the merchant on the transaction to their MCC
        if is_not_empty(rule_mcc):
            # Create a temporary MCC column for filtering
            df_filtered['temp_mcc'] = df_filtered['merchant'].map(merchant_mcc_map)
            df_filtered = df_filtered[df_filtered['temp_mcc'].isin(rule_mcc)]

        # Filter by Account Type (Original)
        # If the original rule had specific account types, we'd filter by them too.
        # Assuming original is wildcard (empty), so everyone matches this criteria initially.
        if is_not_empty(rule_account_type_original):
             df_filtered['temp_acct'] = df_filtered['merchant'].map(merchant_account_map)
             df_filtered = df_filtered[df_filtered['temp_acct'].isin(rule_account_type_original)]

        # Get list of merchants currently using this fee
        current_merchants = df_filtered['merchant'].unique()

        # --- Step 5: Identify Affected Merchants ---
        # Affected = Currently using Fee 64 AND Account Type is NOT 'R'
        affected_merchants = []
        
        for merchant in current_merchants:
            acct_type = merchant_account_map.get(merchant)
            if acct_type != 'R':
                affected_merchants.append(merchant)

        # --- Step 6: Output Results ---
        affected_merchants.sort()
        if affected_merchants:
            print(", ".join(affected_merchants))
        else:
            print("No merchants would be affected.")

except Exception as e:
    print(f"An error occurred: {e}")
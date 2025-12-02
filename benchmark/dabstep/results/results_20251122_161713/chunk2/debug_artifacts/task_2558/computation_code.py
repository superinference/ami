import pandas as pd
import json

# ═══════════════════════════════════════════════════════════
# Helper Functions
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

def match_fee_rule(transaction, rule):
    """
    Determines if a transaction matches a fee rule.
    Handles wildcards (empty lists/None) and specific field matching.
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != transaction.get('card_scheme'):
        return False
        
    # 2. Account Type (Wildcard: [] or None matches all)
    if is_not_empty(rule.get('account_type')):
        if transaction.get('account_type') not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (Wildcard: [] or None matches all)
    if is_not_empty(rule.get('merchant_category_code')):
        # Ensure types match (int vs str)
        tx_mcc = int(transaction.get('mcc', 0)) if transaction.get('mcc') else 0
        rule_mccs = [int(x) for x in rule['merchant_category_code']]
        if tx_mcc not in rule_mccs:
            return False
            
    # 4. Is Credit (Wildcard: None matches all)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != transaction.get('is_credit'):
            return False
            
    # 5. ACI (Wildcard: [] or None matches all)
    if is_not_empty(rule.get('aci')):
        if transaction.get('aci') not in rule['aci']:
            return False
            
    # 6. Intracountry (Wildcard: None matches all)
    if rule.get('intracountry') is not None:
        # Determine if transaction is intracountry
        is_intra = transaction.get('issuing_country') == transaction.get('acquirer_country')
        # Rule expects boolean or string representation
        rule_intra = rule['intracountry']
        if isinstance(rule_intra, str):
            rule_intra = (rule_intra.lower() == 'true' or rule_intra == '1.0')
        elif isinstance(rule_intra, float):
            rule_intra = (rule_intra == 1.0)
            
        if rule_intra != is_intra:
            return False
            
    return True

# ═══════════════════════════════════════════════════════════
# Main Analysis Script
# ═══════════════════════════════════════════════════════════

# Define file paths
payments_path = '/output/chunk2/data/context/payments.csv'
fees_path = '/output/chunk2/data/context/fees.json'
merchant_data_path = '/output/chunk2/data/context/merchant_data.json'

# 1. Load Data
try:
    df_payments = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_data_path, 'r') as f:
        merchant_data = json.load(f)
    df_merchants = pd.DataFrame(merchant_data)
except Exception as e:
    print(f"Error loading files: {e}")
    exit()

# 2. Get Fee ID 64
fee_64 = next((fee for fee in fees_data if fee['ID'] == 64), None)
if not fee_64:
    print("Fee ID 64 not found.")
    exit()

print(f"Analyzing Fee ID 64: {json.dumps(fee_64, indent=2)}")

# 3. Merge Merchant Data into Payments
# We need 'account_type' and 'merchant_category_code' (mapped to 'mcc') in the transaction data
# to correctly check if the fee currently applies.
df_merged = pd.merge(df_payments, df_merchants, on='merchant', how='left')
df_merged.rename(columns={'merchant_category_code': 'mcc'}, inplace=True)

# 4. Identify Transactions where Fee 64 CURRENTLY applies
# We iterate through unique combinations to speed up matching, or apply row-wise if needed.
# Given the dataset size, filtering by the main static criteria first is faster.

# Static filters based on Fee 64 definition (optimization)
# Fee 64: card_scheme='SwiftCharge', is_credit=True, aci=['D'] (based on typical data, but we use the loaded object)
filtered_txs = df_merged.copy()

if fee_64.get('card_scheme'):
    filtered_txs = filtered_txs[filtered_txs['card_scheme'] == fee_64['card_scheme']]

if fee_64.get('is_credit') is not None:
    filtered_txs = filtered_txs[filtered_txs['is_credit'] == fee_64['is_credit']]

# Apply full match_fee_rule to handle lists (aci, mcc, account_type) correctly
# This ensures we only look at transactions where Fee 64 ACTUALLY applies right now.
matching_indices = []
for idx, row in filtered_txs.iterrows():
    # Convert row to dict for the helper function
    tx_dict = row.to_dict()
    if match_fee_rule(tx_dict, fee_64):
        matching_indices.append(idx)

df_matching_fee_64 = filtered_txs.loc[matching_indices]

print(f"\nTransactions currently matching Fee 64: {len(df_matching_fee_64)}")
unique_merchants_using_fee_64 = df_matching_fee_64['merchant'].unique()
print(f"Merchants currently using Fee 64: {list(unique_merchants_using_fee_64)}")

# 5. Determine Affected Merchants
# Affected = Currently using Fee 64 AND Account Type is NOT 'O'
# If account type IS 'O', the new rule (only applied to 'O') still covers them, so they are NOT affected.
# If account type is NOT 'O', the new rule excludes them, so they ARE affected.

affected_merchants = []

for merchant in unique_merchants_using_fee_64:
    # Get merchant's account type
    merchant_info = df_merchants[df_merchants['merchant'] == merchant].iloc[0]
    account_type = merchant_info['account_type']
    
    if account_type != 'O':
        affected_merchants.append(merchant)

# 6. Output Results
print("\nAffected Merchants (Would lose Fee 64 status):")
# Sort for consistent output
affected_merchants.sort()
print(", ".join(affected_merchants))
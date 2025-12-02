import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# Helper Functions
# ═══════════════════════════════════════════════════════════

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
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value) if value is not None else 0.0

def is_match(transaction_value, rule_value):
    """
    Generic matcher for fee rules.
    rule_value: The constraint from the fee rule (can be value, list, or None).
    transaction_value: The actual value from the transaction/merchant.
    """
    # Wildcard (None or empty list) matches everything
    if rule_value is None:
        return True
    if isinstance(rule_value, list):
        if len(rule_value) == 0:
            return True
        return transaction_value in rule_value
    
    # Direct comparison for scalars (bool, str, int)
    # Handle boolean specifically to avoid 1.0 == True issues if types differ
    if isinstance(rule_value, bool) or isinstance(transaction_value, bool):
        return bool(rule_value) == bool(transaction_value)
        
    return rule_value == transaction_value

def get_fee_rule_by_id(fees_data, fee_id):
    for rule in fees_data:
        if rule['ID'] == fee_id:
            return rule
    return None

# ═══════════════════════════════════════════════════════════
# Main Execution
# ═══════════════════════════════════════════════════════════

# File paths
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
    print("Data loaded successfully.")
except FileNotFoundError as e:
    print(f"Error loading files: {e}")
    exit()

# 2. Get Merchant Attributes for 'Belles_cookbook_store'
target_merchant = 'Belles_cookbook_store'
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)

if not merchant_info:
    print(f"Merchant {target_merchant} not found in merchant_data.json")
    exit()

merchant_mcc = merchant_info['merchant_category_code']
merchant_account_type = merchant_info['account_type']

print(f"Merchant: {target_merchant}")
print(f"MCC: {merchant_mcc}")
print(f"Account Type: {merchant_account_type}")

# 3. Get Fee Rule ID=150
target_fee_id = 150
fee_rule = get_fee_rule_by_id(fees_data, target_fee_id)

if not fee_rule:
    print(f"Fee ID {target_fee_id} not found in fees.json")
    exit()

print(f"\nFee Rule {target_fee_id} Details:")
print(json.dumps(fee_rule, indent=2))

original_rate = fee_rule['rate']
new_rate = 99

# 4. Filter Transactions
# Filter for Merchant and Year first
df_filtered = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == 2023)
].copy()

print(f"\nTotal 2023 transactions for {target_merchant}: {len(df_filtered)}")

# 5. Identify Matching Transactions
# We need to check every transaction against the constraints of Fee 150.
# Constraints to check:
# - card_scheme
# - is_credit
# - aci
# - merchant_category_code (using merchant_info)
# - account_type (using merchant_info)
# - intracountry (calculated from issuing_country and acquirer_country)
# - monthly_volume / monthly_fraud_level (Ignored for this specific "what if this fee changed" question, 
#   assuming the question implies the fee applies based on structural characteristics)

matching_indices = []

for index, row in df_filtered.iterrows():
    # 1. Check Merchant/Static attributes against rule
    # MCC
    if not is_match(merchant_mcc, fee_rule['merchant_category_code']):
        continue
    # Account Type
    if not is_match(merchant_account_type, fee_rule['account_type']):
        continue
        
    # 2. Check Transaction attributes against rule
    # Card Scheme
    if not is_match(row['card_scheme'], fee_rule['card_scheme']):
        continue
    # Is Credit
    if not is_match(row['is_credit'], fee_rule['is_credit']):
        continue
    # ACI
    if not is_match(row['aci'], fee_rule['aci']):
        continue
        
    # 3. Check Intracountry
    # Intracountry is True if issuing_country == acquirer_country
    is_intracountry = (row['issuing_country'] == row['acquirer_country'])
    
    # Handle the specific logic for intracountry in fees.json
    # Usually it's a boolean or null.
    # If rule['intracountry'] is 0.0/1.0 (float) or "0.0"/"1.0" (str), we need to handle that.
    rule_intra = fee_rule['intracountry']
    
    # Normalize rule_intra to boolean or None
    if rule_intra is not None:
        # Convert 0.0/1.0 to boolean
        if isinstance(rule_intra, (float, int)):
            rule_intra_bool = bool(rule_intra)
        elif isinstance(rule_intra, str):
            rule_intra_bool = (float(rule_intra) == 1.0)
        else:
            rule_intra_bool = rule_intra
            
        if is_intracountry != rule_intra_bool:
            continue

    # If we passed all checks, this transaction matches the fee rule
    matching_indices.append(index)

df_matching = df_filtered.loc[matching_indices]
print(f"Transactions matching Fee ID {target_fee_id}: {len(df_matching)}")

# 6. Calculate Delta
# Formula: Fee = Fixed + (Rate * Amount / 10000)
# Delta = New_Fee - Old_Fee
# Delta = (Fixed + New_Rate * Amount / 10000) - (Fixed + Old_Rate * Amount / 10000)
# Delta = (New_Rate - Old_Rate) * Amount / 10000

if len(df_matching) > 0:
    total_affected_volume = df_matching['eur_amount'].sum()
    print(f"Total Affected Volume: {total_affected_volume}")
    
    # Calculate delta with high precision
    delta = (new_rate - original_rate) * total_affected_volume / 10000
    
    # Print result with high precision as requested for delta questions
    print(f"\nCalculated Delta:")
    print(f"{delta:.14f}")
else:
    print("\nNo matching transactions found.")
    print("0.00000000000000")
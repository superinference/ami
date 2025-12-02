import pandas as pd
import json
import numpy as np

# ==========================================
# HELPER FUNCTIONS
# ==========================================

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
    return 0.0

def parse_range(range_str):
    """Parses a range string like '100k-1m' or '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    # Handle suffixes
    s = range_str.lower().replace(',', '').replace('%', '')
    multiplier = 1
    
    # Helper to parse single value with k/m suffix
    def parse_val(val):
        val = val.strip()
        m = 1
        if val.endswith('k'):
            m = 1000
            val = val[:-1]
        elif val.endswith('m'):
            m = 1000000
            val = val[:-1]
        elif val.startswith('>'):
            return float(val[1:]) * m, float('inf')
        elif val.startswith('<'):
            return float('-inf'), float(val[1:]) * m
        return float(val) * m

    if '-' in s:
        parts = s.split('-')
        try:
            min_val = parse_val(parts[0])
            max_val = parse_val(parts[1])
            # Handle cases where parse_val returns tuple for >/<
            if isinstance(min_val, tuple): min_val = min_val[0]
            if isinstance(max_val, tuple): max_val = max_val[0]
            
            # Adjust for percentages if original string had %
            if '%' in range_str:
                min_val /= 100
                max_val /= 100
            return min_val, max_val
        except:
            return None, None
    
    # Handle single values like ">5"
    try:
        val = parse_val(s)
        if isinstance(val, tuple):
            v_min, v_max = val
            if '%' in range_str:
                v_min = v_min / 100 if v_min != float('-inf') else v_min
                v_max = v_max / 100 if v_max != float('inf') else v_max
            return v_min, v_max
    except:
        pass
        
    return None, None

def match_fee_rule(tx_context, rule):
    """
    Checks if a transaction context matches a fee rule.
    tx_context must contain: 
      card_scheme, is_credit, aci, eur_amount, 
      merchant_account_type, merchant_mcc, 
      is_intracountry, monthly_volume, monthly_fraud_rate
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match)
    if rule.get('account_type'):
        if tx_context['merchant_account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_context['merchant_mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 5. ACI (List match)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        if rule['intracountry'] != tx_context['is_intracountry']:
            return False

    # 7. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if min_v is not None and max_v is not None:
            if not (min_v <= tx_context['monthly_volume'] <= max_v):
                return False

    # 8. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        if min_f is not None and max_f is not None:
            # Fraud rate in context is usually 0.083 for 8.3%
            if not (min_f <= tx_context['monthly_fraud_rate'] <= max_f):
                return False

    return True

# ==========================================
# MAIN LOGIC
# ==========================================

# 1. Load Data
payments_path = '/output/chunk3/data/context/payments.csv'
fees_path = '/output/chunk3/data/context/fees.json'
merchant_path = '/output/chunk3/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Get Target Fee Rule (ID=787)
target_rule_id = 787
target_rule = next((r for r in fees_data if r['ID'] == target_rule_id), None)

if not target_rule:
    print(f"Error: Fee rule ID {target_rule_id} not found.")
    exit()

original_rate = target_rule['rate']
new_rate = 1  # As per question
print(f"Target Rule ID: {target_rule_id}")
print(f"Original Rate: {original_rate}")
print(f"New Rate: {new_rate}")
print(f"Rule Criteria: {json.dumps(target_rule, indent=2)}")

# 3. Get Merchant Metadata (Rafa_AI)
merchant_name = 'Rafa_AI'
merchant_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)

if not merchant_info:
    print(f"Error: Merchant {merchant_name} not found.")
    exit()

merchant_account_type = merchant_info['account_type']
merchant_mcc = merchant_info['merchant_category_code']
print(f"Merchant: {merchant_name}, Account Type: {merchant_account_type}, MCC: {merchant_mcc}")

# 4. Filter Transactions (Rafa_AI, June 2023)
# Create date column for filtering
df_payments['date'] = pd.to_datetime(
    df_payments['year'].astype(str) + df_payments['day_of_year'].astype(str).str.zfill(3), 
    format='%Y%j'
)

# Filter for Merchant and Month
df_rafa_june = df_payments[
    (df_payments['merchant'] == merchant_name) & 
    (df_payments['date'].dt.month == 6) & 
    (df_payments['date'].dt.year == 2023)
].copy()

print(f"Total Transactions for {merchant_name} in June 2023: {len(df_rafa_june)}")

# 5. Calculate Monthly Stats (Volume & Fraud)
# These are needed because the rule might depend on them
monthly_volume = df_rafa_june['eur_amount'].sum()
monthly_fraud_count = df_rafa_june['has_fraudulent_dispute'].sum()
monthly_tx_count = len(df_rafa_june)
monthly_fraud_rate = monthly_fraud_count / monthly_volume if monthly_volume > 0 else 0 # Fraud is ratio of fraudulent volume over total volume per manual?
# Manual says: "Fraud is defined as the ratio of fraudulent volume over total volume."
# Let's check if 'has_fraudulent_dispute' implies the whole amount is fraud. Usually yes.
fraud_volume = df_rafa_june[df_rafa_june['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_rate_vol = fraud_volume / monthly_volume if monthly_volume > 0 else 0

print(f"Monthly Volume: {monthly_volume}")
print(f"Monthly Fraud Rate (Volume based): {monthly_fraud_rate_vol:.4%}")

# 6. Identify Transactions Matching Rule 787
matching_amounts = []

for _, row in df_rafa_june.iterrows():
    # Construct context for matching
    tx_context = {
        'card_scheme': row['card_scheme'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'eur_amount': row['eur_amount'],
        'merchant_account_type': merchant_account_type,
        'merchant_mcc': merchant_mcc,
        'is_intracountry': row['issuing_country'] == row['acquirer_country'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate_vol
    }
    
    if match_fee_rule(tx_context, target_rule):
        matching_amounts.append(row['eur_amount'])

affected_volume = sum(matching_amounts)
count_matching = len(matching_amounts)

print(f"Transactions matching Rule {target_rule_id}: {count_matching}")
print(f"Affected Volume: {affected_volume:.2f}")

# 7. Calculate Delta
# Formula: Fee = Fixed + (Rate * Amount / 10000)
# Delta = NewFee - OldFee
# Delta = (Fixed + NewRate*Amt/10000) - (Fixed + OldRate*Amt/10000)
# Delta = (NewRate - OldRate) * Amt / 10000

delta = (new_rate - original_rate) * affected_volume / 10000

print("-" * 30)
print(f"{delta:.14f}")
print("-" * 30)
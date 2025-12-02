# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2556
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 6822 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---
def coerce_to_float(value):
    """Convert string with %, $, k, m, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        
        # Handle Percentages
        if '%' in v:
            return float(v.replace('%', '')) / 100
            
        # Handle k/m suffixes (Volume)
        lower_v = v.lower()
        if 'k' in lower_v:
            return float(lower_v.replace('k', '')) * 1_000
        if 'm' in lower_v:
            return float(lower_v.replace('m', '')) * 1_000_000
            
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
            return 0.0
    return float(value) if value is not None else 0.0

def parse_range(rule_value):
    """Parses strings like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max)."""
    if rule_value is None:
        return -float('inf'), float('inf')
    
    s = str(rule_value).strip().lower()
    
    # Helper to parse individual values using coerce_to_float logic
    def parse_val(x):
        # Re-implement basic parsing for range parts to ensure k/m/% handled
        return coerce_to_float(x)

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif s.startswith('>'):
        return parse_val(s[1:]), float('inf')
    elif s.startswith('<'):
        return -float('inf'), parse_val(s[1:])
    else:
        # Exact match treated as range [val, val]
        val = parse_val(s)
        return val, val

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
fees_path = '/output/chunk4/data/context/fees.json'
merchant_data_path = '/output/chunk4/data/context/merchant_data.json'
payments_path = '/output/chunk4/data/context/payments.csv'

with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchants = json.load(f)
df_payments = pd.read_csv(payments_path)

# 2. Get Fee 64 Details
fee_64 = next((f for f in fees if f['ID'] == 64), None)
if not fee_64:
    print("Fee ID 64 not found.")
    exit()

# 3. Prepare Merchant Map (Merchant -> Account Type, MCC)
# We need this to check merchant attributes against Fee 64
merchant_map = {
    m['merchant']: {
        'account_type': m['account_type'],
        'mcc': m['merchant_category_code']
    } 
    for m in merchants
}

# 4. Filter Payments for 2023 (Base Population)
df = df_payments[df_payments['year'] == 2023].copy()

# 5. Apply Static Fee 64 Constraints (Current Rules)
# We filter the dataframe down to transactions that CURRENTLY match Fee 64

# Constraint: Card Scheme
if fee_64['card_scheme']:
    df = df[df['card_scheme'] == fee_64['card_scheme']]

# Constraint: Is Credit
if fee_64['is_credit'] is not None:
    df = df[df['is_credit'] == fee_64['is_credit']]

# Constraint: ACI (List)
if is_not_empty(fee_64['aci']):
    df = df[df['aci'].isin(fee_64['aci'])]

# Constraint: Intracountry
if fee_64['intracountry'] is not None:
    # Intracountry means Issuing Country == Acquirer Country
    is_intra = df['issuing_country'] == df['acquirer_country']
    if fee_64['intracountry']:
        df = df[is_intra]
    else:
        df = df[~is_intra]

# Constraint: Merchant Attributes (MCC & Account Type)
# Map attributes to transactions
df['merchant_account_type'] = df['merchant'].map(lambda x: merchant_map.get(x, {}).get('account_type'))
df['merchant_mcc'] = df['merchant'].map(lambda x: merchant_map.get(x, {}).get('mcc'))

# Filter by MCC (if rule exists)
if is_not_empty(fee_64['merchant_category_code']):
    df = df[df['merchant_mcc'].isin(fee_64['merchant_category_code'])]

# Filter by Account Type (Current Rule)
# Note: We must apply the *current* account type rule first to see who pays it now.
if is_not_empty(fee_64['account_type']):
    df = df[df['merchant_account_type'].isin(fee_64['account_type'])]

# 6. Apply Dynamic Fee Constraints (Volume & Fraud)
# These require aggregation by Merchant and Month
has_vol_rule = fee_64['monthly_volume'] is not None
has_fraud_rule = fee_64['monthly_fraud_level'] is not None

if has_vol_rule or has_fraud_rule:
    # Create Month column from Day of Year (Year is 2023)
    df['date'] = pd.to_datetime(df['year'] * 1000 + df['day_of_year'], format='%Y%j')
    df['month'] = df['date'].dt.month
    
    # Aggregate stats per Merchant per Month
    monthly_stats = df.groupby(['merchant', 'month']).agg(
        total_vol=('eur_amount', 'sum'),
        # Fraud volume: Sum of amount where has_fraudulent_dispute is True
        fraud_vol=('eur_amount', lambda x: x[df.loc[x.index, 'has_fraudulent_dispute']].sum())
    ).reset_index()
    
    # Calculate Fraud Rate (Volume based)
    monthly_stats['fraud_rate'] = monthly_stats['fraud_vol'] / monthly_stats['total_vol']
    monthly_stats['fraud_rate'] = monthly_stats['fraud_rate'].fillna(0)

    # Parse Rule Ranges
    vol_min, vol_max = parse_range(fee_64['monthly_volume'])
    fraud_min, fraud_max = parse_range(fee_64['monthly_fraud_level'])
    
    # Filter valid (Merchant, Month) groups
    valid_groups = monthly_stats.copy()
    
    if has_vol_rule:
        valid_groups = valid_groups[
            (valid_groups['total_vol'] >= vol_min) & 
            (valid_groups['total_vol'] <= vol_max)
        ]
        
    if has_fraud_rule:
        valid_groups = valid_groups[
            (valid_groups['fraud_rate'] >= fraud_min) & 
            (valid_groups['fraud_rate'] <= fraud_max)
        ]
    
    # Keep only transactions belonging to valid groups
    valid_keys = set(zip(valid_groups['merchant'], valid_groups['month']))
    df['key'] = list(zip(df['merchant'], df['month']))
    df = df[df['key'].isin(valid_keys)]

# 7. Identify Affected Merchants
# "Affected" = Merchants who matched ALL current rules (are in `df`)
# BUT do NOT match the new hypothetical rule (Account Type == 'F')
affected_txs = df[df['merchant_account_type'] != 'F']
affected_merchants = sorted(affected_txs['merchant'].unique().tolist())

# 8. Output Result
if affected_merchants:
    print(", ".join(affected_merchants))
else:
    print("No merchants affected")

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

# Load the datasets
fees_df = pd.read_json('/output/chunk2/data/context/fees.json')
merchant_data = pd.read_json('/output/chunk2/data/context/merchant_data.json')
payments_df = pd.read_csv('/output/chunk2/data/context/payments.csv')

# Filter payments for 2023
payments_2023 = payments_df[payments_df['year'] == 2023].copy()

# Merge merchant data to get account_type, merchant_category_code, capture_delay
# payments has 'merchant', merchant_data has 'merchant'
merged_df = pd.merge(payments_2023, merchant_data, on='merchant', how='left')

# Drop rows where merchant data is missing (if any)
merged_df = merged_df.dropna(subset=['account_type'])

# Get Fee Rule 787
rule_787 = fees_df[fees_df['ID'] == 787].iloc[0].to_dict()
print(f"Original Rule 787: {rule_787}")

# Define New Rule 787 (Only Account Type H)
rule_787_new = rule_787.copy()
rule_787_new['account_type'] = ['H']

def parse_range_check(value, rule_value):
    """Helper to check values against rule constraints (exact, range, >, <)."""
    if rule_value is None:
        return True
    str_val = str(value)
    str_rule = str(rule_value)
    if str_val == str_rule: return True
    
    try:
        if str_rule.startswith('>'):
            limit = float(str_rule[1:])
            if str_val == 'manual': return True # manual > days
            if str_val == 'immediate': return False
            return float(str_val) > limit
        if str_rule.startswith('<'):
            limit = float(str_rule[1:])
            if str_val == 'immediate': return True
            if str_val == 'manual': return False
            return float(str_val) < limit
        if '-' in str_rule:
            low, high = map(float, str_rule.split('-'))
            return low <= float(str_val) <= high
    except:
        return False # Fail safe
    return False

def match_fee_rule(row, rule):
    # 1. Card Scheme
    if rule.get('card_scheme') and row['card_scheme'] != rule['card_scheme']:
        return False
    # 2. Account Type
    if rule.get('account_type') and row['account_type'] not in rule['account_type']:
        return False
    # 3. Merchant Category Code
    if rule.get('merchant_category_code') and row['merchant_category_code'] not in rule['merchant_category_code']:
        return False
    # 4. Is Credit
    if rule.get('is_credit') is not None and bool(row['is_credit']) != bool(rule['is_credit']):
        return False
    # 5. ACI
    if rule.get('aci') and row['aci'] not in rule['aci']:
        return False
    # 6. Intracountry
    if rule.get('intracountry') is not None:
        is_intra = (row['issuing_country'] == row['acquirer_country'])
        if is_intra != bool(rule['intracountry']):
            return False
    # 7. Capture Delay
    if rule.get('capture_delay') and not parse_range_check(row['capture_delay'], rule['capture_delay']):
        return False
    return True

# Optimization: Filter by card_scheme if present in rule to reduce rows
if rule_787.get('card_scheme'):
    merged_df = merged_df[merged_df['card_scheme'] == rule_787['card_scheme']]

# Find Merchants matching Original Rule
matches_orig = merged_df.apply(lambda x: match_fee_rule(x, rule_787), axis=1)
merchants_orig = set(merged_df[matches_orig]['merchant'].unique())

# Find Merchants matching New Rule
matches_new = merged_df.apply(lambda x: match_fee_rule(x, rule_787_new), axis=1)
merchants_new = set(merged_df[matches_new]['merchant'].unique())

# Affected Merchants (Symmetric Difference: Lost or Gained)
affected_merchants = merchants_orig.symmetric_difference(merchants_new)

print(f"Merchants matching Original Rule: {merchants_orig}")
print(f"Merchants matching New Rule: {merchants_new}")
print(f"Affected Merchants: {list(affected_merchants)}")
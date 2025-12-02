# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2549
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: sufficient
# Plan steps: 1
# Code length: 7394 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v:
            return float(v.replace('k', '')) * 1000
        if 'm' in v:
            return float(v.replace('m', '')) * 1_000_000
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value)

def parse_range_check(rule_value, actual_value):
    """
    Checks if actual_value fits within the rule_value range string.
    Handles: None (wildcard), ">X", "<X", "X-Y", "X%".
    """
    if rule_value is None:
        return True
    
    # Clean string for parsing
    s = str(rule_value).strip().lower()
    
    # Handle ranges (e.g., "100k-1m", "7.7%-8.3%")
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            low = coerce_to_float(parts[0])
            high = coerce_to_float(parts[1])
            return low <= actual_value <= high
            
    # Handle inequalities
    if s.startswith('>'):
        limit = coerce_to_float(s[1:])
        return actual_value > limit
    if s.startswith('<'):
        limit = coerce_to_float(s[1:])
        return actual_value < limit
        
    # Exact match (fallback)
    return coerce_to_float(s) == actual_value

def match_fee_rule(ctx, rule):
    """
    Determines if a fee rule applies to a transaction context.
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != ctx['card_scheme']:
        return False

    # 2. Account Type (List match or Wildcard)
    if rule['account_type'] and ctx['account_type'] not in rule['account_type']:
        return False

    # 3. Merchant Category Code (List match or Wildcard)
    if rule['merchant_category_code'] and ctx['mcc'] not in rule['merchant_category_code']:
        return False

    # 4. ACI (List match or Wildcard)
    if rule['aci'] and ctx['aci'] not in rule['aci']:
        return False

    # 5. Is Credit (Boolean or Wildcard)
    if rule['is_credit'] is not None and rule['is_credit'] != ctx['is_credit']:
        return False

    # 6. Intracountry (Boolean or Wildcard)
    if rule['intracountry'] is not None:
        if bool(rule['intracountry']) != ctx['intracountry']:
            return False

    # 7. Capture Delay (String match or Wildcard)
    if rule['capture_delay'] is not None:
        r_delay = str(rule['capture_delay'])
        m_delay = str(ctx['capture_delay'])
        
        # Handle inequalities if rule is range-based
        if r_delay.startswith('>'):
            try:
                limit = float(r_delay[1:])
                # Only compare if merchant delay is numeric
                if m_delay.replace('.','',1).isdigit():
                    if not (float(m_delay) > limit): return False
                else:
                    return False # String vs Number inequality mismatch
            except:
                return False
        elif r_delay.startswith('<'):
            try:
                limit = float(r_delay[1:])
                if m_delay.replace('.','',1).isdigit():
                    if not (float(m_delay) < limit): return False
                else:
                    return False
            except:
                return False
        elif r_delay != m_delay:
            return False

    # 8. Monthly Volume (Range check)
    if not parse_range_check(rule['monthly_volume'], ctx['monthly_volume']):
        return False

    # 9. Monthly Fraud Level (Range check)
    if not parse_range_check(rule['monthly_fraud_level'], ctx['monthly_fraud_rate']):
        return False

    return True

def calculate_fee_for_transaction(amount, rule):
    """Calculates fee: fixed + (rate * amount / 10000)"""
    fixed = rule['fixed_amount']
    variable = (rule['rate'] * amount) / 10000
    return fixed + variable

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# 1. Load Data
base_path = '/output/chunk6/data/context/'
df_payments = pd.read_csv(base_path + 'payments.csv')
with open(base_path + 'fees.json', 'r') as f:
    fees_data = json.load(f)
with open(base_path + 'merchant_data.json', 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Rafa_AI and Year 2023
merchant_name = 'Rafa_AI'
df_rafa = df_payments[(df_payments['merchant'] == merchant_name) & (df_payments['year'] == 2023)].copy()

# 3. Get Merchant Config
merchant_config = next((item for item in merchant_data if item["merchant"] == merchant_name), None)
if not merchant_config:
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

original_mcc = merchant_config['merchant_category_code']
account_type = merchant_config['account_type']
capture_delay = merchant_config['capture_delay']

# 4. Preprocessing: Add Month and Intracountry
# 2023 is not a leap year
df_rafa['month'] = pd.to_datetime(df_rafa['day_of_year'], unit='D', origin='2022-12-31').dt.month
df_rafa['intracountry'] = df_rafa['issuing_country'] == df_rafa['acquirer_country']

# 5. Calculate Monthly Stats (Volume and Fraud Rate)
# Group by month to get totals
monthly_stats = df_rafa.groupby('month').agg(
    total_vol=('eur_amount', 'sum'),
    fraud_vol=('eur_amount', lambda x: x[df_rafa.loc[x.index, 'has_fraudulent_dispute']].sum())
).reset_index()

monthly_stats['fraud_rate'] = monthly_stats['fraud_vol'] / monthly_stats['total_vol']

# Create a lookup dictionary for monthly stats
stats_lookup = monthly_stats.set_index('month').to_dict('index')

# 6. Simulation Function
def calculate_total_fees_for_year(df, mcc_code):
    total_fees = 0.0
    
    # Iterate through all transactions
    for _, tx in df.iterrows():
        month = tx['month']
        stats = stats_lookup.get(month)
        
        # Context for matching
        ctx = {
            'card_scheme': tx['card_scheme'],
            'account_type': account_type,
            'mcc': mcc_code,
            'aci': tx['aci'],
            'is_credit': tx['is_credit'],
            'intracountry': tx['intracountry'],
            'capture_delay': capture_delay,
            'monthly_volume': stats['total_vol'],
            'monthly_fraud_rate': stats['fraud_rate']
        }
        
        # Find matching rule (First match wins)
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            fee = calculate_fee_for_transaction(tx['eur_amount'], matched_rule)
            total_fees += fee
            
    return total_fees

# 7. Run Scenarios
# Scenario 1: Original MCC
fees_original = calculate_total_fees_for_year(df_rafa, original_mcc)

# Scenario 2: New MCC (5411)
fees_new = calculate_total_fees_for_year(df_rafa, 5411)

# 8. Calculate Delta
delta = fees_new - fees_original

# 9. Output
print(f"{delta:.14f}")

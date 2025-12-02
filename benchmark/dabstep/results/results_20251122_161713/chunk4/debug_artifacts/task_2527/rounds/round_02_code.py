# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2527
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 10659 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if pd.isna(value) or value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except ValueError:
                return 0.0
        # Range handling (e.g., "50-60") - return mean
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except ValueError:
                pass
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_volume_range(vol_str):
    """Parses volume strings like '100k-1m' into (min, max) floats."""
    if not vol_str or not isinstance(vol_str, str):
        return None, None
    
    s = vol_str.lower().replace('€', '').replace(',', '')
    parts = s.split('-')
    
    def parse_val(v):
        v = v.strip()
        mult = 1
        if 'k' in v:
            mult = 1_000
            v = v.replace('k', '')
        elif 'm' in v:
            mult = 1_000_000
            v = v.replace('m', '')
        return float(v) * mult

    try:
        if len(parts) == 2:
            return parse_val(parts[0]), parse_val(parts[1])
        elif '>' in s:
            return parse_val(s.replace('>', '')), float('inf')
        elif '<' in s:
            return 0.0, parse_val(s.replace('<', ''))
    except:
        return None, None
    return None, None

def parse_capture_delay(delay_str):
    """Parses capture delay rule into a comparable format."""
    # Returns (operator, value)
    # operator: 'eq', 'gt', 'lt', 'range'
    if not delay_str:
        return None
    
    s = str(delay_str).lower().strip()
    
    if s in ['immediate', 'manual']:
        return ('eq', s)
    
    if '-' in s:
        try:
            parts = s.split('-')
            return ('range', (float(parts[0]), float(parts[1])))
        except:
            pass
            
    if '>' in s:
        return ('gt', float(s.replace('>', '')))
    if '<' in s:
        return ('lt', float(s.replace('<', '')))
        
    # Try simple number
    try:
        return ('eq', float(s))
    except:
        return ('eq', s)

def check_capture_delay(merchant_delay, rule_delay_parsed):
    """Checks if merchant delay matches rule."""
    if not rule_delay_parsed:
        return True # No rule
        
    op, rule_val = rule_delay_parsed
    
    # Handle merchant delay format
    merch_val = str(merchant_delay).lower().strip()
    
    # If merchant value is numeric string, convert to float for comparison
    merch_num = None
    try:
        merch_num = float(merch_val)
    except:
        pass

    if op == 'eq':
        # If rule is numeric (e.g. 1), match numeric or string
        if isinstance(rule_val, float) and merch_num is not None:
            return merch_num == rule_val
        return merch_val == str(rule_val)
        
    if op == 'gt':
        if merch_num is not None:
            return merch_num > rule_val
        return False # Cannot compare string > number
        
    if op == 'lt':
        if merch_num is not None:
            return merch_num < rule_val
        return False
        
    if op == 'range':
        min_v, max_v = rule_val
        if merch_num is not None:
            return min_v <= merch_num <= max_v
        return False
        
    return False

# ═══════════════════════════════════════════════════════════
# MAIN LOGIC
# ═══════════════════════════════════════════════════════════

# 1. Load Data
fees_path = '/output/chunk4/data/context/fees.json'
merchant_path = '/output/chunk4/data/context/merchant_data.json'
payments_path = '/output/chunk4/data/context/payments.csv'

with open(fees_path, 'r') as f:
    fees_data = json.load(f)

with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

df_payments = pd.read_csv(payments_path)

# 2. Get Fee Rule 454
fee_rule = next((f for f in fees_data if f['ID'] == 454), None)

if not fee_rule:
    print("Error: Fee ID 454 not found.")
else:
    print(f"Analyzing Fee ID 454: {json.dumps(fee_rule, indent=2)}")

    # 3. Prepare Merchant Data Lookup
    # Create a dictionary for fast lookup of merchant attributes
    merchant_lookup = {}
    for m in merchant_data:
        merchant_lookup[m['merchant']] = {
            'account_type': m['account_type'],
            'mcc': m['merchant_category_code'],
            'capture_delay': m['capture_delay']
        }

    # 4. Prepare Payments Data
    # Filter for 2023
    df_2023 = df_payments[df_payments['year'] == 2023].copy()
    
    # Add Month column (approximate or exact using datetime)
    # Using datetime is safer for day_of_year conversion
    df_2023['date'] = pd.to_datetime(df_2023['year'] * 1000 + df_2023['day_of_year'], format='%Y%j')
    df_2023['month'] = df_2023['date'].dt.month
    
    # Add Intracountry flag
    df_2023['is_intracountry'] = df_2023['issuing_country'] == df_2023['acquirer_country']

    # 5. Calculate Monthly Stats (Volume and Fraud)
    # Group by Merchant and Month
    monthly_stats = df_2023.groupby(['merchant', 'month']).agg(
        monthly_vol=('eur_amount', 'sum'),
        fraud_vol=('eur_amount', lambda x: x[df_2023.loc[x.index, 'has_fraudulent_dispute']].sum())
    ).reset_index()
    
    # Calculate Fraud Rate (Volume based as per manual definition for 'monthly_fraud_level')
    # "ratio between monthly total volume and monthly volume notified as fraud"
    # Note: Manual phrasing is slightly ambiguous, usually it's fraud/total. 
    # If total is 0, rate is 0.
    monthly_stats['fraud_rate'] = monthly_stats.apply(
        lambda row: (row['fraud_vol'] / row['monthly_vol']) if row['monthly_vol'] > 0 else 0, axis=1
    )

    # 6. Parse Fee Rule Constraints
    rule_card_scheme = fee_rule.get('card_scheme')
    rule_account_types = fee_rule.get('account_type') # List
    rule_mccs = fee_rule.get('merchant_category_code') # List
    rule_is_credit = fee_rule.get('is_credit')
    rule_aci = fee_rule.get('aci') # List
    rule_intracountry = fee_rule.get('intracountry')
    
    # Parse complex constraints
    vol_min, vol_max = parse_volume_range(fee_rule.get('monthly_volume'))
    fraud_min, fraud_max = parse_volume_range(fee_rule.get('monthly_fraud_level')) # Re-using parse_volume_range as it handles ranges similarly, but need to handle %
    
    # Custom parsing for fraud if it has % (parse_volume_range handles k/m, let's make sure we handle %)
    # Actually, let's use a specific simple parser for fraud if needed, or rely on the fact that coerce_to_float handles %.
    # Let's refine fraud parsing manually here to be safe.
    rule_fraud_str = fee_rule.get('monthly_fraud_level')
    fraud_range = (None, None)
    if rule_fraud_str:
        # Example: ">8.3%"
        s = rule_fraud_str.replace('%', '')
        if '>' in s:
            fraud_range = (float(s.replace('>', '')) / 100, float('inf'))
        elif '<' in s:
            fraud_range = (0.0, float(s.replace('<', '')) / 100)
        elif '-' in s:
            p = s.split('-')
            fraud_range = (float(p[0])/100, float(p[1])/100)

    rule_capture_parsed = parse_capture_delay(fee_rule.get('capture_delay'))

    # 7. Apply Filters to Transactions
    # We will iterate through the transactions (merged with stats) and check conditions
    
    # Merge monthly stats back to transactions
    df_merged = pd.merge(df_2023, monthly_stats, on=['merchant', 'month'], how='left')
    
    affected_merchants = set()
    
    # Optimization: Filter by Card Scheme first if present (fastest reduction)
    if rule_card_scheme:
        df_merged = df_merged[df_merged['card_scheme'] == rule_card_scheme]
        
    # Filter by is_credit if not None
    if rule_is_credit is not None:
        df_merged = df_merged[df_merged['is_credit'] == rule_is_credit]
        
    # Filter by intracountry if not None
    if rule_intracountry is not None:
        # Note: fee_rule['intracountry'] might be 0.0/1.0 or False/True
        # Convert rule to bool
        rule_intra_bool = bool(fee_rule['intracountry'])
        df_merged = df_merged[df_merged['is_intracountry'] == rule_intra_bool]

    # Iterate remaining to check complex conditions
    # Group by merchant to speed up static checks
    for merchant, group in df_merged.groupby('merchant'):
        # 1. Static Merchant Checks
        m_info = merchant_lookup.get(merchant)
        if not m_info:
            continue
            
        # Account Type
        if rule_account_types and m_info['account_type'] not in rule_account_types:
            continue
            
        # MCC
        if rule_mccs and m_info['mcc'] not in rule_mccs:
            continue
            
        # Capture Delay
        if rule_capture_parsed and not check_capture_delay(m_info['capture_delay'], rule_capture_parsed):
            continue
            
        # 2. Dynamic/Monthly Checks (Check if ANY transaction in the group passes)
        # We need to check ACI (transaction level) and Monthly Volume/Fraud (month level)
        
        # Filter group for ACI
        if rule_aci:
            # rule_aci is a list, e.g. ['A', 'C']
            # Check if any transaction has ACI in this list
            # But we need the specific transaction that ALSO satisfies volume/fraud
            group = group[group['aci'].isin(rule_aci)]
            
        if group.empty:
            continue
            
        # Filter group for Monthly Volume
        if vol_min is not None:
            group = group[(group['monthly_vol'] >= vol_min) & (group['monthly_vol'] <= vol_max)]
            
        if group.empty:
            continue
            
        # Filter group for Monthly Fraud
        if fraud_range[0] is not None:
            f_min, f_max = fraud_range
            group = group[(group['fraud_rate'] >= f_min) & (group['fraud_rate'] <= f_max)]
            
        if not group.empty:
            affected_merchants.add(merchant)

    # 8. Output Result
    result_list = sorted(list(affected_merchants))
    print(f"Merchants affected by Fee 454 in 2023: {result_list}")

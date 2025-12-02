# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2481
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 10035 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import re
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS (Robust Data Processing)
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators for simple conversion
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except ValueError:
                return 0.0
        # Range handling (e.g., "50-60") - return mean for simple coercion
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

def parse_volume_string(vol_str):
    """Parses volume strings like '100k', '1m' into floats."""
    if not isinstance(vol_str, str):
        return float(vol_str)
    s = vol_str.lower().strip()
    multiplier = 1
    if s.endswith('k'):
        multiplier = 1_000
        s = s[:-1]
    elif s.endswith('m'):
        multiplier = 1_000_000
        s = s[:-1]
    try:
        return float(s) * multiplier
    except ValueError:
        return 0.0

def check_range(value, range_rule, is_percentage=False):
    """
    Checks if a value fits within a rule range string.
    Examples: '100k-1m', '>5', '<3', '7.7%-8.3%'
    """
    if range_rule is None:
        return True
    
    # Handle percentage conversion for the rule string if needed
    # The value passed in is expected to be a float (e.g., 0.08 for 8%)
    
    rule = str(range_rule).strip()
    
    # Parse operators
    if rule.startswith('>'):
        limit_str = rule[1:]
        limit = coerce_to_float(limit_str)
        return value > limit
    if rule.startswith('<'):
        limit_str = rule[1:]
        limit = coerce_to_float(limit_str)
        return value < limit
        
    # Parse ranges (e.g., "100k-1m")
    if '-' in rule:
        parts = rule.split('-')
        if len(parts) == 2:
            # Custom parsing for volume/percentage strings
            lower = parse_volume_string(parts[0].replace('%', ''))
            upper = parse_volume_string(parts[1].replace('%', ''))
            
            if is_percentage:
                lower = lower / 100 if '%' in parts[0] else lower
                upper = upper / 100 if '%' in parts[1] else upper
            else:
                # Handle k/m for volumes
                lower = parse_volume_string(parts[0])
                upper = parse_volume_string(parts[1])
                
            return lower <= value <= upper
            
    # Exact match (fallback)
    try:
        limit = parse_volume_string(rule)
        if is_percentage and '%' in rule:
            limit /= 100
        return value == limit
    except:
        return False

def check_capture_delay(merchant_delay, rule_delay):
    """Checks if merchant capture delay matches the rule."""
    if rule_delay is None:
        return True
    
    m_val = str(merchant_delay).lower()
    r_val = str(rule_delay).lower()
    
    # Direct string match (e.g., "manual", "immediate")
    if m_val == r_val:
        return True
    
    # Numeric comparison if merchant has numeric delay (e.g., "1", "2")
    if m_val.isdigit():
        days = float(m_val)
        return check_range(days, r_val)
        
    return False

def match_fee_rule(ctx, rule):
    """
    Determines if a fee rule applies to a transaction context.
    ctx: dict containing transaction and merchant details
    rule: dict containing fee rule details
    """
    # 1. Card Scheme
    if rule['card_scheme'] != ctx['card_scheme']:
        return False
        
    # 2. Account Type (List match)
    if rule['account_type'] and ctx['account_type'] not in rule['account_type']:
        return False
        
    # 3. Merchant Category Code (List match)
    if rule['merchant_category_code'] and ctx['merchant_category_code'] not in rule['merchant_category_code']:
        return False
        
    # 4. Is Credit (Boolean match)
    if rule['is_credit'] is not None and rule['is_credit'] != ctx['is_credit']:
        return False
        
    # 5. ACI (List match)
    if rule['aci'] and ctx['aci'] not in rule['aci']:
        return False
        
    # 6. Intracountry (Boolean logic)
    is_intra = (ctx['issuing_country'] == ctx['acquirer_country'])
    if rule['intracountry'] is not None:
        # rule['intracountry'] is 0.0 (False) or 1.0 (True) or boolean in JSON
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    # 7. Monthly Volume (Range match)
    if rule['monthly_volume']:
        if not check_range(ctx['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 8. Monthly Fraud Level (Range match)
    if rule['monthly_fraud_level']:
        if not check_range(ctx['monthly_fraud_level'], rule['monthly_fraud_level'], is_percentage=True):
            return False
            
    # 9. Capture Delay
    if rule['capture_delay']:
        if not check_capture_delay(ctx['capture_delay'], rule['capture_delay']):
            return False
            
    return True

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

def execute_step():
    # 1. Load Data
    print("Loading data...")
    payments_path = '/output/chunk4/data/context/payments.csv'
    fees_path = '/output/chunk4/data/context/fees.json'
    merchant_path = '/output/chunk4/data/context/merchant_data.json'
    
    df = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees = json.load(f)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)
        
    target_merchant = 'Crossfit_Hanna'
    target_year = 2023
    target_rule_id = 384
    new_rate_val = 1
    
    # 2. Filter Transactions
    print(f"Filtering for {target_merchant} in {target_year}...")
    df_filtered = df[
        (df['merchant'] == target_merchant) & 
        (df['year'] == target_year)
    ].copy()
    
    if len(df_filtered) == 0:
        print("No transactions found.")
        return

    # 3. Get Merchant Metadata
    m_meta = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not m_meta:
        print(f"Merchant {target_merchant} not found in merchant_data.json")
        return
        
    # 4. Get Target Rule Info
    target_rule = next((r for r in fees if r['ID'] == target_rule_id), None)
    if not target_rule:
        print(f"Rule ID {target_rule_id} not found in fees.json")
        return
    
    old_rate_val = target_rule['rate']
    print(f"Target Rule {target_rule_id}: Old Rate = {old_rate_val}, New Rate = {new_rate_val}")
    
    # 5. Calculate Monthly Stats (Volume & Fraud Rate)
    # Convert day_of_year to month
    # 2023 is not a leap year
    df_filtered['date'] = pd.to_datetime(df_filtered['year'] * 1000 + df_filtered['day_of_year'], format='%Y%j')
    df_filtered['month'] = df_filtered['date'].dt.month
    
    monthly_stats = {}
    for month in range(1, 13):
        m_df = df_filtered[df_filtered['month'] == month]
        if len(m_df) > 0:
            vol = m_df['eur_amount'].sum()
            # Fraud volume: sum of amounts where has_fraudulent_dispute is True
            fraud_vol = m_df[m_df['has_fraudulent_dispute'] == True]['eur_amount'].sum()
            fraud_rate = fraud_vol / vol if vol > 0 else 0.0
        else:
            vol = 0.0
            fraud_rate = 0.0
            
        monthly_stats[month] = {
            'volume': vol,
            'fraud_rate': fraud_rate
        }

    # 6. Iterate Transactions and Calculate Delta
    total_delta = 0.0
    affected_count = 0
    
    # Pre-process merchant static fields for speed
    m_account_type = m_meta.get('account_type')
    m_mcc = m_meta.get('merchant_category_code')
    m_capture_delay = m_meta.get('capture_delay')
    
    print("Processing transactions to find applicable fees...")
    
    for _, row in df_filtered.iterrows():
        # Build Context
        month = row['month']
        stats = monthly_stats.get(month)
        
        ctx = {
            'card_scheme': row['card_scheme'],
            'account_type': m_account_type,
            'merchant_category_code': m_mcc,
            'is_credit': row['is_credit'],
            'aci': row['aci'],
            'issuing_country': row['issuing_country'],
            'acquirer_country': row['acquirer_country'],
            'monthly_volume': stats['volume'],
            'monthly_fraud_level': stats['fraud_rate'],
            'capture_delay': m_capture_delay
        }
        
        # Find First Matching Rule
        matched_id = None
        for rule in fees:
            if match_fee_rule(ctx, rule):
                matched_id = rule['ID']
                break
        
        # Calculate Delta if matched rule is the target rule
        if matched_id == target_rule_id:
            # Fee = Fixed + (Rate * Amount / 10000)
            # Delta = NewFee - OldFee
            # Delta = (Fixed + NewRate*Amt/10000) - (Fixed + OldRate*Amt/10000)
            # Delta = (NewRate - OldRate) * Amt / 10000
            
            amount = row['eur_amount']
            delta = (new_rate_val - old_rate_val) * amount / 10000.0
            total_delta += delta
            affected_count += 1

    print(f"Transactions affected by Rule {target_rule_id}: {affected_count}")
    print(f"Total Delta: {total_delta:.14f}")

if __name__ == "__main__":
    execute_step()

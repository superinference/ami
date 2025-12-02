# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2366
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8675 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import re

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

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
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value)

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

def parse_range_value(val_str):
    """Parses strings like '100k', '1m', '5%' into floats."""
    if not isinstance(val_str, str):
        return float(val_str)
    val_str = val_str.lower().strip()
    if val_str.endswith('%'):
        return float(val_str.replace('%', '')) / 100.0
    if val_str.endswith('k'):
        return float(val_str.replace('k', '')) * 1000
    if val_str.endswith('m'):
        return float(val_str.replace('m', '')) * 1_000_000
    return float(val_str)

def check_numeric_condition(value, condition_str):
    """Checks if a numeric value satisfies a condition string (e.g., '>5', '3-5', '100k-1m')."""
    if condition_str is None:
        return True
    
    cond = condition_str.strip()
    
    # Range "min-max"
    if '-' in cond:
        try:
            parts = cond.split('-')
            min_val = parse_range_value(parts[0])
            max_val = parse_range_value(parts[1])
            return min_val <= value <= max_val
        except:
            return False
            
    # Inequality ">X" or "<X"
    if cond.startswith('>'):
        limit = parse_range_value(cond[1:])
        return value > limit
    if cond.startswith('<'):
        limit = parse_range_value(cond[1:])
        return value < limit
        
    # Exact match (numeric)
    try:
        return value == parse_range_value(cond)
    except:
        return False

def check_capture_delay(merchant_delay, rule_delay):
    """Matches merchant capture delay against rule."""
    if rule_delay is None:
        return True
    
    # Exact string match (e.g., "manual", "immediate")
    if str(merchant_delay) == str(rule_delay):
        return True
        
    # If rule is numeric condition (e.g., "<3") and merchant has numeric delay
    try:
        m_val = float(merchant_delay)
        return check_numeric_condition(m_val, rule_delay)
    except ValueError:
        # Merchant delay is non-numeric (e.g. "manual"), rule is numeric -> No match
        return False

def match_fee_rule(ctx, rule):
    """
    Determines if a fee rule applies to a transaction context.
    ctx: dict containing transaction and merchant details
    rule: dict containing fee rule details
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != ctx['card_scheme']:
        return False
        
    # 2. Account Type (List match, empty=wildcard)
    if is_not_empty(rule['account_type']):
        if ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match, empty=wildcard)
    if is_not_empty(rule['merchant_category_code']):
        if ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Boolean match, null=wildcard)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != ctx['is_credit']:
            return False
            
    # 5. ACI (List match, empty=wildcard)
    if is_not_empty(rule['aci']):
        if ctx['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Boolean match, null=wildcard)
    if rule['intracountry'] is not None:
        is_intra = (ctx['issuing_country'] == ctx['acquirer_country'])
        # rule['intracountry'] might be 0.0/1.0 or bool
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False
            
    # 7. Capture Delay (String/Range match, null=wildcard)
    if rule['capture_delay'] is not None:
        if not check_capture_delay(ctx['capture_delay'], rule['capture_delay']):
            return False
            
    # 8. Monthly Volume (Range match, null=wildcard)
    if rule['monthly_volume'] is not None:
        if not check_numeric_condition(ctx['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 9. Monthly Fraud Level (Range match, null=wildcard)
    if rule['monthly_fraud_level'] is not None:
        if not check_numeric_condition(ctx['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False
            
    return True

# ---------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------

def main():
    # 1. Load Data
    payments_path = '/output/chunk3/data/context/payments.csv'
    fees_path = '/output/chunk3/data/context/fees.json'
    merchant_path = '/output/chunk3/data/context/merchant_data.json'
    
    df = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees = json.load(f)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)

    # 2. Filter for Rafa_AI in February 2023
    # Feb 2023 (non-leap) is Day 32 to 59
    merchant_name = 'Rafa_AI'
    target_year = 2023
    start_day = 32
    end_day = 59
    
    df_filtered = df[
        (df['merchant'] == merchant_name) &
        (df['year'] == target_year) &
        (df['day_of_year'] >= start_day) &
        (df['day_of_year'] <= end_day)
    ].copy()
    
    if df_filtered.empty:
        print("0.00000000000000")
        return

    # 3. Calculate Merchant Monthly Stats (Volume & Fraud Rate)
    # Manual: "ratio between monthly total volume and monthly volume notified as fraud"
    total_volume = df_filtered['eur_amount'].sum()
    fraud_volume = df_filtered[df_filtered['has_fraudulent_dispute']]['eur_amount'].sum()
    fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0
    
    # 4. Get Merchant Static Data
    m_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
    if not m_info:
        print("Error: Merchant data not found")
        return

    # 5. Get Target Rule (ID=17) Info
    target_rule_id = 17
    target_rule = next((r for r in fees if r['ID'] == target_rule_id), None)
    if not target_rule:
        print("Error: Rule 17 not found")
        return
    
    original_rate = target_rule['rate']
    new_rate = 99
    
    # 6. Identify Transactions where Rule 17 Applies
    affected_volume = 0.0
    
    # Pre-calculate context fields that are constant for the merchant
    base_ctx = {
        'account_type': m_info['account_type'],
        'mcc': m_info['merchant_category_code'],
        'capture_delay': m_info['capture_delay'],
        'monthly_volume': total_volume,
        'monthly_fraud_rate': fraud_rate
    }
    
    for _, tx in df_filtered.iterrows():
        # Update context with transaction-specific fields
        ctx = base_ctx.copy()
        ctx.update({
            'card_scheme': tx['card_scheme'],
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'issuing_country': tx['issuing_country'],
            'acquirer_country': tx['acquirer_country']
        })
        
        # Find the FIRST matching rule (Priority based on list order)
        matched_id = None
        for rule in fees:
            if match_fee_rule(ctx, rule):
                matched_id = rule['ID']
                break
        
        # If the applicable rule is ID 17, add to affected volume
        if matched_id == target_rule_id:
            affected_volume += tx['eur_amount']
            
    # 7. Calculate Delta
    # Delta = (New Rate - Old Rate) * Amount / 10000
    delta = (new_rate - original_rate) * affected_volume / 10000
    
    # Print with high precision
    print(f"{delta:.14f}")

if __name__ == "__main__":
    main()

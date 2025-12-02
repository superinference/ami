# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1791
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: sufficient
# Plan steps: 2
# Code length: 10006 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
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
            except:
                pass
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range(range_str):
    """
    Parses a range string like '100k-1m', '7.7%-8.3%', '>5', '<3'.
    Returns (min_val, max_val).
    """
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    # Helper to parse individual values with suffixes
    def parse_val(x):
        x = x.strip()
        if x.endswith('%'):
            return float(x.replace('%', '')) / 100.0
        factor = 1
        if x.endswith('k'):
            factor = 1000
            x = x[:-1]
        elif x.endswith('m'):
            factor = 1000000
            x = x[:-1]
        try:
            return float(x) * factor
        except ValueError:
            return 0.0

    if '>' in s:
        val = parse_val(s.replace('>', ''))
        return val, float('inf') # >5 means (5, inf) - strictly greater usually, but for fees often inclusive boundary logic varies. Let's assume standard math.
        # Actually, usually >5 means 6 onwards for integers, or 5.0001. Let's use val as exclusive lower bound if possible, or inclusive if logic dictates.
        # For simplicity in fee rules, >5 usually implies >= 6 if integer days, or > 5.0.
        # Let's treat it as strictly greater for float comparison.
        # However, standard implementation often treats > as >= for safety or uses epsilon.
        # Let's stick to strict >.
        return val, float('inf')

    if '<' in s:
        val = parse_val(s.replace('<', ''))
        return float('-inf'), val

    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            return parse_val(parts[0]), parse_val(parts[1])
            
    # Exact match numeric string
    try:
        val = parse_val(s)
        return val, val
    except:
        return None, None

def check_range(value, range_str):
    """Checks if a value falls within a range string."""
    if range_str is None:
        return True
    
    # Handle exact string matches for non-numeric ranges (e.g. 'immediate')
    if isinstance(range_str, str) and not any(c.isdigit() for c in range_str):
        return str(value).lower() == range_str.lower()

    min_v, max_v = parse_range(range_str)
    
    # If parsing failed, fall back to string equality
    if min_v is None: 
        return str(value).lower() == str(range_str).lower()
    
    # Handle strict inequalities if needed, but usually inclusive is safer for ranges like 3-5
    # For >5, min_v is 5, max_v is inf. value 7 -> 5 <= 7 <= inf.
    # Wait, parse_range returns (5, inf) for >5.
    # If logic is strictly >, we need to adjust.
    # But usually '3-5' is inclusive.
    # Let's refine check:
    
    if '>' in range_str:
        return value > min_v
    if '<' in range_str:
        return value < max_v
        
    return min_v <= value <= max_v

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match - rule['account_type'] is a list of allowed types)
    # If rule list is empty, it applies to all.
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    # If rule list is empty, it applies to all.
    if rule.get('merchant_category_code'):
        if tx_context['merchant_category_code'] not in rule['merchant_category_code']:
            return False

    # 4. Capture Delay
    # Merchant has specific value (e.g., '1', 'immediate'). Rule has range/value (e.g., '<3', 'immediate').
    rule_delay = rule.get('capture_delay')
    if rule_delay:
        merch_delay = str(tx_context['capture_delay'])
        
        # Case A: Both are words
        if not merch_delay[0].isdigit() and not rule_delay[0].isdigit() and not rule_delay.startswith(('>', '<')):
            if merch_delay.lower() != rule_delay.lower():
                return False
        
        # Case B: Merchant is numeric (days), Rule is range/numeric
        elif merch_delay.isdigit():
            days = int(merch_delay)
            if not check_range(days, rule_delay):
                return False
        
        # Case C: Mismatch types (e.g. Merchant 'immediate' vs Rule '<3') -> False
        else:
            # If one is numeric-like and other is word-like, they don't match
            # Unless 'immediate' implies 0 days? Usually treated as distinct category.
            return False

    # 5. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not check_range(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 6. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not check_range(tx_context['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    # 7. Is Credit (Boolean match)
    # If rule is None, applies to both.
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 8. ACI (List match)
    # If rule list is empty, applies to all.
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 9. Intracountry (Boolean match)
    # Rule: 1.0 (True), 0.0 (False), None (Any)
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False

    return True

# ---------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------

def main():
    # 1. Load Data
    try:
        df_payments = pd.read_csv('/output/chunk6/data/context/payments.csv')
        with open('/output/chunk6/data/context/fees.json', 'r') as f:
            fees_data = json.load(f)
        with open('/output/chunk6/data/context/merchant_data.json', 'r') as f:
            merchant_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Define Context
    target_merchant = 'Martinis_Fine_Steakhouse'
    target_year = 2023
    # May is days 121 to 151 (non-leap year)
    start_day = 121
    end_day = 151

    # 3. Filter Payments for Merchant and Month
    df_month = df_payments[
        (df_payments['merchant'] == target_merchant) &
        (df_payments['year'] == target_year) &
        (df_payments['day_of_year'] >= start_day) &
        (df_payments['day_of_year'] <= end_day)
    ].copy()

    if df_month.empty:
        print("No transactions found for this merchant in May 2023.")
        return

    # 4. Calculate Monthly Stats (Volume & Fraud)
    # Manual Section 7: "Fraud is defined as the ratio of fraudulent volume over total volume."
    total_volume = df_month['eur_amount'].sum()
    fraud_volume = df_month[df_month['has_fraudulent_dispute']]['eur_amount'].sum()
    
    monthly_fraud_ratio = fraud_volume / total_volume if total_volume > 0 else 0.0
    
    # Debug stats
    # print(f"Total Volume: {total_volume}")
    # print(f"Fraud Ratio: {monthly_fraud_ratio}")

    # 5. Get Static Merchant Attributes
    merch_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
    if not merch_info:
        print(f"Merchant {target_merchant} not found in merchant_data.json")
        return

    # 6. Identify Applicable Fee IDs
    # We need to check every unique transaction profile that occurred
    
    # Calculate intracountry for each transaction
    # Intracountry = (issuing_country == acquirer_country)
    df_month['intracountry'] = df_month['issuing_country'] == df_month['acquirer_country']
    
    # Get unique profiles
    unique_tx_profiles = df_month[[
        'card_scheme', 'is_credit', 'aci', 'intracountry'
    ]].drop_duplicates()

    applicable_fee_ids = set()

    for _, tx in unique_tx_profiles.iterrows():
        # Build context for matching
        context = {
            'card_scheme': tx['card_scheme'],
            'account_type': merch_info['account_type'],
            'merchant_category_code': merch_info['merchant_category_code'],
            'capture_delay': merch_info['capture_delay'],
            'monthly_volume': total_volume,
            'monthly_fraud_level': monthly_fraud_ratio,
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'intracountry': tx['intracountry']
        }
        
        # Check against all rules
        for rule in fees_data:
            if match_fee_rule(context, rule):
                applicable_fee_ids.add(rule['ID'])

    # 7. Output Result
    sorted_ids = sorted(list(applicable_fee_ids))
    print(", ".join(map(str, sorted_ids)))

if __name__ == "__main__":
    main()

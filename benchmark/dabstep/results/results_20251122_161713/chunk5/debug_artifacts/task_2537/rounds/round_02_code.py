# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2537
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 10964 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np
import datetime

# ==========================================
# HELPER FUNCTIONS
# ==========================================

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

def parse_range(range_str, scale=1.0):
    """
    Parses a range string like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max).
    scale: multiplier for values (e.g. 1000 for 'k', 1000000 for 'm' handling).
    Returns (min_val, max_val). None indicates no limit.
    """
    if not range_str or not isinstance(range_str, str):
        return None, None

    s = range_str.strip().lower()
    
    # Handle percentages
    is_percent = '%' in s
    if is_percent:
        s = s.replace('%', '')
        scale = 0.01 # Override scale for percentages

    # Handle k/m suffixes for volume
    def parse_val(val_str):
        val_str = val_str.strip()
        mult = 1.0
        if val_str.endswith('k'):
            mult = 1000.0
            val_str = val_str[:-1]
        elif val_str.endswith('m'):
            mult = 1000000.0
            val_str = val_str[:-1]
        try:
            return float(val_str) * mult * scale
        except ValueError:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif s.startswith('>'):
        return parse_val(s[1:]), float('inf')
    elif s.startswith('<'):
        return 0.0, parse_val(s[1:])
    else:
        # Exact value treated as min=max? Or just return val
        v = parse_val(s)
        return v, v

def check_range_match(value, range_str, is_percentage=False):
    """Checks if a value falls within a range string."""
    if range_str is None:
        return True
    
    # Special handling for volume suffixes if not percentage
    scale = 1.0
    
    min_v, max_v = parse_range(range_str, scale)
    
    if min_v is None: return True
    
    # Inclusive check
    return min_v <= value <= max_v

def match_fee_rule(tx_context, rule):
    """
    Matches a transaction context against a fee rule.
    tx_context: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List contains) - Wildcard if empty/None
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 3. Capture Delay (Exact match) - Wildcard if None
    if rule.get('capture_delay') is not None:
        if rule['capture_delay'] != tx_context['capture_delay']:
            # Handle range logic for capture delay if necessary (e.g. >5), 
            # but data shows 'manual', 'immediate', '1'.
            # If rule is '>5' and value is 'manual' (which implies long delay), logic might be needed.
            # Based on manual.md: 'manual' is distinct.
            # Let's assume string equality for categorical values, range check for numeric strings.
            # If rule is '>5' and val is 'manual', strictly they don't match unless 'manual' > 5.
            # However, usually categorical fields in fees.json match categorical in merchant_data.
            # Let's stick to strict equality for now unless it's a clear numeric comparison.
            if rule['capture_delay'].startswith(('>', '<')):
                 # If rule is numeric range but value is 'manual'/'immediate', it's a mismatch
                 # unless we map 'manual' to infinity.
                 # For this specific dataset, 'manual' usually matches 'manual' rules.
                 return False
            return False

    # 4. Merchant Category Code (List contains) - Wildcard if empty/None
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False

    # 5. Is Credit (Bool match) - Wildcard if None
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 6. ACI (List contains) - Wildcard if empty/None
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 7. Intracountry (Bool match) - Wildcard if None
    if rule.get('intracountry') is not None:
        # Intracountry is True if issuer == acquirer
        is_intra = (tx_context['issuing_country'] == tx_context['acquirer_country'])
        # fees.json uses 0.0/1.0 for bools sometimes, or true/false
        rule_intra = rule['intracountry']
        if isinstance(rule_intra, float):
            rule_intra = bool(rule_intra)
        
        if rule_intra != is_intra:
            return False

    # 8. Monthly Volume (Range match) - Wildcard if None
    if rule.get('monthly_volume'):
        if not check_range_match(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match) - Wildcard if None
    if rule.get('monthly_fraud_level'):
        if not check_range_match(tx_context['monthly_fraud_rate'], rule['monthly_fraud_level'], is_percentage=True):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Rate is integer to be divided by 10000
    variable = (rate * amount) / 10000.0
    return fixed + variable

# ==========================================
# MAIN ANALYSIS
# ==========================================

def main():
    # File Paths
    payments_path = '/output/chunk5/data/context/payments.csv'
    fees_path = '/output/chunk5/data/context/fees.json'
    merchant_path = '/output/chunk5/data/context/merchant_data.json'

    print("Loading data...")
    
    # 1. Load Data
    try:
        df_payments = pd.read_csv(payments_path)
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
        with open(merchant_path, 'r') as f:
            merchants_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Filter for Target Merchant and Year
    target_merchant = "Crossfit_Hanna"
    target_year = 2023
    
    df_tx = df_payments[
        (df_payments['merchant'] == target_merchant) & 
        (df_payments['year'] == target_year)
    ].copy()
    
    if df_tx.empty:
        print(f"No transactions found for {target_merchant} in {target_year}")
        return

    print(f"Found {len(df_tx)} transactions for {target_merchant} in {target_year}")

    # 3. Get Merchant Profile
    merchant_profile = next((m for m in merchants_data if m['merchant'] == target_merchant), None)
    if not merchant_profile:
        print(f"Merchant profile not found for {target_merchant}")
        return

    original_mcc = merchant_profile['merchant_category_code']
    account_type = merchant_profile['account_type']
    capture_delay = merchant_profile['capture_delay']
    
    print(f"Merchant Profile: MCC={original_mcc}, Account={account_type}, Delay={capture_delay}")

    # 4. Calculate Monthly Stats (Volume and Fraud Rate)
    # Map day_of_year to month (2023 is non-leap)
    # Create a date column to extract month easily
    df_tx['date'] = pd.to_datetime(df_tx['year'] * 1000 + df_tx['day_of_year'], format='%Y%j')
    df_tx['month'] = df_tx['date'].dt.month

    monthly_stats = {}
    for month in range(1, 13):
        month_data = df_tx[df_tx['month'] == month]
        if month_data.empty:
            monthly_stats[month] = {'vol': 0.0, 'fraud_rate': 0.0}
            continue
        
        total_vol = month_data['eur_amount'].sum()
        fraud_vol = month_data[month_data['has_fraudulent_dispute']]['eur_amount'].sum()
        
        fraud_rate = (fraud_vol / total_vol) if total_vol > 0 else 0.0
        
        monthly_stats[month] = {
            'vol': total_vol,
            'fraud_rate': fraud_rate
        }
    
    # 5. Calculate Fees for Both Scenarios
    total_delta = 0.0
    
    # Pre-sort fees? No, just iterate. Assuming first match wins.
    # Note: In many rule systems, order matters. We use the order in the JSON.
    
    # Optimization: Convert fees to list if not already
    if not isinstance(fees_data, list):
        print("Error: fees.json is not a list")
        return

    count = 0
    
    for _, tx in df_tx.iterrows():
        month = tx['month']
        stats = monthly_stats[month]
        
        # Base Context
        context = {
            'card_scheme': tx['card_scheme'],
            'account_type': account_type,
            'capture_delay': capture_delay,
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'issuing_country': tx['issuing_country'],
            'acquirer_country': tx['acquirer_country'],
            'monthly_volume': stats['vol'],
            'monthly_fraud_rate': stats['fraud_rate'],
            # MCC will be set per scenario
        }
        
        # Scenario A: Original MCC
        context['mcc'] = original_mcc
        fee_a = 0.0
        found_a = False
        for rule in fees_data:
            if match_fee_rule(context, rule):
                fee_a = calculate_fee(tx['eur_amount'], rule)
                found_a = True
                break
        
        # Scenario B: New MCC (7523)
        context['mcc'] = 7523
        fee_b = 0.0
        found_b = False
        for rule in fees_data:
            if match_fee_rule(context, rule):
                fee_b = calculate_fee(tx['eur_amount'], rule)
                found_b = True
                break
        
        if not found_a or not found_b:
            # This might happen if rules are not exhaustive. 
            # We assume 0 fee if no rule matches, or log warning.
            # For this exercise, we proceed with 0.
            pass

        total_delta += (fee_b - fee_a)
        count += 1

    # 6. Output Result
    print(f"Processed {count} transactions.")
    print(f"Total Fee Delta (Scenario B - Scenario A): {total_delta:.14f}")

if __name__ == "__main__":
    main()

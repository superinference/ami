# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2550
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 9720 characters (FULL CODE)
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
        # Handle percentages
        if '%' in v:
            v = v.replace('%', '')
            try:
                return float(v) / 100.0
            except ValueError:
                return 0.0
        # Handle simple numbers
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range_string(range_str):
    """
    Parses a range string into (min, max).
    Examples:
      '100k-1m' -> (100000, 1000000)
      '>5' -> (5, inf)
      '<3' -> (-inf, 3)
      '3-5' -> (3, 5)
      '0%-0.8%' -> (0.0, 0.008)
    """
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    # Helper to parse single number with suffixes
    def parse_num(n_s):
        n_s = n_s.strip()
        mult = 1.0
        if n_s.endswith('%'):
            n_s = n_s[:-1]
            mult = 0.01
        elif n_s.endswith('k'):
            n_s = n_s[:-1]
            mult = 1000.0
        elif n_s.endswith('m'):
            n_s = n_s[:-1]
            mult = 1000000.0
        return float(n_s) * mult

    try:
        if '-' in s:
            parts = s.split('-')
            return parse_num(parts[0]), parse_num(parts[1])
        elif s.startswith('>='):
            return parse_num(s[2:]), float('inf')
        elif s.startswith('>'):
            return parse_num(s[1:]) + 1e-9, float('inf') # slightly more than
        elif s.startswith('<='):
            return float('-inf'), parse_num(s[2:])
        elif s.startswith('<'):
            return float('-inf'), parse_num(s[1:]) - 1e-9 # slightly less than
        elif s == 'immediate':
            return 0.0, 0.0
        elif s == 'manual':
            # Manual is conceptually infinite delay, but handled by string check usually.
            # If numeric comparison needed, treat as very high.
            return 9999.0, 9999.0 
        else:
            # Exact match treated as range [x, x]
            val = parse_num(s)
            return val, val
    except:
        return None, None

def check_range_match(value, rule_range_str):
    """
    Checks if a numeric value fits within a rule's range string.
    Handles mixed types (string vs numeric) for fields like capture_delay.
    """
    if rule_range_str is None:
        return True
    
    # Special handling for capture_delay keywords
    if rule_range_str in ['immediate', 'manual']:
        return str(value) == rule_range_str
    
    # If rule is numeric range but value is string (e.g. 'manual'), no match
    # Unless value can be cast to float
    try:
        if str(value) == 'immediate':
            num_val = 0.0
        elif str(value) == 'manual':
            num_val = 9999.0
        else:
            num_val = float(value)
    except (ValueError, TypeError):
        return False

    min_v, max_v = parse_range_string(rule_range_str)
    if min_v is None:
        return False # Parse error
        
    return min_v <= num_val <= max_v

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    """
    # 1. Card Scheme (Exact)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False

    # 2. Account Type (List)
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False

    # 3. Capture Delay (Range/String)
    if rule.get('capture_delay'):
        if not check_range_match(tx_ctx['capture_delay'], rule['capture_delay']):
            return False

    # 4. Merchant Category Code (List)
    if rule.get('merchant_category_code'):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False

    # 5. Is Credit (Bool)
    if rule.get('is_credit') is not None:
        # Rule might be boolean or string "True"/"False"
        r_cred = rule['is_credit']
        if isinstance(r_cred, str):
            r_cred = (r_cred.lower() == 'true')
        if r_cred != tx_ctx['is_credit']:
            return False

    # 6. ACI (List)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 7. Intracountry (Bool/Float)
    if rule.get('intracountry') is not None:
        # Calculate transaction intracountry status
        tx_intra = (tx_ctx['issuing_country'] == tx_ctx['acquirer_country'])
        
        # Parse rule value
        r_intra = rule['intracountry']
        if isinstance(r_intra, str):
            if r_intra.lower() == 'true': r_intra = True
            elif r_intra.lower() == 'false': r_intra = False
            else:
                try:
                    r_intra = bool(float(r_intra))
                except:
                    pass
        elif isinstance(r_intra, (int, float)):
            r_intra = bool(r_intra)
            
        if r_intra != tx_intra:
            return False

    # 8. Monthly Volume (Range)
    if rule.get('monthly_volume'):
        if not check_range_match(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range)
    if rule.get('monthly_fraud_level'):
        if not check_range_match(tx_ctx['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False

    return True

# ---------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------

def main():
    # Paths
    payments_path = '/output/chunk2/data/context/payments.csv'
    fees_path = '/output/chunk2/data/context/fees.json'
    merchant_path = '/output/chunk2/data/context/merchant_data.json'

    # Load Data
    try:
        df = pd.read_csv(payments_path)
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
        with open(merchant_path, 'r') as f:
            merchant_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # Filter for Rafa_AI and 2023
    target_merchant = 'Rafa_AI'
    df_rafa = df[(df['merchant'] == target_merchant) & (df['year'] == 2023)].copy()
    
    if df_rafa.empty:
        print("No transactions found for Rafa_AI in 2023.")
        return

    # Get Merchant Attributes
    m_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not m_info:
        print(f"Merchant {target_merchant} not found in merchant_data.json")
        return

    original_mcc = m_info['merchant_category_code']
    account_type = m_info['account_type']
    capture_delay = m_info['capture_delay']

    # Calculate Monthly Stats
    # Convert day_of_year to month (2023 is not a leap year)
    df_rafa['month'] = pd.to_datetime(df_rafa['year'] * 1000 + df_rafa['day_of_year'], format='%Y%j').dt.month

    # Group by month to calculate volume and fraud rate per month
    monthly_groups = df_rafa.groupby('month')
    monthly_stats = {}
    
    for month, group in monthly_groups:
        total_vol = group['eur_amount'].sum()
        fraud_vol = group[group['has_fraudulent_dispute'] == True]['eur_amount'].sum()
        fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
        monthly_stats[month] = {
            'volume': total_vol,
            'fraud_rate': fraud_rate
        }

    # Function to calculate total fees for a given MCC
    def calculate_fees(target_mcc):
        total_fees = 0.0
        
        # Iterate through each transaction
        for _, row in df_rafa.iterrows():
            month = row['month']
            stats = monthly_stats.get(month)
            
            # Context for rule matching
            ctx = {
                'card_scheme': row['card_scheme'],
                'account_type': account_type,
                'capture_delay': capture_delay,
                'mcc': target_mcc,
                'is_credit': row['is_credit'],
                'aci': row['aci'],
                'issuing_country': row['issuing_country'],
                'acquirer_country': row['acquirer_country'],
                'monthly_volume': stats['volume'],
                'monthly_fraud_rate': stats['fraud_rate']
            }
            
            matched_rule = None
            # Find the first matching rule
            for rule in fees_data:
                if match_fee_rule(ctx, rule):
                    matched_rule = rule
                    break
            
            if matched_rule:
                # Fee = fixed_amount + (rate * transaction_value / 10000)
                fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * row['eur_amount'] / 10000.0)
                total_fees += fee
            else:
                # If no rule matches, assume 0 fee (or log if needed)
                pass
            
        return total_fees

    # Calculate Original Fees
    fees_orig = calculate_fees(original_mcc)

    # Calculate New Fees (MCC 5911)
    fees_new = calculate_fees(5911)

    # Calculate Delta
    delta = fees_new - fees_orig
    
    # Output Result
    print(f"Original MCC: {original_mcc}")
    print(f"Original Fees: {fees_orig:.4f}")
    print(f"New Fees (MCC 5911): {fees_new:.4f}")
    print(f"Fee Delta: {delta:.14f}")

if __name__ == "__main__":
    main()

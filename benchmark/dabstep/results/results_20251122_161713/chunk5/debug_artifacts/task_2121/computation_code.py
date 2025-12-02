import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """
    Convert string with %, $, commas, k, m to float. 
    Handles common formats found in fee rules.
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        
        # Handle percentages
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100.0
            except ValueError:
                return 0.0
        
        # Handle suffixes k (thousand) and m (million)
        multiplier = 1.0
        if 'k' in v:
            multiplier = 1000.0
            v = v.replace('k', '')
        elif 'm' in v:
            multiplier = 1000000.0
            v = v.replace('m', '')
            
        # Handle comparison operators
        v_clean = v.lstrip('><≤≥=')
        
        try:
            return float(v_clean) * multiplier
        except ValueError:
            # Handle ranges like "100-200" by taking average (fallback, though usually ranges are parsed in parse_range_check)
            if '-' in v_clean:
                parts = v_clean.split('-')
                if len(parts) == 2:
                    try:
                        return ((float(parts[0]) + float(parts[1])) / 2) * multiplier
                    except ValueError:
                        return 0.0
            return 0.0
    return 0.0

def parse_range_check(value, rule_str):
    """
    Checks if a numeric value fits within a rule string (e.g., '>5', '100k-1m', '5.5%').
    """
    if rule_str is None:
        return True
    
    # If value is string (e.g. 'manual'), do direct string comparison
    if isinstance(value, str) and not value.replace('.','',1).isdigit():
        return value.lower() == str(rule_str).lower()

    # Ensure value is float
    try:
        num_val = float(value)
    except (ValueError, TypeError):
        return False

    s = str(rule_str).strip().lower()
    
    # Helper to parse a number from the string using our robust coercer
    def parse_num(n_str):
        return coerce_to_float(n_str)

    if '-' in s:
        # Range: "100-200" or "7.7%-8.3%"
        parts = s.split('-')
        if len(parts) == 2:
            low = parse_num(parts[0])
            high = parse_num(parts[1])
            return low <= num_val <= high
    elif s.startswith('>='):
        limit = parse_num(s[2:])
        return num_val >= limit
    elif s.startswith('>'):
        limit = parse_num(s[1:])
        return num_val > limit
    elif s.startswith('<='):
        limit = parse_num(s[2:])
        return num_val <= limit
    elif s.startswith('<'):
        limit = parse_num(s[1:])
        return num_val < limit
    else:
        # Exact match (numeric)
        limit = parse_num(s)
        # Use a small epsilon for float comparison if needed, or direct equality
        return num_val == limit
    
    return False

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a transaction context matches a fee rule.
    tx_ctx: dict containing transaction and merchant details
    rule: dict containing fee rule details
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx.get('card_scheme'):
        return False

    # 2. Account Type (List match)
    # Rule has list of allowed types. If empty/None, allows all.
    if rule.get('account_type'):
        if tx_ctx.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        # tx_ctx['merchant_category_code'] is int, rule['merchant_category_code'] is list of ints
        if tx_ctx.get('merchant_category_code') not in rule['merchant_category_code']:
            return False

    # 4. Authorization Characteristics Indicator (ACI) (List match)
    if rule.get('aci'):
        if tx_ctx.get('aci') not in rule['aci']:
            return False

    # 5. Is Credit (Boolean match)
    # If rule is None, applies to both.
    if rule.get('is_credit') is not None:
        # Ensure strict boolean comparison
        if bool(rule['is_credit']) != bool(tx_ctx.get('is_credit')):
            return False

    # 6. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        # Intracountry means Issuer Country == Acquirer Country
        is_intra = (tx_ctx.get('issuing_country') == tx_ctx.get('acquirer_country'))
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    # 7. Capture Delay (Range/String match)
    if rule.get('capture_delay'):
        if not parse_range_check(tx_ctx.get('capture_delay'), rule['capture_delay']):
            return False

    # 8. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_ctx.get('monthly_fraud_level'), rule['monthly_fraud_level']):
            return False

    # 9. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_ctx.get('monthly_volume'), rule['monthly_volume']):
            return False

    return True

# ---------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------

def main():
    # File paths
    payments_path = '/output/chunk5/data/context/payments.csv'
    fees_path = '/output/chunk5/data/context/fees.json'
    merchant_path = '/output/chunk5/data/context/merchant_data.json'
    
    # 1. Load Data
    try:
        df_payments = pd.read_csv(payments_path)
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
        with open(merchant_path, 'r') as f:
            merchant_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Filter for Merchant and Timeframe (Feb 2023)
    target_merchant = "Golfclub_Baron_Friso"
    
    # Filter for the specific merchant
    df_merchant_all = df_payments[df_payments['merchant'] == target_merchant]
    
    # Filter for February 2023
    # 2023 is not a leap year.
    # Jan: 1-31
    # Feb: 32-59
    df_feb = df_merchant_all[
        (df_merchant_all['year'] == 2023) & 
        (df_merchant_all['day_of_year'] >= 32) & 
        (df_merchant_all['day_of_year'] <= 59)
    ].copy()

    if df_feb.empty:
        print("No transactions found for this merchant in Feb 2023.")
        return

    # 3. Calculate Dynamic Merchant Stats (Volume & Fraud) for Feb 2023
    # These stats apply to ALL transactions in that month for rule matching
    monthly_volume = df_feb['eur_amount'].sum()
    
    fraud_count = df_feb['has_fraudulent_dispute'].sum()
    total_count = len(df_feb)
    monthly_fraud_rate = fraud_count / total_count if total_count > 0 else 0.0
    
    # 4. Get Static Merchant Attributes
    # Find merchant in merchant_data.json
    merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print(f"Merchant {target_merchant} not found in merchant_data.json")
        return

    # 5. Get Target Fee Rule (ID=65)
    target_fee_id = 65
    target_rule = next((r for r in fees_data if r['ID'] == target_fee_id), None)
    
    if not target_rule:
        print(f"Fee rule ID {target_fee_id} not found.")
        return

    original_rate = target_rule['rate']
    new_rate = 1  # As per question
    
    # 6. Identify Matching Transactions
    matching_amount_sum = 0.0
    match_count = 0

    for _, row in df_feb.iterrows():
        # Build transaction context
        tx_ctx = {
            'card_scheme': row['card_scheme'],
            'account_type': merchant_info['account_type'],
            'merchant_category_code': merchant_info['merchant_category_code'],
            'is_credit': row['is_credit'],
            'aci': row['aci'],
            'issuing_country': row['issuing_country'],
            'acquirer_country': row['acquirer_country'],
            'capture_delay': merchant_info['capture_delay'],
            'monthly_volume': monthly_volume,
            'monthly_fraud_level': monthly_fraud_rate
        }
        
        if match_fee_rule(tx_ctx, target_rule):
            matching_amount_sum += row['eur_amount']
            match_count += 1

    # 7. Calculate Delta
    # Formula: Fee = Fixed + (Rate * Amount / 10000)
    # Delta Fee = (New Rate - Old Rate) * Amount / 10000
    # Total Delta = (New Rate - Old Rate) * Sum(Amounts) / 10000
    
    delta = (new_rate - original_rate) * matching_amount_sum / 10000.0
    
    # 8. Print Result with High Precision
    print(f"{delta:.14f}")

if __name__ == "__main__":
    main()
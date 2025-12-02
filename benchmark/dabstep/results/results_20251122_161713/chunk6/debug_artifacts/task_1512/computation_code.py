import pandas as pd
import json
import re

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v:
            return float(v.replace('k', '')) * 1_000
        if 'm' in v:
            return float(v.replace('m', '')) * 1_000_000
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def is_not_empty(array):
    """Check if array/list is not empty."""
    if array is None:
        return False
    if hasattr(array, 'size'):
        return array.size > 0
    return len(array) > 0

def parse_range_check(actual_val, rule_str):
    """
    Checks if actual_val falls within the rule_str range.
    Handles: '100k-1m', '>5', '<3', '7.7%-8.3%', 'immediate', 'manual'
    """
    if rule_str is None:
        return True
        
    # Handle exact string matches (e.g., 'immediate', 'manual')
    if isinstance(actual_val, str) and not actual_val.replace('.', '', 1).isdigit():
        return actual_val.lower() == rule_str.lower()
    
    # If actual_val is numeric (or numeric string), parse rule
    try:
        val = float(actual_val)
    except ValueError:
        # If actual is non-numeric string (e.g. 'manual') but rule is numeric (e.g. '>5')
        return False

    # Handle inequalities
    if rule_str.startswith('>'):
        limit = coerce_to_float(rule_str[1:])
        return val > limit
    if rule_str.startswith('<'):
        limit = coerce_to_float(rule_str[1:])
        return val < limit
        
    # Handle ranges (e.g., '100k-1m', '0.0%-0.5%')
    if '-' in rule_str:
        parts = rule_str.split('-')
        if len(parts) == 2:
            lower = coerce_to_float(parts[0])
            upper = coerce_to_float(parts[1])
            return lower <= val <= upper
            
    # Handle exact numeric match (rare in this dataset but possible)
    return val == coerce_to_float(rule_str)

def match_fee_rule(ctx, rule):
    """
    Determines if a fee rule applies to the given transaction context.
    ctx: dict containing transaction/merchant details
    rule: dict containing fee rule criteria
    """
    # 1. Card Scheme (Must match)
    if rule['card_scheme'] != ctx['card_scheme']:
        return False

    # 2. Account Type (List - Wildcard if empty)
    if is_not_empty(rule['account_type']):
        if ctx['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List - Wildcard if empty)
    if is_not_empty(rule['merchant_category_code']):
        if ctx['mcc'] not in rule['merchant_category_code']:
            return False

    # 4. ACI (List - Wildcard if empty)
    if is_not_empty(rule['aci']):
        if ctx['aci'] not in rule['aci']:
            return False

    # 5. Is Credit (Bool - Wildcard if None)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != ctx['is_credit']:
            return False

    # 6. Intracountry (Bool/Float - Wildcard if None)
    if rule['intracountry'] is not None:
        # JSON uses 0.0/1.0 for bools often
        rule_intra = bool(rule['intracountry'])
        if rule_intra != ctx['intracountry']:
            return False

    # 7. Capture Delay (String/Range - Wildcard if None)
    if rule['capture_delay'] is not None:
        if not parse_range_check(ctx['capture_delay'], rule['capture_delay']):
            return False

    # 8. Monthly Volume (Range - Wildcard if None)
    if rule['monthly_volume'] is not None:
        if not parse_range_check(ctx['monthly_volume'], rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range - Wildcard if None)
    if rule['monthly_fraud_level'] is not None:
        if not parse_range_check(ctx['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee: fixed_amount + (rate * amount / 10000)"""
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000)

# ---------------------------------------------------------
# MAIN ANALYSIS
# ---------------------------------------------------------

def main():
    # 1. Load Data
    payments_path = '/output/chunk6/data/context/payments.csv'
    fees_path = '/output/chunk6/data/context/fees.json'
    merchant_path = '/output/chunk6/data/context/merchant_data.json'
    
    df_payments = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)

    # 2. Determine "Average Scenario" Parameters (Modes)
    # ACI
    aci_mode = df_payments['aci'].mode()[0]
    
    # Is Credit
    is_credit_mode = df_payments['is_credit'].mode()[0]
    
    # Intracountry (Issuing == Acquirer)
    # We calculate the boolean series first, then get the mode
    is_intra_series = df_payments['issuing_country'] == df_payments['acquirer_country']
    intracountry_mode = is_intra_series.mode()[0]
    
    # Most Frequent Merchant
    merchant_mode = df_payments['merchant'].mode()[0]
    
    print(f"Average Scenario Parameters:")
    print(f"  Merchant: {merchant_mode}")
    print(f"  ACI: {aci_mode}")
    print(f"  Is Credit: {is_credit_mode}")
    print(f"  Intracountry: {intracountry_mode}")

    # 3. Get Merchant Specifics (Static & Dynamic)
    # Static Data from merchant_data.json
    m_info = next((m for m in merchant_data if m['merchant'] == merchant_mode), None)
    if not m_info:
        print(f"Error: Merchant {merchant_mode} not found in merchant_data.json")
        return

    account_type = m_info['account_type']
    mcc = m_info['merchant_category_code']
    capture_delay = m_info['capture_delay']

    # Dynamic Data from payments.csv (Volume & Fraud)
    # Filter for the specific merchant
    merchant_txs = df_payments[df_payments['merchant'] == merchant_mode]
    
    # Monthly Volume: Total Volume / 12 (Assuming 2023 is full year data)
    total_volume = merchant_txs['eur_amount'].sum()
    monthly_volume = total_volume / 12.0
    
    # Monthly Fraud Level: Fraud Volume / Total Volume
    # Manual: "Fraud is defined as the ratio of fraudulent volume over total volume."
    fraud_volume = merchant_txs[merchant_txs['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    monthly_fraud_level = fraud_volume / total_volume if total_volume > 0 else 0.0
    
    print(f"  Account Type: {account_type}")
    print(f"  MCC: {mcc}")
    print(f"  Capture Delay: {capture_delay}")
    print(f"  Monthly Volume: €{monthly_volume:,.2f}")
    print(f"  Monthly Fraud Rate: {monthly_fraud_level:.2%}")

    # 4. Calculate Fees for Each Scheme
    transaction_amount = 500.0
    schemes = set(r['card_scheme'] for r in fees_data)
    
    scheme_fees = {}
    
    # Context for matching
    context = {
        'aci': aci_mode,
        'is_credit': is_credit_mode,
        'intracountry': intracountry_mode,
        'account_type': account_type,
        'mcc': mcc,
        'capture_delay': capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level
    }

    for scheme in schemes:
        context['card_scheme'] = scheme
        
        # Find matching rule
        # We iterate through fees and take the first match. 
        # (Assuming fees.json is ordered or rules are mutually exclusive for a given context)
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(context, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            fee = calculate_fee(transaction_amount, matched_rule)
            scheme_fees[scheme] = fee
            # print(f"  {scheme}: €{fee:.4f} (Rule ID: {matched_rule['ID']})")
        else:
            # print(f"  {scheme}: No matching rule found")
            pass

    # 5. Identify Most Expensive
    if scheme_fees:
        most_expensive_scheme = max(scheme_fees, key=scheme_fees.get)
        max_fee = scheme_fees[most_expensive_scheme]
        print("-" * 30)
        print(f"Result: {most_expensive_scheme}")
    else:
        print("No applicable fees found for any scheme.")

if __name__ == "__main__":
    main()
import pandas as pd
import json
import numpy as np

# --- Helper Functions ---
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

def parse_range_check(value, rule_string):
    """
    Checks if a numeric value fits within a rule string.
    Rule strings: '100k-1m', '>5', '<3', '0%-0.5%', 'immediate', 'manual'
    """
    if rule_string is None:
        return True
    
    # Handle string literals (e.g., 'manual', 'immediate')
    # If the rule is a specific string like "manual", we require an exact match
    if isinstance(rule_string, str) and not any(c.isdigit() for c in rule_string):
        return str(value) == rule_string

    # Convert value to float for comparison
    try:
        val_float = float(value)
    except (ValueError, TypeError):
        # If value is a string (e.g. 'manual') and rule is numeric/range, it doesn't match
        return False

    rs = str(rule_string).strip()
    
    # Helper to parse number with k/m/%
    def parse_num(s):
        s = s.strip()
        factor = 1.0
        if '%' in s:
            factor = 0.01
            s = s.replace('%', '')
        elif 'k' in s.lower():
            factor = 1000.0
            s = s.lower().replace('k', '')
        elif 'm' in s.lower():
            factor = 1000000.0
            s = s.lower().replace('m', '')
        return float(s) * factor

    try:
        if '-' in rs:
            low_str, high_str = rs.split('-')
            low = parse_num(low_str)
            high = parse_num(high_str)
            return low <= val_float <= high
        elif rs.startswith('>'):
            limit = parse_num(rs[1:])
            return val_float > limit
        elif rs.startswith('<'):
            limit = parse_num(rs[1:])
            return val_float < limit
        else:
            # Exact match numeric string
            return val_float == parse_num(rs)
    except:
        return False

def match_fee_rule(ctx, rule):
    """
    Checks if a fee rule applies to the transaction context.
    ctx: dict containing transaction/merchant details
    rule: dict containing fee rule details
    """
    # 1. Card Scheme
    if rule.get('card_scheme') != ctx.get('card_scheme'):
        return False

    # 2. Account Type (List or Wildcard)
    if rule.get('account_type'):
        if ctx.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List or Wildcard)
    if rule.get('merchant_category_code'):
        if ctx.get('mcc') not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Bool or Wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != ctx.get('is_credit'):
            return False

    # 5. ACI (List or Wildcard)
    if rule.get('aci'):
        if ctx.get('aci') not in rule['aci']:
            return False

    # 6. Intracountry (0.0/1.0 or Wildcard)
    if rule.get('intracountry') is not None:
        # Rule uses 0.0/1.0, ctx uses True/False
        # Convert rule float to bool (0.0 -> False, 1.0 -> True)
        rule_intra = bool(rule['intracountry'])
        if rule_intra != ctx.get('intracountry'):
            return False

    # 7. Capture Delay (String/Range or Wildcard)
    if rule.get('capture_delay'):
        if not parse_range_check(ctx.get('capture_delay'), rule['capture_delay']):
            return False

    # 8. Monthly Volume (Range or Wildcard)
    if rule.get('monthly_volume'):
        if not parse_range_check(ctx.get('monthly_volume'), rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range or Wildcard)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(ctx.get('monthly_fraud_level'), rule['monthly_fraud_level']):
            return False

    return True

def calculate_fee(amount, rule):
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    return fixed + (rate * amount / 10000.0)

# --- Main Analysis ---
def analyze():
    # Load Data
    payments = pd.read_csv('/output/chunk5/data/context/payments.csv')
    with open('/output/chunk5/data/context/fees.json', 'r') as f:
        fees = json.load(f)
    with open('/output/chunk5/data/context/merchant_data.json', 'r') as f:
        merchants = json.load(f)

    # 1. Determine Average Scenario Parameters
    
    # Global Modes (Most common transaction characteristics)
    mode_is_credit = payments['is_credit'].mode()[0]
    mode_aci = payments['aci'].mode()[0]
    
    # Intracountry Mode
    # Compare issuing_country and acquirer_country
    is_intra = payments['issuing_country'] == payments['acquirer_country']
    mode_intracountry = is_intra.mode()[0]
    
    # Most Frequent Merchant (Defines the merchant profile for the scenario)
    mode_merchant_name = payments['merchant'].mode()[0]
    
    # Merchant Specifics
    # Get merchant metadata
    merchant_info = next((m for m in merchants if m['merchant'] == mode_merchant_name), None)
    if not merchant_info:
        print(f"Error: Merchant {mode_merchant_name} not found in merchant_data.json")
        return

    mcc = merchant_info['merchant_category_code']
    account_type = merchant_info['account_type']
    capture_delay = merchant_info['capture_delay']
    
    # Calculate Merchant Volume & Fraud
    # Filter payments for this merchant
    merchant_txs = payments[payments['merchant'] == mode_merchant_name]
    
    # Monthly Volume: Total EUR / 12 (Assuming 2023 is full year)
    total_volume = merchant_txs['eur_amount'].sum()
    monthly_volume = total_volume / 12.0
    
    # Monthly Fraud Level: Fraud Volume / Total Volume (as per manual)
    fraud_volume = merchant_txs[merchant_txs['has_fraudulent_dispute']]['eur_amount'].sum()
    monthly_fraud_level = fraud_volume / total_volume if total_volume > 0 else 0.0

    print(f"Scenario Parameters (Average Scenario):")
    print(f"  Merchant: {mode_merchant_name}")
    print(f"  Is Credit: {mode_is_credit}")
    print(f"  ACI: {mode_aci}")
    print(f"  Intracountry: {mode_intracountry}")
    print(f"  MCC: {mcc}")
    print(f"  Account Type: {account_type}")
    print(f"  Capture Delay: {capture_delay}")
    print(f"  Monthly Volume: {monthly_volume:,.2f}")
    print(f"  Monthly Fraud Level: {monthly_fraud_level:.4%}")

    # 2. Evaluate Fees for Target Transaction
    target_amount = 4321.0
    schemes = ['NexPay', 'GlobalCard', 'SwiftCharge', 'TransactPlus']
    
    results = {}
    
    # Context for matching
    ctx = {
        'account_type': account_type,
        'mcc': mcc,
        'is_credit': mode_is_credit,
        'aci': mode_aci,
        'intracountry': mode_intracountry,
        'capture_delay': capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level
    }

    for scheme in schemes:
        ctx['card_scheme'] = scheme
        
        # Find matching rules
        matches = []
        for rule in fees:
            if match_fee_rule(ctx, rule):
                matches.append(rule)
        
        if not matches:
            print(f"No matching rule found for {scheme}")
            results[scheme] = float('inf')
            continue
            
        # Calculate fee using the matches. 
        # If multiple rules match, we take the minimum fee (assuming best optimization)
        fees_calculated = [calculate_fee(target_amount, m) for m in matches]
        min_fee = min(fees_calculated)
        results[scheme] = min_fee

    # 3. Determine Cheapest
    cheapest_scheme = min(results, key=results.get)
    cheapest_fee = results[cheapest_scheme]

    print("-" * 30)
    print(f"Fees for 4321 EUR transaction:")
    for s, f in results.items():
        print(f"  {s}: {f:.4f} EUR")
    
    print("-" * 30)
    print(f"Cheapest Scheme: {cheapest_scheme}")
    
    # Final Answer Output
    print(cheapest_scheme)

if __name__ == "__main__":
    analyze()
import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
    if pd.isna(value) or value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    
    s = str(value).strip().lower()
    s = s.replace(',', '').replace('€', '').replace('$', '').replace('_', '')
    s = s.replace('>', '').replace('<', '').replace('≥', '').replace('≤', '')
    
    try:
        if '%' in s:
            return float(s.replace('%', '')) / 100.0
        if 'k' in s:
            return float(s.replace('k', '')) * 1000
        if 'm' in s:
            return float(s.replace('m', '')) * 1_000_000
        return float(s)
    except ValueError:
        return 0.0

def parse_range(range_str):
    """Parses range strings like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max)."""
    if pd.isna(range_str) or range_str is None:
        return (float('-inf'), float('inf'))
    
    s = str(range_str).strip().lower()
    
    # Handle percentages
    is_percent = '%' in s
    
    # Handle greater/less than
    if s.startswith('>'):
        val = coerce_to_float(s)
        return (val, float('inf'))
    if s.startswith('<'):
        val = coerce_to_float(s)
        return (float('-inf'), val)
        
    # Handle ranges "min-max"
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            return (coerce_to_float(parts[0]), coerce_to_float(parts[1]))
            
    # Handle exact match as range [x, x]
    val = coerce_to_float(s)
    return (val, val)

def check_range(value, range_str):
    """Checks if a value falls within a parsed range string."""
    if pd.isna(range_str) or range_str is None:
        return True
    min_val, max_val = parse_range(range_str)
    return min_val <= value <= max_val

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    tx_ctx: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Must match the simulation target)
    if rule.get('card_scheme') != tx_ctx['target_scheme']:
        return False

    # 2. Merchant Category Code (List match or wildcard)
    if rule.get('merchant_category_code'):
        # If rule has specific MCCs, merchant must be one of them
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False

    # 3. Account Type (List match or wildcard)
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False

    # 4. Capture Delay (Exact match or wildcard)
    if rule.get('capture_delay'):
        r_delay = str(rule['capture_delay']).lower()
        m_delay = str(tx_ctx['capture_delay']).lower()
        
        # If rule is a range (e.g. >5), try to parse merchant delay
        if any(x in r_delay for x in ['>', '<', '-']):
            try:
                # If merchant delay is numeric string '1', '7'
                delay_days = float(m_delay)
                min_d, max_d = parse_range(r_delay)
                if not (min_d <= delay_days <= max_d):
                    return False
            except ValueError:
                # Merchant delay is 'manual'/'immediate', rule is numeric range -> No match
                return False
        else:
            # Exact string match (e.g. 'manual' == 'manual')
            if r_delay != m_delay:
                return False

    # 5. Monthly Volume (Range check)
    if rule.get('monthly_volume'):
        if not check_range(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False

    # 6. Monthly Fraud Level (Range check)
    if rule.get('monthly_fraud_level'):
        if not check_range(tx_ctx['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False

    # 7. Is Credit (Boolean match or wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 8. ACI (List match or wildcard)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 9. Intracountry (Boolean match or wildcard)
    if rule.get('intracountry') is not None:
        # Intracountry is float 0.0 or 1.0 in JSON sometimes, convert to bool
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['is_intracountry']:
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee: fixed_amount + (rate * amount / 10000)."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    return fixed + (rate * amount / 10000.0)

# ---------------------------------------------------------
# MAIN ANALYSIS
# ---------------------------------------------------------

def main():
    # 1. Load Data
    try:
        df_payments = pd.read_csv('/output/chunk4/data/context/payments.csv')
        with open('/output/chunk4/data/context/merchant_data.json', 'r') as f:
            merchant_data = json.load(f)
        with open('/output/chunk4/data/context/fees.json', 'r') as f:
            fees_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Filter for Merchant 'Crossfit_Hanna' and Year 2023
    merchant_name = 'Crossfit_Hanna'
    target_year = 2023
    
    df_merchant = df_payments[
        (df_payments['merchant'] == merchant_name) & 
        (df_payments['year'] == target_year)
    ].copy()
    
    if df_merchant.empty:
        print(f"No transactions found for {merchant_name} in {target_year}")
        return

    # 3. Get Merchant Profile
    merchant_profile = next((item for item in merchant_data if item["merchant"] == merchant_name), None)
    if not merchant_profile:
        print(f"Merchant profile not found for {merchant_name}")
        return
        
    mcc = merchant_profile.get('merchant_category_code')
    account_type = merchant_profile.get('account_type')
    capture_delay = merchant_profile.get('capture_delay')

    # 4. Calculate Monthly Stats (Global for 2023)
    # Note: Monthly volume is Total Volume / 12
    total_volume = df_merchant['eur_amount'].sum()
    monthly_volume = total_volume / 12.0
    
    # Fraud Rate: Fraud Volume / Total Volume
    fraud_volume = df_merchant[df_merchant['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    monthly_fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0
    
    print(f"--- Stats for {merchant_name} ---")
    print(f"Monthly Volume: €{monthly_volume:,.2f}")
    print(f"Fraud Rate: {monthly_fraud_rate:.4%}")
    print(f"MCC: {mcc}, Account: {account_type}, Delay: {capture_delay}")

    # 5. Identify Available Card Schemes
    # We want to test ALL schemes present in the fees file to see which is cheapest
    available_schemes = set(rule['card_scheme'] for rule in fees_data if rule.get('card_scheme'))
    
    # 6. Simulation Loop
    scheme_costs = {}

    # Pre-calculate transaction contexts to avoid re-looping overhead
    # We create a list of dicts for the transactions
    transactions_ctx = []
    for _, tx in df_merchant.iterrows():
        is_intra = (tx['issuing_country'] == tx['acquirer_country'])
        transactions_ctx.append({
            'amount': tx['eur_amount'],
            'is_credit': bool(tx['is_credit']),
            'aci': tx['aci'],
            'is_intracountry': is_intra
        })

    for scheme in available_schemes:
        total_scheme_fee = 0.0
        matched_tx_count = 0
        
        # Filter fees for this scheme
        scheme_rules = [r for r in fees_data if r.get('card_scheme') == scheme]
        
        # Context for matching (mix of merchant stats and transaction specifics)
        base_ctx = {
            'target_scheme': scheme,
            'mcc': mcc,
            'account_type': account_type,
            'capture_delay': capture_delay,
            'monthly_volume': monthly_volume,
            'monthly_fraud_rate': monthly_fraud_rate
        }
        
        valid_scheme = True
        
        for tx_ctx in transactions_ctx:
            # Update context with transaction specifics
            ctx = base_ctx.copy()
            ctx.update({
                'is_credit': tx_ctx['is_credit'],
                'aci': tx_ctx['aci'],
                'is_intracountry': tx_ctx['is_intracountry']
            })
            
            # Find matching rule
            matched_rule = None
            for rule in scheme_rules:
                if match_fee_rule(ctx, rule):
                    matched_rule = rule
                    break
            
            if matched_rule:
                fee = calculate_fee(tx_ctx['amount'], matched_rule)
                total_scheme_fee += fee
                matched_tx_count += 1
            else:
                # If a transaction cannot be matched to a rule, this scheme might not support it.
                # In a real scenario, this would be a blocker. 
                # For this exercise, if coverage is low, we discard the scheme.
                pass
        
        # Only consider schemes that cover the vast majority of transactions (>99%)
        # or simply track the coverage.
        coverage = matched_tx_count / len(df_merchant)
        
        if coverage > 0.99:
            scheme_costs[scheme] = total_scheme_fee
            print(f"Scheme: {scheme}, Total Fee: €{total_scheme_fee:,.2f}, Coverage: {coverage:.1%}")
        else:
            print(f"Scheme: {scheme} discarded due to low coverage ({coverage:.1%})")

    # 7. Determine Winner
    if scheme_costs:
        best_scheme = min(scheme_costs, key=scheme_costs.get)
        min_fee = scheme_costs[best_scheme]
        print(f"\nBest Scheme: {best_scheme} (Total Fee: €{min_fee:,.2f})")
        print(best_scheme) # Final Answer
    else:
        print("No valid schemes found.")

if __name__ == "__main__":
    main()
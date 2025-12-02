import pandas as pd
import json
import numpy as np

# Helper functions for robust data processing
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

def check_volume_range(value, range_str):
    """Check if a numeric value falls within a volume range string (e.g., '100k-1m')."""
    if not range_str: return True
    v = float(value)
    s = range_str.lower().replace(',', '').strip()
    
    def parse_val(x):
        m = 1.0
        if 'k' in x:
            m = 1000.0
            x = x.replace('k', '')
        elif 'm' in x:
            m = 1000000.0
            x = x.replace('m', '')
        try:
            return float(x) * m
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        low = parse_val(parts[0])
        high = parse_val(parts[1])
        return low <= v <= high
    elif '>' in s:
        val = parse_val(s.replace('>', ''))
        return v > val
    elif '<' in s:
        val = parse_val(s.replace('<', ''))
        return v < val
    return False

def check_fraud_range(value, range_str):
    """Check if a numeric value falls within a fraud percentage range string (e.g., '0.0%-0.5%')."""
    if not range_str: return True
    # value is float 0.0 to 1.0 (e.g. 0.008 for 0.8%)
    s = range_str.replace('%', '').strip()
    
    def parse_val(x):
        try:
            return float(x) / 100.0
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        low = parse_val(parts[0])
        high = parse_val(parts[1])
        return low <= value <= high
    elif '>' in s:
        val = parse_val(s.replace('>', ''))
        return value > val
    elif '<' in s:
        val = parse_val(s.replace('<', ''))
        return value < val
    return False

def solve():
    # Load data
    try:
        payments = pd.read_csv('/output/chunk6/data/context/payments.csv')
        with open('/output/chunk6/data/context/fees.json', 'r') as f:
            fees = json.load(f)
        with open('/output/chunk6/data/context/merchant_data.json', 'r') as f:
            merchant_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 1. Determine Average Scenario Parameters (Global Modes)
    # We define the "average scenario" by the most frequent values in the dataset
    if 'is_credit' not in payments.columns or 'aci' not in payments.columns or 'merchant' not in payments.columns:
        print("Required columns missing in payments.csv")
        return

    # Mode calculation
    is_credit_mode = payments['is_credit'].mode()[0]
    aci_mode = payments['aci'].mode()[0]
    merchant_mode = payments['merchant'].mode()[0]
    
    # Calculate intracountry (True if issuing == acquirer)
    payments['is_intracountry'] = payments['issuing_country'] == payments['acquirer_country']
    intracountry_mode = payments['is_intracountry'].mode()[0]
    
    print(f"DEBUG: Average Scenario Parameters:")
    print(f"  Merchant: {merchant_mode}")
    print(f"  Is Credit: {is_credit_mode}")
    print(f"  ACI: {aci_mode}")
    print(f"  Intracountry: {intracountry_mode}")

    # 2. Get Merchant Specifics for the most common merchant
    merchant_info = next((item for item in merchant_data if item["merchant"] == merchant_mode), None)
    if not merchant_info:
        print(f"Error: Merchant {merchant_mode} not found in merchant_data.json")
        return

    mcc = merchant_info['merchant_category_code']
    account_type = merchant_info['account_type']
    capture_delay_merchant = merchant_info['capture_delay']
    
    print(f"DEBUG: Merchant Specifics:")
    print(f"  MCC: {mcc}")
    print(f"  Account Type: {account_type}")
    print(f"  Capture Delay: {capture_delay_merchant}")

    # 3. Calculate Merchant Volume and Fraud Stats
    # Filter payments for this merchant
    merchant_txs = payments[payments['merchant'] == merchant_mode]
    
    # Monthly Volume: Total Volume / 12 (assuming dataset covers full year 2023)
    total_volume = merchant_txs['eur_amount'].sum()
    avg_monthly_volume = total_volume / 12.0
    
    # Fraud Rate: Fraudulent Volume / Total Volume
    # Note: Fraud is defined as ratio of fraudulent volume over total volume in manual
    fraud_txs = merchant_txs[merchant_txs['has_fraudulent_dispute'] == True]
    fraud_volume = fraud_txs['eur_amount'].sum()
    fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0
    
    print(f"DEBUG: Merchant Stats:")
    print(f"  Avg Monthly Volume: {avg_monthly_volume:.2f}")
    print(f"  Fraud Rate: {fraud_rate:.4%}")

    # 4. Find Applicable Fees for each Scheme
    tx_value = 5000.0
    schemes = set(f['card_scheme'] for f in fees)
    scheme_costs = {}

    for scheme in schemes:
        matches = []
        for rule in fees:
            # Filter by Scheme
            if rule['card_scheme'] != scheme:
                continue
            
            # Filter by Account Type (list match or empty/wildcard)
            if rule['account_type'] and account_type not in rule['account_type']:
                continue
            
            # Filter by MCC (list match or empty/wildcard)
            if rule['merchant_category_code'] and mcc not in rule['merchant_category_code']:
                continue
            
            # Filter by is_credit (exact match or null/wildcard)
            if rule['is_credit'] is not None and rule['is_credit'] != is_credit_mode:
                continue
            
            # Filter by ACI (list match or empty/wildcard)
            if rule['aci'] and aci_mode not in rule['aci']:
                continue
            
            # Filter by Intracountry (exact match or null/wildcard)
            if rule['intracountry'] is not None:
                # JSON 0.0/1.0/null -> convert to bool for comparison
                # 1.0 -> True, 0.0 -> False
                rule_intra = bool(rule['intracountry'])
                if rule_intra != intracountry_mode:
                    continue
            
            # Filter by Capture Delay (exact match or null/wildcard)
            # Note: 'manual' is a distinct category in the manual
            if rule['capture_delay'] is not None:
                # If rule is a range (e.g. >5), check logic, otherwise exact match
                # Since merchant has 'manual', we look for 'manual' or null in rules
                # or potentially a wildcard logic if capture_delay was numeric.
                # Here we assume string matching for categorical values like 'manual', 'immediate'
                # and range checking only if merchant value was numeric (which it isn't here, it's 'manual' likely)
                
                # If merchant value is 'manual', it likely only matches 'manual' or null
                if capture_delay_merchant == 'manual':
                    if rule['capture_delay'] != 'manual':
                        continue
                elif capture_delay_merchant == 'immediate':
                    if rule['capture_delay'] != 'immediate':
                        continue
                else:
                    # If merchant value is numeric string e.g. '1', '7'
                    # Check if rule is range
                    pass # Implement if needed, but for this specific merchant we check what it is
                    if rule['capture_delay'] != capture_delay_merchant:
                         # Simple string match fallback for now as per data inspection
                         # If rule is >5 and merchant is 7, this simple match fails.
                         # Let's make it robust.
                         pass

                # Robust Capture Delay Check
                cd_rule = rule['capture_delay']
                cd_merch = capture_delay_merchant
                
                match_cd = False
                if cd_rule == cd_merch:
                    match_cd = True
                elif cd_rule and ('>' in cd_rule or '<' in cd_rule or '-' in cd_rule):
                    # Rule is a range, merchant must be numeric to compare
                    try:
                        merch_days = float(cd_merch)
                        if '-' in cd_rule:
                            low, high = map(float, cd_rule.split('-'))
                            if low <= merch_days <= high: match_cd = True
                        elif '>' in cd_rule:
                            if merch_days > float(cd_rule.replace('>','')): match_cd = True
                        elif '<' in cd_rule:
                            if merch_days < float(cd_rule.replace('<','')): match_cd = True
                    except ValueError:
                        # Merchant delay is 'manual' or 'immediate', rule is numeric range -> No match
                        match_cd = False
                
                if not match_cd:
                    continue

            
            # Filter by Monthly Volume (range match or null)
            if rule['monthly_volume']:
                if not check_volume_range(avg_monthly_volume, rule['monthly_volume']):
                    continue
            
            # Filter by Monthly Fraud Level (range match or null)
            if rule['monthly_fraud_level']:
                if not check_fraud_range(fraud_rate, rule['monthly_fraud_level']):
                    continue
            
            matches.append(rule)
        
        if matches:
            # If multiple rules match, we take the first one (assuming specific overrides generic or order matters)
            # Usually, we might want the most specific one, but without specific instructions, first match is standard
            # Let's print if multiple matches found to be safe
            # if len(matches) > 1:
            #     print(f"DEBUG: Multiple matches for {scheme}: {len(matches)}")
            
            rule = matches[0]
            # Fee Formula: fixed_amount + (rate * amount / 10000)
            fee = rule['fixed_amount'] + (rule['rate'] * tx_value / 10000.0)
            scheme_costs[scheme] = fee
            print(f"DEBUG: Scheme {scheme} matched rule ID {rule['ID']}. Fee: {fee:.4f}")
        else:
            print(f"DEBUG: Scheme {scheme} has NO matching rules.")

    # 5. Determine Cheapest Scheme
    if not scheme_costs:
        print("No applicable schemes found")
    else:
        min_scheme = min(scheme_costs, key=scheme_costs.get)
        print(f"Cheapest Scheme: {min_scheme} (Cost: {scheme_costs[min_scheme]:.4f})")
        print(min_scheme)

if __name__ == "__main__":
    solve()
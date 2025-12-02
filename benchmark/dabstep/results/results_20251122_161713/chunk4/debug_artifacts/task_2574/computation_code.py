import pandas as pd
import json

# ═══════════════════════════════════════════════════════════
# Helper Functions
# ═══════════════════════════════════════════════════════════
def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
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

def is_not_empty(array):
    """Check if array/list is not empty."""
    if array is None:
        return False
    if hasattr(array, 'size'):
        return array.size > 0
    try:
        return len(array) > 0
    except TypeError:
        return False

# ═══════════════════════════════════════════════════════════
# Main Analysis Script
# ═══════════════════════════════════════════════════════════

# 1. Load Data
fees_path = '/output/chunk4/data/context/fees.json'
merchant_path = '/output/chunk4/data/context/merchant_data.json'
payments_path = '/output/chunk4/data/context/payments.csv'

try:
    # Load Fees
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    
    # Find Fee ID 787
    fee_rule = next((item for item in fees_data if item["ID"] == 787), None)
    
    if not fee_rule:
        print("Error: Fee ID 787 not found.")
    else:
        # Load Merchant Data
        with open(merchant_path, 'r') as f:
            merchant_data = json.load(f)
        
        # Create lookup dictionary: merchant_name -> {account_type, mcc}
        merchant_lookup = {
            m['merchant']: {
                'account_type': m.get('account_type'), 
                'mcc': m.get('merchant_category_code')
            } 
            for m in merchant_data
        }

        # Load Payments
        df = pd.read_csv(payments_path)
        
        # 2. Filter Transactions based on Fee 787's Transaction Criteria
        # Criteria: card_scheme, is_credit, aci, etc.
        
        # Filter by Card Scheme (if specified in rule)
        if fee_rule.get('card_scheme'):
            df = df[df['card_scheme'] == fee_rule['card_scheme']]
            
        # Filter by Credit Status (if specified in rule)
        # Note: JSON uses boolean true/false/null. CSV uses boolean True/False.
        if fee_rule.get('is_credit') is not None:
            df = df[df['is_credit'] == fee_rule['is_credit']]
            
        # Filter by ACI (if specified in rule as a list)
        # Rule: if rule['aci'] is not empty, transaction 'aci' must be in it
        if is_not_empty(fee_rule.get('aci')):
            df = df[df['aci'].isin(fee_rule['aci'])]
            
        # Filter by Intracountry (if specified)
        if fee_rule.get('intracountry') is not None:
            # Intracountry means issuing_country == acquirer_country
            is_intra = df['issuing_country'] == df['acquirer_country']
            if fee_rule['intracountry']:
                df = df[is_intra]
            else:
                df = df[~is_intra]

        # 3. Identify Merchants matching Merchant-Level Criteria
        # We have a set of transactions that match the technical specs of the fee.
        # Now we check which of the associated merchants match the NEW hypothetical rule:
        # Rule: Account Type MUST be 'F' (and MCC must match original rule if exists)
        
        affected_merchants = set()
        
        # Get unique merchants from the filtered transactions
        candidate_merchants = df['merchant'].unique()
        
        for merchant in candidate_merchants:
            info = merchant_lookup.get(merchant)
            if not info:
                continue
                
            # Check 1: Hypothetical Condition - Account Type must be 'F'
            if info['account_type'] != 'F':
                continue
                
            # Check 2: Original Rule Condition - MCC (if rule has specific MCCs)
            rule_mccs = fee_rule.get('merchant_category_code')
            if is_not_empty(rule_mccs):
                if info['mcc'] not in rule_mccs:
                    continue
            
            # If all checks pass, this merchant is affected
            affected_merchants.add(merchant)
            
        # 4. Output Result
        if affected_merchants:
            # Sort for consistent output
            print(", ".join(sorted(list(affected_merchants))))
        else:
            print("No merchants found matching the criteria.")

except Exception as e:
    print(f"An error occurred: {e}")
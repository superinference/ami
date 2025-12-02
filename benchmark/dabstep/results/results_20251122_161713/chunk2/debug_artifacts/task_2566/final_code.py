import pandas as pd
import json

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
            except ValueError:
                pass
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range_check(value, rule_value):
    """
    Checks if a numeric value satisfies a rule string (e.g., ">5", "100k-1m", "8%").
    """
    if rule_value is None:
        return True
    
    # Handle simple equality for strings (e.g., "immediate")
    if isinstance(rule_value, str) and not any(c in rule_value for c in ['<', '>', '-', '%', 'k', 'm']):
        return str(value) == rule_value

    # Parse value to float
    val_float = coerce_to_float(value)
    
    # Parse rule
    rule_str = str(rule_value).strip()
    
    # Handle ranges (e.g., "100k-1m", "7.7%-8.3%")
    if '-' in rule_str:
        parts = rule_str.split('-')
        if len(parts) == 2:
            # Convert k/m suffixes
            def parse_suffix(s):
                s = s.lower().replace('%', '')
                mult = 1
                if 'k' in s: mult = 1000; s = s.replace('k', '')
                if 'm' in s: mult = 1000000; s = s.replace('m', '')
                try:
                    return float(s) * mult
                except:
                    return 0
            
            min_val = parse_suffix(parts[0])
            max_val = parse_suffix(parts[1])
            
            # Adjust for percentages if the rule string contained %
            if '%' in rule_str:
                min_val /= 100
                max_val /= 100
                
            return min_val <= val_float <= max_val

    # Handle inequalities
    if rule_str.startswith('>'):
        limit = coerce_to_float(rule_str[1:])
        return val_float > limit
    if rule_str.startswith('<'):
        limit = coerce_to_float(rule_str[1:])
        return val_float < limit
        
    return val_float == coerce_to_float(rule_str)

def match_fee_rule(tx_data, rule):
    """
    Determines if a transaction matches a fee rule.
    tx_data: dict containing transaction and merchant details
    rule: dict containing fee rule criteria
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_data.get('card_scheme'):
        return False
        
    # 2. Account Type (List in rule)
    if rule.get('account_type'):
        # If rule has specific account types, tx must match one of them
        # If rule['account_type'] is empty [], it matches ALL (Wildcard)
        if tx_data.get('account_type') not in rule['account_type']:
            return False
            
    # 3. Capture Delay (String/Range in rule)
    if rule.get('capture_delay'):
        if not parse_range_check(tx_data.get('capture_delay'), rule['capture_delay']):
            return False
            
    # 4. Monthly Fraud Level (Range in rule)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_data.get('monthly_fraud_level'), rule['monthly_fraud_level']):
            return False
            
    # 5. Monthly Volume (Range in rule)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_data.get('monthly_volume'), rule['monthly_volume']):
            return False
            
    # 6. Merchant Category Code (List in rule)
    if rule.get('merchant_category_code'):
        if tx_data.get('merchant_category_code') not in rule['merchant_category_code']:
            return False
            
    # 7. Is Credit (Bool in rule)
    if rule.get('is_credit') is not None:
        # Handle string/bool mismatch
        rule_credit = str(rule['is_credit']).lower() == 'true'
        tx_credit = str(tx_data.get('is_credit')).lower() == 'true'
        if rule_credit != tx_credit:
            return False
            
    # 8. ACI (List in rule)
    if rule.get('aci'):
        if tx_data.get('aci') not in rule['aci']:
            return False
            
    # 9. Intracountry (Bool in rule)
    if rule.get('intracountry') is not None:
        # Handle string/bool/float mismatch (0.0/1.0)
        rule_intra = float(rule['intracountry']) == 1.0 if isinstance(rule['intracountry'], (int, float)) else str(rule['intracountry']).lower() == 'true'
        tx_intra = tx_data.get('intracountry', False)
        if rule_intra != tx_intra:
            return False
            
    return True

# ---------------------------------------------------------
# MAIN ANALYSIS
# ---------------------------------------------------------

def main():
    # 1. Load Data
    try:
        fees = json.load(open('/output/chunk2/data/context/fees.json'))
        merchant_data = json.load(open('/output/chunk2/data/context/merchant_data.json'))
        payments = pd.read_csv('/output/chunk2/data/context/payments.csv')
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Get Fee Rule ID 17
    fee_17 = next((f for f in fees if f['ID'] == 17), None)
    if not fee_17:
        print("Fee ID 17 not found.")
        return

    # 3. Prepare Merchant Info Map
    # merchant_name -> {account_type, mcc, capture_delay}
    merch_info = {m['merchant']: m for m in merchant_data}

    # 4. Calculate Merchant Stats (Volume & Fraud) for 2023
    # Fee rules often depend on monthly volume/fraud. We'll use average monthly stats for 2023.
    merchant_stats = {}
    for merchant in payments['merchant'].unique():
        txs = payments[payments['merchant'] == merchant]
        if txs.empty:
            continue
            
        total_vol = txs['eur_amount'].sum()
        fraud_count = txs['has_fraudulent_dispute'].sum()
        tx_count = len(txs)
        
        # Average monthly volume (Total / 12)
        avg_monthly_vol = total_vol / 12.0
        
        # Fraud rate (Count / Total Count)
        fraud_rate = (fraud_count / tx_count) if tx_count > 0 else 0.0
        
        merchant_stats[merchant] = {
            'monthly_volume': avg_monthly_vol,
            'monthly_fraud_level': fraud_rate
        }

    # 5. Identify Affected Merchants
    # Logic: 
    # - A merchant is affected if they CURRENTLY match Fee 17...
    # - ...BUT their account_type is NOT 'D'.
    # - If their account_type IS 'D', the new rule (only applied to D) still covers them (no change).
    
    affected_merchants = set()

    for merchant in payments['merchant'].unique():
        # Get merchant static data
        m_data = merch_info.get(merchant)
        if not m_data:
            continue
            
        account_type = m_data.get('account_type')
        
        # If account type is 'D', they are NOT affected by the restriction "only applied to D".
        # They matched before (wildcard) and they match now ('D').
        if account_type == 'D':
            continue
            
        # Check if merchant currently uses Fee 17
        # We check if ANY of their transactions match the ORIGINAL Fee 17 criteria.
        
        # Get merchant's transactions
        txs = payments[payments['merchant'] == merchant]
        
        # Optimization: Pre-filter by static Fee 17 fields to reduce iteration
        if fee_17.get('card_scheme'):
            txs = txs[txs['card_scheme'] == fee_17['card_scheme']]
        
        # Handle is_credit (careful with types)
        if fee_17.get('is_credit') is not None:
            target_credit = str(fee_17['is_credit']).lower() == 'true'
            txs = txs[txs['is_credit'] == target_credit]
            
        if txs.empty:
            continue
            
        # Check dynamic fields row by row until a match is found
        matches_fee_17 = False
        
        # Get stats for this merchant
        stats = merchant_stats.get(merchant, {'monthly_volume': 0, 'monthly_fraud_level': 0})
        
        for _, tx in txs.iterrows():
            # Build the check dictionary combining transaction, merchant, and stats data
            check_data = {
                'card_scheme': tx['card_scheme'],
                'account_type': account_type,
                'capture_delay': m_data.get('capture_delay'),
                'monthly_fraud_level': stats['monthly_fraud_level'],
                'monthly_volume': stats['monthly_volume'],
                'merchant_category_code': m_data.get('merchant_category_code'),
                'is_credit': tx['is_credit'],
                'aci': tx['aci'],
                'intracountry': (tx['issuing_country'] == tx['acquirer_country'])
            }
            
            # Check against ORIGINAL Fee 17 (which has empty account_type list)
            if match_fee_rule(check_data, fee_17):
                matches_fee_17 = True
                break # Found a matching transaction, so this merchant uses Fee 17
        
        # If they used Fee 17 but are NOT type 'D', they are affected (they lose the fee)
        if matches_fee_17:
            affected_merchants.add(merchant)

    # 6. Output Result
    if not affected_merchants:
        print("No merchants affected.")
    else:
        # Sort for consistent output
        print(", ".join(sorted(list(affected_merchants))))

if __name__ == "__main__":
    main()
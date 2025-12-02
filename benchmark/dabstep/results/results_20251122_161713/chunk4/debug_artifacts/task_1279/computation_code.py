import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

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
            except:
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
        except:
            return 0.0
    return 0.0

def parse_rule_value(val_str):
    """Parses a single value string like '100k', '0.5%', '5' into float for comparison."""
    if not isinstance(val_str, str):
        return float(val_str) if val_str is not None else 0.0
    
    s = val_str.strip().lower()
    multiplier = 1
    if '%' in s:
        multiplier = 0.01
        s = s.replace('%', '')
    elif 'k' in s:
        multiplier = 1000
        s = s.replace('k', '')
    elif 'm' in s:
        multiplier = 1000000
        s = s.replace('m', '')
        
    try:
        return float(s) * multiplier
    except:
        return 0.0

def check_numeric_rule(actual_val, rule_str):
    """Checks if actual_val fits the rule_str (e.g. '100k-1m', '>5', '0.0%-0.5%')."""
    if rule_str is None:
        return True
    
    s = str(rule_str).strip()
    
    try:
        if '-' in s:
            parts = s.split('-')
            low = parse_rule_value(parts[0])
            high = parse_rule_value(parts[1])
            # Use a small epsilon for float comparison if needed, but direct comparison usually fine here
            return low <= actual_val <= high
        elif s.startswith('>'):
            limit = parse_rule_value(s[1:])
            return actual_val > limit
        elif s.startswith('<'):
            limit = parse_rule_value(s[1:])
            return actual_val < limit
        else:
            # Exact match numeric
            return actual_val == parse_rule_value(s)
    except:
        return False

def check_capture_delay(actual_val, rule_str):
    """Specific checker for capture delay which mixes strings and numbers."""
    if rule_str is None:
        return True
    
    s_rule = str(rule_str).strip().lower()
    s_act = str(actual_val).strip().lower()
    
    # Direct string match (e.g. 'immediate', 'manual')
    if s_rule == s_act:
        return True
        
    # If actual is numeric (e.g. '1', '7'), check against numeric rules
    if s_act.isdigit():
        act_float = float(s_act)
        # Rule might be '>5', '3-5', '<3'
        if any(x in s_rule for x in ['<', '>', '-']):
            return check_numeric_rule(act_float, s_rule)
            
    return False

def get_month(doy):
    """Maps day_of_year (1-365) to month (1-12)."""
    # Cumulative days at start of each month for non-leap year
    days = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365]
    for i in range(1, len(days)):
        if doy <= days[i]:
            return i
    return 12

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    # fee = fixed_amount + rate * transaction_value / 10000
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000.0)

# ═══════════════════════════════════════════════════════════
# MAIN LOGIC
# ═══════════════════════════════════════════════════════════

def main():
    # 1. Load Data
    try:
        payments = pd.read_csv('/output/chunk4/data/context/payments.csv')
        with open('/output/chunk4/data/context/fees.json', 'r') as f:
            fees = json.load(f)
        with open('/output/chunk4/data/context/merchant_data.json', 'r') as f:
            merchant_data = json.load(f)
    except Exception as e:
        print(f"Error loading files: {e}")
        return

    # 2. Prepare Merchant Data Lookup
    merchants_dict = {m['merchant']: m for m in merchant_data}

    # 3. Prepare Monthly Stats (Volume and Fraud Rate)
    # Add month column
    payments['month'] = payments['day_of_year'].apply(get_month)
    
    # Calculate fraud amount per transaction (0 if not fraud)
    # Manual: "Fraud is defined as the ratio of fraudulent volume over total volume."
    payments['fraud_amt'] = np.where(payments['has_fraudulent_dispute'] == True, payments['eur_amount'], 0.0)
    
    # Group by merchant and month to get totals
    stats = payments.groupby(['merchant', 'month']).agg({
        'eur_amount': 'sum',
        'fraud_amt': 'sum'
    }).reset_index()
    
    stats.rename(columns={'eur_amount': 'total_vol', 'fraud_amt': 'fraud_vol'}, inplace=True)
    
    # Calculate fraud rate (ratio)
    stats['fraud_rate'] = stats.apply(lambda x: x['fraud_vol'] / x['total_vol'] if x['total_vol'] > 0 else 0.0, axis=1)
    
    # Create lookup dict: (merchant, month) -> {vol, rate}
    stats_lookup = {}
    for _, row in stats.iterrows():
        stats_lookup[(row['merchant'], row['month'])] = {
            'vol': row['total_vol'],
            'rate': row['fraud_rate']
        }

    # 4. Filter Target Transactions
    # Question: "For credit transactions, what would be the average fee that the card scheme SwiftCharge..."
    target_df = payments[
        (payments['card_scheme'] == 'SwiftCharge') & 
        (payments['is_credit'] == True)
    ].copy()
    
    if target_df.empty:
        print("No matching transactions found.")
        return

    # 5. Calculate Fee for each transaction
    calculated_fees = []
    
    # Pre-filter fees to optimize (only SwiftCharge rules)
    swift_fees = [r for r in fees if r['card_scheme'] == 'SwiftCharge']
    
    for _, tx in target_df.iterrows():
        merchant = tx['merchant']
        m_data = merchants_dict.get(merchant)
        
        if not m_data:
            continue
            
        # Get monthly stats for this specific transaction's merchant and month
        month = tx['month']
        m_stats = stats_lookup.get((merchant, month), {'vol': 0, 'rate': 0})
        
        # Build Context for Rule Matching
        ctx = {
            'card_scheme': tx['card_scheme'],
            'is_credit': tx['is_credit'], # This is True
            'aci': tx['aci'],
            'intracountry': tx['issuing_country'] == tx['acquirer_country'],
            'mcc': m_data['merchant_category_code'],
            'account_type': m_data['account_type'],
            'capture_delay': m_data['capture_delay'],
            'monthly_volume': m_stats['vol'],
            'monthly_fraud_level': m_stats['rate']
        }
        
        # Find First Matching Fee Rule
        matched_rule = None
        for rule in swift_fees:
            # Check Scheme (Already filtered, but good for safety)
            if rule['card_scheme'] != ctx['card_scheme']: continue
            
            # Check Credit (Boolean or None)
            # Rule is_credit: True (matches credit), False (matches debit), None (matches both)
            # We are processing credit txs, so we need rule['is_credit'] to be True or None.
            if rule['is_credit'] is not None and rule['is_credit'] != ctx['is_credit']: continue
            
            # Check Intracountry (Boolean or None)
            if rule['intracountry'] is not None:
                rule_intra = bool(rule['intracountry'])
                if rule_intra != ctx['intracountry']: continue
            
            # Check MCC (List or None)
            if rule['merchant_category_code'] is not None:
                if ctx['mcc'] not in rule['merchant_category_code']: continue
                
            # Check Account Type (List or None)
            if rule['account_type'] is not None:
                if ctx['account_type'] not in rule['account_type']: continue
                
            # Check ACI (List or None)
            if rule['aci'] is not None:
                if ctx['aci'] not in rule['aci']: continue
                
            # Check Capture Delay (String/Range or None)
            if not check_capture_delay(ctx['capture_delay'], rule['capture_delay']): continue
            
            # Check Volume (Range String or None)
            if not check_numeric_rule(ctx['monthly_volume'], rule['monthly_volume']): continue
            
            # Check Fraud (Range String or None)
            if not check_numeric_rule(ctx['monthly_fraud_level'], rule['monthly_fraud_level']): continue
            
            # If all checks pass, we found our rule
            matched_rule = rule
            break
            
        if matched_rule:
            # Calculate Fee for hypothetical 50 EUR transaction
            fee = calculate_fee(50.0, matched_rule)
            calculated_fees.append(fee)
            
    # 6. Calculate and Print Average
    if calculated_fees:
        avg_fee = sum(calculated_fees) / len(calculated_fees)
        # Print with high precision
        print(f"{avg_fee:.14f}")
    else:
        print("No fees calculated")

if __name__ == "__main__":
    main()
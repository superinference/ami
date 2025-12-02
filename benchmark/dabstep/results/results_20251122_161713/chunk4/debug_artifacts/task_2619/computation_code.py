import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

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
    return float(value) if value is not None else 0.0

def parse_range_check(value, range_str):
    """
    Checks if a numeric value falls within a range string.
    Handles: '100k-1m', '>8.3%', '<3', 'immediate', 'manual', null.
    """
    if range_str is None:
        return True
        
    s = str(range_str).lower().strip()
    
    # Handle categorical matches explicitly first (exact string match)
    if s in ['immediate', 'manual']:
        return s == str(value).lower()
        
    # Clean up value for numeric comparison
    # Map categorical values to numeric proxies for range comparison if needed
    val_num = value
    if isinstance(value, str):
        if value.lower() == 'immediate': val_num = 0.0
        elif value.lower() == 'manual': val_num = 999.0 # Treat as > any normal day count
        else:
            try:
                val_num = float(value)
            except:
                return False # Cannot compare arbitrary string to numeric range
    
    # Clean up range string
    s_clean = s.replace(',', '').replace('€', '').replace('$', '')
    is_pct = '%' in s_clean
    s_clean = s_clean.replace('%', '')
    
    # Multipliers for k (thousands) and m (millions)
    def parse_val(v):
        m = 1.0
        if 'k' in v: m = 1000.0; v = v.replace('k', '')
        if 'm' in v: m = 1000000.0; v = v.replace('m', '')
        try:
            return float(v) * m
        except:
            return 0.0

    # Scale adjustment: 
    # If range was percentage (e.g. 8.3%), parse_val returns 8.3.
    # If value is ratio (e.g. 0.09), we need to compare 0.09 to 0.083.
    scale = 0.01 if is_pct else 1.0

    if '-' in s_clean:
        parts = s_clean.split('-')
        if len(parts) == 2:
            low = parse_val(parts[0]) * scale
            high = parse_val(parts[1]) * scale
            return low <= val_num <= high
    elif '>' in s_clean:
        low = parse_val(s_clean.replace('>', '')) * scale
        return val_num > low
    elif '<' in s_clean:
        high = parse_val(s_clean.replace('<', '')) * scale
        return val_num < high
    else:
        # Exact numeric match
        target = parse_val(s_clean) * scale
        return val_num == target
    
    return False

def match_fee_rule(ctx, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    """
    # 1. Card Scheme (Exact Match)
    if rule['card_scheme'] != ctx['card_scheme']:
        return False
        
    # 2. Account Type (List or Null)
    if rule['account_type'] is not None and len(rule['account_type']) > 0:
        if ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List or Null)
    if rule['merchant_category_code'] is not None and len(rule['merchant_category_code']) > 0:
        if ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (String/Range or Null)
    if not parse_range_check(ctx['capture_delay'], rule['capture_delay']):
        return False
            
    # 5. Monthly Volume (Range or Null)
    if not parse_range_check(ctx['monthly_volume'], rule['monthly_volume']):
        return False
        
    # 6. Monthly Fraud Level (Range or Null)
    if not parse_range_check(ctx['monthly_fraud_level'], rule['monthly_fraud_level']):
        return False
        
    # 7. Is Credit (Bool or Null)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != ctx['is_credit']:
            return False
            
    # 8. ACI (List or Null)
    if rule['aci'] is not None and len(rule['aci']) > 0:
        if ctx['aci'] not in rule['aci']:
            return False
            
    # 9. Intracountry (Bool/Float or Null)
    if rule['intracountry'] is not None:
        # Rule might be 0.0, 1.0, True, False
        rule_val = bool(rule['intracountry'])
        if rule_val != ctx['intracountry']:
            return False
            
    return True

# ---------------------------------------------------------
# MAIN ANALYSIS
# ---------------------------------------------------------

def main():
    # Load Data
    payments_path = '/output/chunk4/data/context/payments.csv'
    fees_path = '/output/chunk4/data/context/fees.json'
    merchant_path = '/output/chunk4/data/context/merchant_data.json'

    df = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees = json.load(f)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)

    # 1. Filter for Crossfit_Hanna in May (Days 121-151)
    merchant_name = 'Crossfit_Hanna'
    df_merchant = df[df['merchant'] == merchant_name].copy()
    
    # May is roughly day 121 to 151 (non-leap year: Jan=31, Feb=28, Mar=31, Apr=30 -> 120 days. May 1 is 121)
    df_may = df_merchant[(df_merchant['day_of_year'] >= 121) & (df_merchant['day_of_year'] <= 151)].copy()
    
    if df_may.empty:
        print("No transactions found for Crossfit_Hanna in May.")
        return

    # 2. Calculate Merchant Stats for May
    monthly_volume = df_may['eur_amount'].sum()
    
    fraud_txs = df_may[df_may['has_fraudulent_dispute'] == True]
    fraud_volume = fraud_txs['eur_amount'].sum()
    monthly_fraud_level = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0
    
    # Get Merchant Static Data
    m_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
    if not m_info:
        print(f"Merchant {merchant_name} not found in merchant_data.json")
        return

    # 3. Simulate Fees for Each Scheme
    schemes = ['GlobalCard', 'NexPay', 'SwiftCharge', 'TransactPlus']
    scheme_totals = {}

    for scheme in schemes:
        total_fee = 0.0
        
        for _, tx in df_may.iterrows():
            # Determine transaction context
            # Intracountry: Issuer == Acquirer
            is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
            
            ctx = {
                'card_scheme': scheme, # SIMULATED SCHEME
                'account_type': m_info['account_type'],
                'mcc': m_info['merchant_category_code'],
                'capture_delay': m_info['capture_delay'],
                'monthly_volume': monthly_volume,
                'monthly_fraud_level': monthly_fraud_level,
                'is_credit': tx['is_credit'],
                'aci': tx['aci'],
                'intracountry': is_intracountry,
                'amount': tx['eur_amount']
            }
            
            # Find matching fee rule
            # We take the first rule that matches the context
            matched_rule = None
            for rule in fees:
                if match_fee_rule(ctx, rule):
                    matched_rule = rule
                    break
            
            if matched_rule:
                # Fee = Fixed + (Rate * Amount / 10000)
                fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * ctx['amount'] / 10000.0)
                total_fee += fee
            else:
                # If no rule matches, we assume 0 fee (or could be an error, but 0 is safe for summation)
                pass
                
        scheme_totals[scheme] = total_fee

    # 4. Find the Maximum
    max_scheme = max(scheme_totals, key=scheme_totals.get)
    
    # Output the result
    print(max_scheme)

if __name__ == "__main__":
    main()
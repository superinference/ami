import pandas as pd
import json

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

def parse_range_check(rule_val_str, target_val):
    """
    Checks if target_val falls within the range defined by rule_val_str.
    Handles ranges (100k-1m), inequalities (>5), and keywords (manual).
    """
    if rule_val_str is None:
        return True
    
    s = str(rule_val_str).strip().lower()
    
    # Helper to parse k/m suffixes
    def parse_num(n_str):
        n_str = n_str.replace('%', '')
        mult = 1
        if 'k' in n_str:
            mult = 1000
            n_str = n_str.replace('k', '')
        elif 'm' in n_str:
            mult = 1000000
            n_str = n_str.replace('m', '')
        return float(n_str) * mult

    is_percent = '%' in s
    
    # Handle Ranges (e.g., "100k-1m")
    if '-' in s:
        try:
            parts = s.split('-')
            low = parse_num(parts[0])
            high = parse_num(parts[1])
            if is_percent:
                low /= 100
                high /= 100
            return low <= target_val <= high
        except:
            return False
    
    # Handle Inequalities
    if s.startswith('>'):
        val = parse_num(s[1:])
        if is_percent: val /= 100
        return target_val > val
        
    if s.startswith('<'):
        val = parse_num(s[1:])
        if is_percent: val /= 100
        return target_val < val
        
    # Handle Keywords
    if s == 'immediate':
        return target_val == 0
    if s == 'manual':
        return target_val == 999  # Sentinel for manual
        
    # Exact match
    try:
        val = parse_num(s)
        if is_percent: val /= 100
        return target_val == val
    except:
        return False

def match_fee_rule(tx_ctx, rule):
    """Determines if a fee rule applies to a specific transaction context."""
    
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List match)
    if rule['account_type'] and tx_ctx['account_type'] not in rule['account_type']:
        return False
        
    # 3. Merchant Category Code (List match)
    if rule['merchant_category_code'] and tx_ctx['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 4. Is Credit (Boolean match)
    if rule['is_credit'] is not None:
        rule_credit = str(rule['is_credit']).lower() == 'true'
        if rule_credit != tx_ctx['is_credit']:
            return False
            
    # 5. ACI (List match)
    if rule['aci'] and tx_ctx['aci'] not in rule['aci']:
        return False
        
    # 6. Intracountry (Boolean match)
    if rule['intracountry'] is not None:
        # JSON often uses 0.0/1.0 for bools
        rule_intra = bool(float(rule['intracountry']))
        if rule_intra != tx_ctx['intracountry']:
            return False
            
    # 7. Monthly Volume (Range match)
    if rule['monthly_volume']:
        if not parse_range_check(rule['monthly_volume'], tx_ctx['monthly_volume']):
            return False
            
    # 8. Monthly Fraud Level (Range match)
    if rule['monthly_fraud_level']:
        if not parse_range_check(rule['monthly_fraud_level'], tx_ctx['monthly_fraud_rate']):
            return False
            
    # 9. Capture Delay (Complex match)
    if rule['capture_delay']:
        m_delay = tx_ctx['capture_delay']
        delay_val = -1
        if m_delay == 'immediate': delay_val = 0
        elif m_delay == 'manual': delay_val = 999
        else: 
            try: delay_val = float(m_delay)
            except: pass
            
        if not parse_range_check(rule['capture_delay'], delay_val):
            return False
            
    return True

def execute_analysis():
    # Load Data
    payments = pd.read_csv('/output/chunk5/data/context/payments.csv')
    with open('/output/chunk5/data/context/fees.json') as f:
        fees = json.load(f)
    with open('/output/chunk5/data/context/merchant_data.json') as f:
        merchant_data = json.load(f)
        
    # Filter for Belles_cookbook_store in July 2023 (Days 182-212)
    target_merchant = 'Belles_cookbook_store'
    df = payments[
        (payments['merchant'] == target_merchant) &
        (payments['day_of_year'] >= 182) &
        (payments['day_of_year'] <= 212) &
        (payments['year'] == 2023)
    ].copy()
    
    if df.empty:
        print("No transactions found.")
        return

    # Calculate Monthly Stats (Volume & Fraud Rate)
    monthly_vol = df['eur_amount'].sum()
    fraud_vol = df[df['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    monthly_fraud_rate = fraud_vol / monthly_vol if monthly_vol > 0 else 0.0
    
    # Get Merchant Static Data
    m_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not m_info:
        print("Merchant data not found.")
        return
        
    # Get Rule 384 details
    rule_384 = next((r for r in fees if r['ID'] == 384), None)
    if not rule_384:
        print("Rule 384 not found.")
        return
    
    rate_old = rule_384['rate']
    rate_new = 1
    
    # Prepare Context for Matching
    base_ctx = {
        'account_type': m_info['account_type'],
        'mcc': m_info['merchant_category_code'],
        'capture_delay': m_info['capture_delay'],
        'monthly_volume': monthly_vol,
        'monthly_fraud_rate': monthly_fraud_rate
    }
    
    affected_amount = 0.0
    
    # Iterate transactions to find matches
    for _, tx in df.iterrows():
        ctx = base_ctx.copy()
        ctx['card_scheme'] = tx['card_scheme']
        ctx['is_credit'] = tx['is_credit']
        ctx['aci'] = tx['aci']
        ctx['intracountry'] = (tx['issuing_country'] == tx['acquirer_country'])
        
        # Find the FIRST matching rule (Priority Order)
        matched_id = None
        for rule in fees:
            if match_fee_rule(ctx, rule):
                matched_id = rule['ID']
                break
        
        # If this transaction uses Rule 384, add to affected volume
        if matched_id == 384:
            affected_amount += tx['eur_amount']
            
    # Calculate Delta
    # Delta = (New Rate - Old Rate) * Amount / 10000
    delta = (rate_new - rate_old) * affected_amount / 10000
    
    # Print with high precision
    print(f"{delta:.14f}")

if __name__ == "__main__":
    execute_analysis()
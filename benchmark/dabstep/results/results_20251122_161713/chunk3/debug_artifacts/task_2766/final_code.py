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
        v = v.lstrip('><≤≥')  # Remove comparison operators
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
        except:
            return 0.0
    return float(value) if value is not None else 0.0

def parse_volume_check(rule_vol, actual_vol):
    """Check if actual volume falls within the rule's volume range string (e.g., '100k-1m')."""
    if rule_vol is None: return True
    v = str(rule_vol).lower().replace(',', '')
    try:
        if '-' in v:
            low, high = v.split('-')
            l = float(low.replace('k', '000').replace('m', '000000'))
            h = float(high.replace('k', '000').replace('m', '000000'))
            return l <= actual_vol <= h
        elif '>' in v:
            val = float(v.replace('>', '').replace('k', '000').replace('m', '000000'))
            return actual_vol > val
        elif '<' in v:
            val = float(v.replace('<', '').replace('k', '000').replace('m', '000000'))
            return actual_vol < val
    except:
        return False
    return False

def parse_fraud_check(rule_fraud, actual_fraud_rate):
    """Check if actual fraud rate falls within the rule's fraud range string (e.g., '>8.3%')."""
    if rule_fraud is None: return True
    f = str(rule_fraud).replace('%', '')
    try:
        if '-' in f:
            low, high = f.split('-')
            return float(low)/100 <= actual_fraud_rate <= float(high)/100
        elif '>' in f:
            return actual_fraud_rate > float(f.replace('>', ''))/100
        elif '<' in f:
            return actual_fraud_rate < float(f.replace('<', ''))/100
    except:
        return False
    return False

def parse_capture_delay_check(rule_delay, merchant_delay):
    """Check if merchant capture delay matches rule (handles 'manual', 'immediate', numeric)."""
    if rule_delay is None: return True
    r = str(rule_delay).lower()
    m = str(merchant_delay).lower()
    
    # Direct categorical match (e.g., "manual" == "manual")
    if r == m: return True
    
    # Numeric handling
    m_val = None
    if m == 'immediate': m_val = 0
    elif m.isdigit(): m_val = float(m)
    
    if m_val is not None:
        try:
            if '-' in r:
                low, high = r.split('-')
                return float(low) <= m_val <= float(high)
            elif '>' in r:
                return m_val > float(r.replace('>', ''))
            elif '<' in r:
                return m_val < float(r.replace('<', ''))
        except:
            pass
    return False

def execute_step():
    # File paths
    payments_path = '/output/chunk3/data/context/payments.csv'
    fees_path = '/output/chunk3/data/context/fees.json'
    merchant_path = '/output/chunk3/data/context/merchant_data.json'
    
    # Load data
    # print("Loading data...")
    df = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees = json.load(f)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)
        
    # 1. Filter for Rafa_AI and 2023
    df = df[(df['merchant'] == 'Rafa_AI') & (df['year'] == 2023)].copy()
    # print(f"Filtered transactions for Rafa_AI in 2023: {len(df)}")
    
    if len(df) == 0:
        print("No transactions found for Rafa_AI in 2023.")
        return

    # 2. Get Merchant Attributes
    merchant_info = next((m for m in merchant_data if m['merchant'] == 'Rafa_AI'), None)
    if not merchant_info:
        # Fallback or error if merchant not found (though prompt implies it exists)
        # print("Merchant Rafa_AI not found in merchant_data.json")
        return

    m_account_type = merchant_info.get('account_type')
    m_mcc = merchant_info.get('merchant_category_code')
    m_capture_delay = merchant_info.get('capture_delay')
    
    # 3. Calculate Monthly Stats (Volume and Fraud Rate)
    # Convert day_of_year to month (2023 is non-leap)
    df['month'] = pd.to_datetime(df['year'] * 1000 + df['day_of_year'], format='%Y%j').dt.month
    
    monthly_stats = {}
    for month in range(1, 13):
        month_df = df[df['month'] == month]
        vol = month_df['eur_amount'].sum()
        fraud_vol = month_df[month_df['has_fraudulent_dispute']]['eur_amount'].sum()
        fraud_rate = (fraud_vol / vol) if vol > 0 else 0.0
        monthly_stats[month] = {'vol': vol, 'fraud_rate': fraud_rate}
        
    # 4. Prepare for Simulation
    schemes = ['GlobalCard', 'NexPay', 'SwiftCharge', 'TransactPlus']
    scheme_costs = {s: 0.0 for s in schemes}
    
    # Determine intracountry status (Issuing == Acquirer)
    df['intracountry'] = df['issuing_country'] == df['acquirer_country']
    
    # Group transactions by attributes that affect fees to optimize loop
    # We need sum of amount (for variable fee) and count (for fixed fee)
    grouped = df.groupby(['month', 'is_credit', 'aci', 'intracountry'])['eur_amount'].agg(['sum', 'count']).reset_index()
    
    # print(f"Processing {len(grouped)} unique transaction profiles across 4 schemes...")
    
    # 5. Simulate Fees
    for scheme in schemes:
        total_fee = 0.0
        
        for _, row in grouped.iterrows():
            month = row['month']
            is_credit = row['is_credit']
            aci = row['aci']
            intracountry = row['intracountry']
            total_amount = row['sum']
            tx_count = row['count']
            
            # Get monthly stats for rule matching
            stats = monthly_stats[month]
            vol = stats['vol']
            fraud_rate = stats['fraud_rate']
            
            # Find the first matching rule in fees.json
            matched_rule = None
            for rule in fees:
                # Check Scheme
                if rule['card_scheme'] != scheme: continue
                
                # Check Account Type (Wildcard [] or Match)
                if rule['account_type'] and m_account_type not in rule['account_type']: continue
                
                # Check MCC (Wildcard [] or Match)
                if rule['merchant_category_code'] and m_mcc not in rule['merchant_category_code']: continue
                
                # Check Capture Delay
                if not parse_capture_delay_check(rule['capture_delay'], m_capture_delay): continue
                
                # Check Monthly Volume
                if not parse_volume_check(rule['monthly_volume'], vol): continue
                
                # Check Monthly Fraud Level
                if not parse_fraud_check(rule['monthly_fraud_level'], fraud_rate): continue
                
                # Check is_credit (Wildcard null or Match)
                if rule['is_credit'] is not None and rule['is_credit'] != is_credit: continue
                
                # Check ACI (Wildcard [] or Match)
                if rule['aci'] and aci not in rule['aci']: continue
                
                # Check Intracountry (Wildcard null or Match)
                if rule['intracountry'] is not None:
                    # fees.json uses 0.0/1.0 for bools
                    rule_intra = bool(rule['intracountry'])
                    if rule_intra != intracountry: continue
                
                matched_rule = rule
                break # Stop at first match
            
            if matched_rule:
                # Calculate Fee: Fixed * Count + Rate * Amount / 10000
                fixed = matched_rule['fixed_amount']
                rate = matched_rule['rate']
                fee = (fixed * tx_count) + (rate * total_amount / 10000.0)
                total_fee += fee
            else:
                # Fallback if no rule matches (should not happen with complete rule sets)
                pass
                
        scheme_costs[scheme] = total_fee
        # print(f"Scheme {scheme}: €{total_fee:,.2f}")
        
    # 6. Result
    best_scheme = min(scheme_costs, key=scheme_costs.get)
    
    # print("-" * 30)
    # print("FEE SIMULATION RESULTS (2023)")
    # print("-" * 30)
    # for s, cost in scheme_costs.items():
    #     print(f"{s}: €{cost:,.2f}")
    # print("-" * 30)
    # print(f"Recommended Scheme: {best_scheme}")
    
    # Final Answer (Just the name as requested by typical question format)
    print(best_scheme)

if __name__ == "__main__":
    execute_step()
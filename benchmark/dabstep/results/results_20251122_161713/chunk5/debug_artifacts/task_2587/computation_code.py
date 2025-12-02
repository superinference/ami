import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas, k/m suffixes to float."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    
    s = str(value).strip().replace(',', '').replace('€', '').replace('$', '')
    s = s.lstrip('><≤≥')  # Remove comparison operators
    
    try:
        if '%' in s:
            return float(s.replace('%', '')) / 100
        if 'k' in s.lower():
            return float(s.lower().replace('k', '')) * 1000
        if 'm' in s.lower():
            return float(s.lower().replace('m', '')) * 1000000
        if '-' in s:
            parts = s.split('-')
            return (coerce_to_float(parts[0]) + coerce_to_float(parts[1])) / 2
        return float(s)
    except:
        return 0.0

def parse_range_check(value, rule_range_str):
    """Checks if a numeric value fits within a rule string like '100k-1m', '>5', '7.7%-8.3%'."""
    if rule_range_str is None:
        return True
    
    s = str(rule_range_str).strip().lower()
    if s == 'none' or s == '':
        return True
    
    def parse_val(x):
        return coerce_to_float(x)

    try:
        if '-' in s:
            parts = s.split('-')
            low = parse_val(parts[0])
            high = parse_val(parts[1])
            return low <= value <= high
        
        if s.startswith('>'):
            limit = parse_val(s[1:])
            return value > limit
            
        if s.startswith('<'):
            limit = parse_val(s[1:])
            return value < limit
            
        # Exact match
        return value == parse_val(s)
    except:
        return False

def check_capture_delay(merchant_delay, rule_delay):
    """Matches merchant capture delay (e.g., '1', 'manual') against rule (e.g., '<3', 'manual')."""
    if rule_delay is None:
        return True
    
    m_delay = str(merchant_delay).lower()
    r_delay = str(rule_delay).lower()
    
    # Direct string match
    if m_delay == r_delay:
        return True
    
    # Numeric comparison
    try:
        m_val = float(m_delay)
    except ValueError:
        if m_delay == 'immediate':
            m_val = 0.0
        else:
            return False # 'manual' vs numeric rule -> False
            
    if r_delay.startswith('<'):
        try:
            limit = float(r_delay[1:])
            return m_val < limit
        except: pass
    elif r_delay.startswith('>'):
        try:
            limit = float(r_delay[1:])
            return m_val > limit
        except: pass
    elif '-' in r_delay:
        try:
            parts = r_delay.split('-')
            low = float(parts[0])
            high = float(parts[1])
            return low <= m_val <= high
        except: pass
        
    return False

def execute_step():
    # 1. Load Data
    try:
        df = pd.read_csv('/output/chunk5/data/context/payments.csv')
        with open('/output/chunk5/data/context/fees.json', 'r') as f:
            fees = json.load(f)
        with open('/output/chunk5/data/context/merchant_data.json', 'r') as f:
            merchants = json.load(f)
        print("Successfully loaded all data files.")
    except Exception as e:
        print(f"Error loading files: {e}")
        return

    # 2. Filter for Belles_cookbook_store in February (Day 32-59)
    target_merchant = 'Belles_cookbook_store'
    start_day = 32
    end_day = 59
    
    df_feb = df[(df['merchant'] == target_merchant) & 
                (df['day_of_year'] >= start_day) & 
                (df['day_of_year'] <= end_day)].copy()
    
    if len(df_feb) == 0:
        print("No transactions found for Belles_cookbook_store in February.")
        return

    # 3. Calculate Monthly Stats (Volume & Fraud) for Fee Rules
    # Note: Using the filtered February data as the "monthly" stats
    total_volume = df_feb['eur_amount'].sum()
    fraud_volume = df_feb[df_feb['has_fraudulent_dispute']]['eur_amount'].sum()
    fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0
    
    print(f"\nMerchant: {target_merchant}")
    print(f"Transactions: {len(df_feb)}")
    print(f"Feb Volume: €{total_volume:,.2f}")
    print(f"Feb Fraud Rate: {fraud_rate:.4%}")

    # 4. Get Merchant Attributes
    merchant_info = next((m for m in merchants if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print("Merchant info not found in merchant_data.json.")
        return
        
    mcc = merchant_info['merchant_category_code']
    account_type = merchant_info['account_type']
    capture_delay = merchant_info['capture_delay']
    
    print(f"Attributes: MCC={mcc}, Account={account_type}, Delay={capture_delay}")

    # 5. Simulate Schemes to find MAX fees
    schemes = ['NexPay', 'GlobalCard', 'SwiftCharge', 'TransactPlus']
    results = {}

    print("\nCalculating projected fees for each scheme...")
    
    for scheme in schemes:
        total_fee = 0.0
        
        # Filter fees for this scheme to optimize loop
        scheme_fees = [f for f in fees if f['card_scheme'] == scheme]
        
        # Iterate through ALL transactions in the filtered set
        for _, tx in df_feb.iterrows():
            # Context for this transaction
            # We simulate the scheme, but keep other tx details
            tx_ctx = {
                'eur_amount': tx['eur_amount'],
                'is_credit': tx['is_credit'],
                'aci': tx['aci'],
                'intracountry': tx['issuing_country'] == tx['acquirer_country']
            }
            
            # Find the first matching rule for this transaction under this scheme
            matched_rule = None
            for rule in scheme_fees:
                # Check all conditions
                
                # Account Type (Rule list empty = wildcard)
                if rule['account_type'] and account_type not in rule['account_type']: continue
                
                # MCC (Rule list empty = wildcard)
                if rule['merchant_category_code'] and mcc not in rule['merchant_category_code']: continue
                
                # Capture Delay
                if not check_capture_delay(capture_delay, rule['capture_delay']): continue
                
                # Monthly Volume
                if not parse_range_check(total_volume, rule['monthly_volume']): continue
                
                # Monthly Fraud Level
                if not parse_range_check(fraud_rate, rule['monthly_fraud_level']): continue
                
                # Is Credit (Rule null = wildcard)
                if rule['is_credit'] is not None and rule['is_credit'] != tx_ctx['is_credit']: continue
                
                # ACI (Rule list empty = wildcard)
                if rule['aci'] and tx_ctx['aci'] not in rule['aci']: continue
                
                # Intracountry (Rule null = wildcard)
                if rule['intracountry'] is not None:
                    # Handle string/float/bool variations in JSON
                    rule_intra = bool(float(rule['intracountry']))
                    if rule_intra != tx_ctx['intracountry']: continue
                
                matched_rule = rule
                break # Stop at first match
            
            if matched_rule:
                # Fee = Fixed + (Rate * Amount / 10000)
                fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * tx_ctx['eur_amount'] / 10000)
                total_fee += fee
            else:
                # Should not happen in well-formed data, but good for debugging
                pass
                
        results[scheme] = total_fee
        print(f"Scheme: {scheme:<15} Total Fee: €{total_fee:,.2f}")

    # 6. Identify Scheme with Maximum Fees
    max_scheme = max(results, key=results.get)
    print(f"\nTo pay the MAXIMUM fees, steer traffic to: {max_scheme}")
    print(max_scheme)

if __name__ == "__main__":
    execute_step()
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

def parse_range(value_str):
    """Parses strings like '100k-1m', '>5', '7.7%-8.3%' into (min, max)."""
    if value_str is None:
        return (-float('inf'), float('inf'))
    
    s = str(value_str).lower().strip()
    is_percent = '%' in s
    
    # Handle k/m suffixes
    def parse_num(n_str):
        n_str = n_str.replace('%', '')
        mult = 1
        if 'k' in n_str:
            mult = 1000
            n_str = n_str.replace('k', '')
        elif 'm' in n_str:
            mult = 1000000
            n_str = n_str.replace('m', '')
        try:
            val = float(n_str) * mult
            return val / 100 if is_percent else val
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return (parse_num(parts[0]), parse_num(parts[1]))
    elif '>' in s:
        return (parse_num(s.replace('>', '')), float('inf'))
    elif '<' in s:
        return (-float('inf'), parse_num(s.replace('<', '')))
    else:
        # Exact match treated as range
        val = parse_num(s)
        return (val, val)

def check_range(value, range_str):
    if range_str is None:
        return True
    min_v, max_v = parse_range(range_str)
    return min_v <= value <= max_v

def match_fee_rule(tx_ctx, rule):
    # 1. Card Scheme
    if rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List in rule)
    if rule['account_type'] and tx_ctx['account_type'] not in rule['account_type']:
        return False
        
    # 3. Capture Delay
    if rule['capture_delay'] is not None:
        m_delay = str(tx_ctx['capture_delay'])
        r_delay = rule['capture_delay']
        match = False
        if m_delay == r_delay:
            match = True
        elif r_delay == '>5':
            if m_delay.isdigit() and int(m_delay) > 5: match = True
        elif r_delay == '3-5':
            if m_delay.isdigit() and 3 <= int(m_delay) <= 5: match = True
        elif r_delay == '<3':
            if m_delay.isdigit() and int(m_delay) < 3: match = True
        
        if not match:
            return False

    # 4. Merchant Category Code (List in rule)
    if rule['merchant_category_code'] and tx_ctx['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 5. Is Credit (Bool)
    if rule['is_credit'] is not None and rule['is_credit'] != tx_ctx['is_credit']:
        return False
        
    # 6. ACI (List in rule)
    if rule['aci'] and tx_ctx['aci'] not in rule['aci']:
        return False
        
    # 7. Intracountry (Bool)
    if rule['intracountry'] is not None:
        is_intra = (tx_ctx['issuing_country'] == tx_ctx['acquirer_country'])
        if rule['intracountry'] != is_intra:
            return False
            
    # 8. Monthly Volume (Range)
    if not check_range(tx_ctx['monthly_volume'], rule['monthly_volume']):
        return False
        
    # 9. Monthly Fraud Level (Range)
    if not check_range(tx_ctx['monthly_fraud_rate'], rule['monthly_fraud_level']):
        return False
        
    return True

def calculate_fee(amount, rule):
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000)

def main():
    # Paths
    payments_path = '/output/chunk4/data/context/payments.csv'
    merchant_path = '/output/chunk4/data/context/merchant_data.json'
    fees_path = '/output/chunk4/data/context/fees.json'
    
    # Load Data
    df_payments = pd.read_csv(payments_path)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
        
    target_merchant = 'Martinis_Fine_Steakhouse'
    
    # 1. Get Merchant Attributes
    m_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not m_info:
        print("Merchant not found")
        return

    # 2. Calculate Monthly Stats for September (Day 244-273)
    sept_mask = (df_payments['merchant'] == target_merchant) & \
                (df_payments['day_of_year'] >= 244) & \
                (df_payments['day_of_year'] <= 273)
    
    df_sept = df_payments[sept_mask]
    
    total_volume = df_sept['eur_amount'].sum()
    fraud_volume = df_sept[df_sept['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0
    
    # 3. Identify Target Transactions (Fraudulent ones in Sept)
    target_txs = df_sept[df_sept['has_fraudulent_dispute'] == True].copy()
    
    # 4. Simulate Fees for each ACI
    possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    aci_costs = {}
    
    for aci in possible_acis:
        total_fee = 0
        valid_aci = True
        
        for _, tx in target_txs.iterrows():
            ctx = {
                'card_scheme': tx['card_scheme'],
                'account_type': m_info['account_type'],
                'capture_delay': m_info['capture_delay'],
                'mcc': m_info['merchant_category_code'],
                'is_credit': tx['is_credit'],
                'aci': aci,
                'issuing_country': tx['issuing_country'],
                'acquirer_country': tx['acquirer_country'],
                'monthly_volume': total_volume,
                'monthly_fraud_rate': fraud_rate
            }
            
            matched_rule = None
            for rule in fees_data:
                if match_fee_rule(ctx, rule):
                    matched_rule = rule
                    break
            
            if matched_rule:
                total_fee += calculate_fee(tx['eur_amount'], matched_rule)
            else:
                valid_aci = False
                break
        
        if valid_aci:
            aci_costs[aci] = total_fee
        else:
            aci_costs[aci] = float('inf')

    # 5. Find Preferred Choice
    valid_costs = {k: v for k, v in aci_costs.items() if v != float('inf')}
    
    if valid_costs:
        best_aci = min(valid_costs, key=valid_costs.get)
        print(best_aci)
    else:
        print("None")

if __name__ == "__main__":
    main()
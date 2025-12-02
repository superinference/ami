# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2762
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7529 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json

# Helper functions for robust data processing
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

def check_volume(merchant_vol, rule_vol_str):
    """Check if merchant volume falls within rule range."""
    if not rule_vol_str: return True
    try:
        s = rule_vol_str.lower().replace(',', '')
        
        def parse_val(v):
            m = 1
            if 'k' in v: m = 1000; v = v.replace('k', '')
            elif 'm' in v: m = 1000000; v = v.replace('m', '')
            return float(v) * m

        if '-' in s:
            low, high = s.split('-')
            return parse_val(low) <= merchant_vol <= parse_val(high)
        elif '>' in s:
            return merchant_vol > parse_val(s.replace('>', ''))
        elif '<' in s:
            return merchant_vol < parse_val(s.replace('<', ''))
        return False
    except:
        return False

def check_fraud(merchant_fraud_rate, rule_fraud_str):
    """Check if merchant fraud rate falls within rule range."""
    if not rule_fraud_str: return True
    try:
        s = rule_fraud_str.replace('%', '')
        if '-' in s:
            low, high = s.split('-')
            return float(low)/100 <= merchant_fraud_rate <= float(high)/100
        elif '>' in s:
            return merchant_fraud_rate > float(s.replace('>', ''))/100
        elif '<' in s:
            return merchant_fraud_rate < float(s.replace('<', ''))/100
        return False
    except:
        return False

def check_capture_delay(merchant_delay, rule_delay):
    """Check if merchant capture delay matches rule."""
    if not rule_delay: return True
    if rule_delay == 'manual': return merchant_delay == 'manual'
    if rule_delay == 'immediate': return merchant_delay == 'immediate'
    
    try:
        # Merchant delay is usually a string digit like "2"
        if merchant_delay in ['manual', 'immediate']: return False
        m_delay = float(merchant_delay)
        
        if '-' in rule_delay:
            low, high = rule_delay.split('-')
            return float(low) <= m_delay <= float(high)
        elif '>' in rule_delay:
            return m_delay > float(rule_delay.replace('>', ''))
        elif '<' in rule_delay:
            return m_delay < float(rule_delay.replace('<', ''))
        return False
    except:
        return False

def execute_step():
    # File paths
    merchant_path = '/output/chunk4/data/context/merchant_data.json'
    payments_path = '/output/chunk4/data/context/payments.csv'
    fees_path = '/output/chunk4/data/context/fees.json'

    # 1. Load Data
    try:
        with open(merchant_path, 'r') as f: merchant_data = json.load(f)
        with open(fees_path, 'r') as f: fees_data = json.load(f)
        df = pd.read_csv(payments_path)
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    target_merchant = 'Golfclub_Baron_Friso'
    
    # 2. Get Merchant Profile
    m_profile = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not m_profile:
        print(f"Merchant {target_merchant} not found")
        return

    # 3. Calculate Merchant Metrics (2023)
    # Filter for merchant and year 2023
    df_m = df[(df['merchant'] == target_merchant) & (df['year'] == 2023)].copy()
    
    if df_m.empty:
        print("No transactions found for 2023")
        return

    total_vol = df_m['eur_amount'].sum()
    monthly_vol = total_vol / 12.0
    
    fraud_vol = df_m[df_m['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    # 4. Prepare Transaction Groups for Simulation
    # Determine intracountry status per transaction
    df_m['is_intracountry'] = df_m['issuing_country'] == df_m['acquirer_country']
    
    # Group by fee-determining columns: is_credit, aci, is_intracountry
    # We sum amounts and count transactions to apply fees in bulk
    grouped = df_m.groupby(['is_credit', 'aci', 'is_intracountry']).agg(
        count=('psp_reference', 'count'),
        sum_amount=('eur_amount', 'sum')
    ).reset_index()

    schemes = ['NexPay', 'GlobalCard', 'SwiftCharge', 'TransactPlus']
    scheme_costs = {}

    # 5. Simulate Costs for Each Scheme
    for scheme in schemes:
        total_cost = 0
        possible = True
        
        for _, row in grouped.iterrows():
            matched_rule = None
            
            # Find the first matching rule in fees.json
            for rule in fees_data:
                # Scheme match
                if rule['card_scheme'] != scheme: continue
                
                # Merchant Profile Matches
                if rule['merchant_category_code'] is not None:
                    if m_profile['merchant_category_code'] not in rule['merchant_category_code']: continue
                
                if rule['account_type'] is not None:
                    if m_profile['account_type'] not in rule['account_type']: continue
                
                if not check_capture_delay(m_profile['capture_delay'], rule['capture_delay']): continue
                if not check_volume(monthly_vol, rule['monthly_volume']): continue
                if not check_fraud(fraud_rate, rule['monthly_fraud_level']): continue
                
                # Transaction Attribute Matches
                if rule['is_credit'] is not None:
                    if rule['is_credit'] != row['is_credit']: continue
                
                if rule['aci'] is not None:
                    if row['aci'] not in rule['aci']: continue
                
                if rule['intracountry'] is not None:
                    # Handle 0.0/1.0/False/True from JSON
                    rule_intra = bool(rule['intracountry'])
                    if rule_intra != row['is_intracountry']: continue
                
                matched_rule = rule
                break 
            
            if matched_rule:
                # Calculate Fee: Fixed + Variable (Rate is per 10,000 units of currency)
                # Fee = (Fixed * Count) + (Rate * TotalAmount / 10000)
                fee = (matched_rule['fixed_amount'] * row['count']) + \
                      (matched_rule['rate'] * row['sum_amount'] / 10000.0)
                total_cost += fee
            else:
                # If a scheme has no rule for a transaction type, it's invalid/expensive
                possible = False
                break
        
        if possible:
            scheme_costs[scheme] = total_cost
        else:
            scheme_costs[scheme] = float('inf')

    # 6. Output Result
    if not scheme_costs:
        print("No valid schemes found")
        return

    # Find scheme with minimum cost
    best_scheme = min(scheme_costs, key=scheme_costs.get)
    print(best_scheme)

if __name__ == "__main__":
    execute_step()

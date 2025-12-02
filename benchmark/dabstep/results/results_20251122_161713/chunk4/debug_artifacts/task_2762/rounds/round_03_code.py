# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2762
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: sufficient
# Plan steps: 2
# Code length: 8558 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# Helper Functions
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if pd.isna(value) or value is None:
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

def check_volume_match(merchant_vol, rule_vol_str):
    """Check if merchant volume falls within rule range (e.g., '100k-1m')."""
    if not rule_vol_str: return True
    try:
        s = rule_vol_str.lower().replace(',', '').replace('€', '')
        
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

def check_fraud_match(merchant_fraud_rate, rule_fraud_str):
    """Check if merchant fraud rate falls within rule range (e.g., '0-0.5%')."""
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

def check_capture_delay_match(merchant_delay, rule_delay):
    """Check if merchant capture delay matches rule."""
    if not rule_delay: return True
    
    # Direct string match for 'manual', 'immediate'
    if str(rule_delay) == str(merchant_delay): return True
    
    # Numeric comparison if possible
    try:
        m_delay = float(merchant_delay)
        if '-' in str(rule_delay):
            low, high = rule_delay.split('-')
            return float(low) <= m_delay <= float(high)
        elif '>' in str(rule_delay):
            return m_delay > float(rule_delay.replace('>', ''))
        elif '<' in str(rule_delay):
            return m_delay < float(rule_delay.replace('<', ''))
        return False
    except:
        return False

# ═══════════════════════════════════════════════════════════
# Main Analysis
# ═══════════════════════════════════════════════════════════

def execute_analysis():
    # 1. Load Data
    try:
        with open('/output/chunk4/data/context/merchant_data.json', 'r') as f:
            merchant_data = json.load(f)
        with open('/output/chunk4/data/context/fees.json', 'r') as f:
            fees_data = json.load(f)
        df = pd.read_csv('/output/chunk4/data/context/payments.csv')
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    target_merchant = 'Golfclub_Baron_Friso'
    
    # 2. Get Merchant Profile
    m_profile = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not m_profile:
        print(f"Merchant {target_merchant} not found")
        return

    # 3. Calculate Merchant Metrics for 2023
    # Filter for merchant and year 2023
    df_m = df[(df['merchant'] == target_merchant) & (df['year'] == 2023)].copy()
    
    if df_m.empty:
        print("No transactions found for 2023")
        return

    # Calculate Volume and Fraud Metrics
    total_vol = df_m['eur_amount'].sum()
    monthly_vol = total_vol / 12.0
    
    fraud_vol = df_m[df_m['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    # 4. Prepare Transaction Groups for Simulation
    # Determine intracountry status: Issuer == Acquirer
    df_m['is_intracountry'] = df_m['issuing_country'] == df_m['acquirer_country']
    
    # Group by fee-determining columns to optimize calculation
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
            
            # Iterate through fees to find the FIRST matching rule for this scheme and transaction type
            # Note: In real engines, there's a priority. Here we assume the JSON order or specificity.
            # We look for a rule that satisfies ALL conditions.
            
            for rule in fees_data:
                # 1. Scheme Match
                if rule['card_scheme'] != scheme: continue
                
                # 2. Merchant Profile Matches (Global for merchant)
                # MCC
                if rule['merchant_category_code'] is not None:
                    if m_profile['merchant_category_code'] not in rule['merchant_category_code']: continue
                
                # Account Type
                if rule['account_type'] is not None:
                    if m_profile['account_type'] not in rule['account_type']: continue
                
                # Capture Delay
                if not check_capture_delay_match(m_profile['capture_delay'], rule['capture_delay']): continue
                
                # Monthly Volume
                if not check_volume_match(monthly_vol, rule['monthly_volume']): continue
                
                # Monthly Fraud Level
                if not check_fraud_match(fraud_rate, rule['monthly_fraud_level']): continue
                
                # 3. Transaction Attribute Matches (Specific to transaction group)
                # Is Credit
                if rule['is_credit'] is not None:
                    if rule['is_credit'] != row['is_credit']: continue
                
                # ACI
                if rule['aci'] is not None:
                    if row['aci'] not in rule['aci']: continue
                
                # Intracountry
                if rule['intracountry'] is not None:
                    # JSON might have boolean or 0.0/1.0
                    rule_intra = bool(rule['intracountry'])
                    if rule_intra != row['is_intracountry']: continue
                
                matched_rule = rule
                break 
            
            if matched_rule:
                # Fee = Fixed + (Rate * Amount / 10000)
                fee = (matched_rule['fixed_amount'] * row['count']) + \
                      (matched_rule['rate'] * row['sum_amount'] / 10000.0)
                total_cost += fee
            else:
                # If a scheme cannot process this transaction type (no rule found), 
                # it's not a valid option for "steering traffic" (or infinitely expensive)
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
    # Filter out infinite costs
    valid_costs = {k: v for k, v in scheme_costs.items() if v != float('inf')}
    
    if not valid_costs:
        print("No valid schemes found (all returned infinite cost)")
    else:
        best_scheme = min(valid_costs, key=valid_costs.get)
        print(best_scheme)

if __name__ == "__main__":
    execute_analysis()

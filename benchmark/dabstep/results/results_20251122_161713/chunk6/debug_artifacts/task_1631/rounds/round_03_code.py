# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1631
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 8537 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import numpy as np
import json

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

def parse_vol_range(v_str):
    """Parse volume string like '100k-1m' into (min, max)."""
    if not v_str: return None
    v_str = str(v_str).lower().replace('k', '000').replace('m', '000000').replace('.', '')
    try:
        if '-' in v_str:
            l, h = v_str.split('-')
            return float(l), float(h)
        if v_str.startswith('>'): return float(v_str[1:]), float('inf')
        if v_str.startswith('<'): return 0.0, float(v_str[1:])
    except:
        return None
    return None

def parse_fraud_range(f_str):
    """Parse fraud string like '0.0%-1.0%' into (min, max)."""
    if not f_str: return None
    s = str(f_str).replace('%', '')
    try:
        if '-' in s:
            l, h = s.split('-')
            return float(l)/100, float(h)/100
        if s.startswith('>'): return float(s[1:])/100, float('inf')
        if s.startswith('<'): return 0.0, float(s[1:])/100
    except:
        return None
    return None

def check_capture_delay(merchant_delay, rule_delay):
    """Check if merchant capture delay matches rule."""
    if not rule_delay: return True # Wildcard
    
    m_delay = str(merchant_delay).lower()
    r_delay = str(rule_delay).lower()
    
    # Direct match
    if m_delay == r_delay: return True
    
    # Numeric comparison
    if m_delay.isdigit():
        val = int(m_delay)
        if r_delay == 'immediate' or r_delay == 'manual': return False
        
        if '-' in r_delay:
            try:
                l, h = map(int, r_delay.split('-'))
                return l <= val <= h
            except: pass
        elif r_delay.startswith('>'):
            try:
                return val > int(r_delay[1:])
            except: pass
        elif r_delay.startswith('<'):
            try:
                return val < int(r_delay[1:])
            except: pass
            
    return False

def solve():
    # 1. Load Data
    try:
        fees_df = pd.read_json('/output/chunk6/data/context/fees.json')
        merchant_data = pd.read_json('/output/chunk6/data/context/merchant_data.json')
        payments = pd.read_csv('/output/chunk6/data/context/payments.csv')
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # 2. Identify Account Type F Merchants
    type_f_merchants_df = merchant_data[merchant_data['account_type'] == 'F']
    type_f_merchants_list = type_f_merchants_df['merchant'].unique()
    
    if len(type_f_merchants_list) == 0:
        print("No Account Type F merchants found.")
        return

    # 3. Calculate Merchant Stats (Monthly Volume, Fraud Rate)
    # Filter payments for relevant merchants first to speed up aggregation
    relevant_payments = payments[payments['merchant'].isin(type_f_merchants_list)]
    
    merchant_stats = {}
    
    # Calculate totals per merchant
    merchant_totals = relevant_payments.groupby('merchant').agg(
        total_volume=('eur_amount', 'sum'),
        fraud_volume=('eur_amount', lambda x: x[relevant_payments.loc[x.index, 'has_fraudulent_dispute']].sum())
    ).to_dict('index')
    
    for m_name in type_f_merchants_list:
        m_static = merchant_data[merchant_data['merchant'] == m_name].iloc[0]
        
        if m_name in merchant_totals:
            vol = merchant_totals[m_name]['total_volume']
            fraud_vol = merchant_totals[m_name]['fraud_volume']
            monthly_vol = vol / 12.0
            fraud_rate = fraud_vol / vol if vol > 0 else 0.0
        else:
            monthly_vol = 0.0
            fraud_rate = 0.0
            
        merchant_stats[m_name] = {
            'account_type': m_static['account_type'],
            'mcc': m_static['merchant_category_code'],
            'capture_delay': str(m_static['capture_delay']),
            'monthly_volume': monthly_vol,
            'fraud_rate': fraud_rate
        }

    # 4. Filter Transactions for Simulation
    # We need actual transactions to get the distribution of ACI, Credit/Debit, Intracountry
    target_txs = relevant_payments[relevant_payments['card_scheme'] == 'SwiftCharge'].copy()
    
    if target_txs.empty:
        print("No SwiftCharge transactions found for Account Type F merchants.")
        return

    # Pre-calculate intracountry
    target_txs['is_intracountry'] = target_txs['issuing_country'] == target_txs['acquirer_country']

    # 5. Prepare Fee Rules
    swift_rules = fees_df[fees_df['card_scheme'] == 'SwiftCharge'].to_dict('records')
    
    # Sort rules by specificity (count of non-null criteria)
    criteria_cols = ['account_type', 'merchant_category_code', 'aci', 'is_credit', 
                     'intracountry', 'capture_delay', 'monthly_volume', 'monthly_fraud_level']
    
    def count_criteria(rule):
        count = 0
        for c in criteria_cols:
            val = rule.get(c)
            if val is not None and val != [] and val != float('nan'):
                count += 1
        return count

    swift_rules.sort(key=count_criteria, reverse=True)

    # 6. Calculate Fees (Simulating 500 EUR)
    calculated_fees = []
    simulated_amount = 500.0
    
    for _, tx in target_txs.iterrows():
        m_name = tx['merchant']
        m_info = merchant_stats.get(m_name)
        
        tx_aci = tx['aci']
        tx_credit = bool(tx['is_credit'])
        tx_intra = bool(tx['is_intracountry'])
        
        matched_rule = None
        
        for rule in swift_rules:
            # 1. Account Type
            if rule['account_type'] and m_info['account_type'] not in rule['account_type']:
                continue
                
            # 2. MCC
            if rule['merchant_category_code'] and m_info['mcc'] not in rule['merchant_category_code']:
                continue
                
            # 3. ACI
            if rule['aci'] and tx_aci not in rule['aci']:
                continue
                
            # 4. Is Credit
            if rule['is_credit'] is not None:
                # Handle numpy bools or standard bools
                if bool(rule['is_credit']) != tx_credit:
                    continue
                    
            # 5. Intracountry
            if rule['intracountry'] is not None:
                # In fees.json, 0.0 is False, 1.0 is True
                if bool(rule['intracountry']) != tx_intra:
                    continue
            
            # 6. Capture Delay
            if not check_capture_delay(m_info['capture_delay'], rule['capture_delay']):
                continue
                
            # 7. Monthly Volume
            if rule['monthly_volume']:
                r_vol = parse_vol_range(rule['monthly_volume'])
                if r_vol:
                    if not (r_vol[0] <= m_info['monthly_volume'] <= r_vol[1]):
                        continue
            
            # 8. Monthly Fraud Level
            if rule['monthly_fraud_level']:
                r_fraud = parse_fraud_range(rule['monthly_fraud_level'])
                if r_fraud:
                    if not (r_fraud[0] <= m_info['fraud_rate'] <= r_fraud[1]):
                        continue
            
            # Match found
            matched_rule = rule
            break
        
        if matched_rule:
            # Fee = Fixed + (Rate * Amount / 10000)
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * simulated_amount / 10000.0)
            calculated_fees.append(fee)

    # 7. Compute Average
    if calculated_fees:
        avg_fee = np.mean(calculated_fees)
        print(f"{avg_fee:.6f}")
    else:
        print("No applicable fee rules found.")

if __name__ == "__main__":
    solve()

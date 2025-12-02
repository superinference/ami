# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1834
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 6965 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- HELPER FUNCTIONS ---
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
        return float(v)
    return float(value)

def parse_range(range_str):
    """Parses a range string like '100k-1m' or '0%-0.5%' into (min, max)."""
    if not range_str or not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    def parse_val(x):
        x = x.strip()
        mult = 1
        if x.endswith('k'):
            mult = 1000
            x = x[:-1]
        elif x.endswith('m'):
            mult = 1000000
            x = x[:-1]
        elif x.endswith('%'):
            mult = 0.01
            x = x[:-1]
        return float(x) * mult

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif s.startswith('>'):
        val = parse_val(s[1:])
        return val, float('inf')
    elif s.startswith('<'):
        val = parse_val(s[1:])
        return float('-inf'), val
    else:
        val = parse_val(s)
        return val, val

def check_range(value, range_str):
    """Checks if a value falls within a range string."""
    if range_str is None:
        return True
    min_val, max_val = parse_range(range_str)
    if min_val is None: return True
    # Inclusive check
    return min_val <= value <= max_val

def match_fee_rule(tx_context, rule):
    """Matches a transaction context against a fee rule."""
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List in rule)
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List in rule)
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay
    if rule.get('capture_delay'):
        # Handle exact match for strings like 'manual'
        if rule['capture_delay'] == tx_context['capture_delay']:
            pass
        # Handle range logic if rule is like '>5' and merchant is numeric string
        elif any(c in rule['capture_delay'] for c in ['<', '>', '-']):
             try:
                 m_val = float(tx_context['capture_delay'])
                 if not check_range(m_val, rule['capture_delay']):
                     return False
             except ValueError:
                 # Merchant delay is non-numeric (e.g. 'manual'), rule is numeric range
                 return False
        else:
            # Exact match failed and not a range
            return False

    # 5. Monthly Fraud Level
    if rule.get('monthly_fraud_level'):
        if not check_range(tx_context['monthly_fraud_ratio'], rule['monthly_fraud_level']):
            return False

    # 6. Monthly Volume
    if rule.get('monthly_volume'):
        if not check_range(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 7. Is Credit (Boolean)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 8. ACI (List in rule)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 9. Intracountry (Boolean)
    if rule.get('intracountry') is not None:
        is_intra = (tx_context['issuing_country'] == tx_context['acquirer_country'])
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    return fixed + (rate * amount / 10000.0)

def main():
    # Load Data
    try:
        payments = pd.read_csv('/output/chunk5/data/context/payments.csv')
        with open('/output/chunk5/data/context/fees.json', 'r') as f:
            fees = json.load(f)
        with open('/output/chunk5/data/context/merchant_data.json', 'r') as f:
            merchant_data = json.load(f)
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    target_merchant = 'Crossfit_Hanna'
    
    # Get Merchant Attributes
    merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print(f"Merchant {target_merchant} not found.")
        return
    
    # Filter Transactions for December 2023 (Day >= 335)
    dec_txs = payments[
        (payments['merchant'] == target_merchant) & 
        (payments['year'] == 2023) & 
        (payments['day_of_year'] >= 335)
    ].copy()
    
    if dec_txs.empty:
        print("0.00")
        return
    
    # Calculate Monthly Stats for December
    # Manual: "Monthly volumes and rates are computed always in natural months"
    monthly_volume = dec_txs['eur_amount'].sum()
    fraud_txs = dec_txs[dec_txs['has_fraudulent_dispute'] == True]
    monthly_fraud_vol = fraud_txs['eur_amount'].sum()
    monthly_fraud_ratio = monthly_fraud_vol / monthly_volume if monthly_volume > 0 else 0.0

    # Calculate Fees
    total_fees = 0.0
    m_account_type = merchant_info.get('account_type')
    m_mcc = merchant_info.get('merchant_category_code')
    m_capture_delay = merchant_info.get('capture_delay')

    for _, tx in dec_txs.iterrows():
        context = {
            'card_scheme': tx['card_scheme'],
            'account_type': m_account_type,
            'mcc': m_mcc,
            'capture_delay': m_capture_delay,
            'monthly_volume': monthly_volume,
            'monthly_fraud_ratio': monthly_fraud_ratio,
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'issuing_country': tx['issuing_country'],
            'acquirer_country': tx['acquirer_country']
        }
        
        # Find first matching rule
        matched = False
        for rule in fees:
            if match_fee_rule(context, rule):
                total_fees += calculate_fee(tx['eur_amount'], rule)
                matched = True
                break
        
        if not matched:
            # In a real scenario, we might log this. For now, assume 0 or continue.
            pass

    print(f"{total_fees:.2f}")

if __name__ == "__main__":
    main()

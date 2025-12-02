# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1834
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 8906 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- HELPER FUNCTIONS ---
def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except ValueError:
                return 0.0
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
    return 0.0

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
        try:
            return float(x) * mult
        except ValueError:
            return 0.0

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
        # Treat exact value as a point range? Or just return val, val
        # Usually ranges in these files are explicit. If it's just "100k", it might be a threshold.
        # But based on manual, they are ranges.
        val = parse_val(s)
        return val, val

def check_range(value, range_str):
    """Checks if a value falls within a range string."""
    if range_str is None:
        return True
    
    # Handle specific non-numeric range strings if any (though volume/fraud are usually numeric)
    # If range_str is not a string (e.g. it's a number in JSON), compare directly
    if isinstance(range_str, (int, float)):
        return value == range_str

    min_val, max_val = parse_range(range_str)
    if min_val is None: 
        return True
    
    # Inclusive check
    # Note: For fraud rates, precision matters. 
    return min_val <= value <= max_val

def match_fee_rule(tx_context, rule):
    """Matches a transaction context against a fee rule."""
    
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List in rule)
    # Rule has list of types, merchant has one type. Merchant type must be in rule list.
    # Wildcard: If rule['account_type'] is empty or None, it matches all.
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List in rule)
    # Rule has list of MCCs, merchant has one.
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (String or Range)
    # Merchant has specific value (e.g. 'manual'). Rule has value or range.
    rule_delay = rule.get('capture_delay')
    if rule_delay is not None:
        merchant_delay = str(tx_context['capture_delay'])
        # Exact string match
        if rule_delay == merchant_delay:
            pass
        # Range match (only if merchant delay is numeric)
        elif any(c in rule_delay for c in ['<', '>', '-']):
             try:
                 m_val = float(merchant_delay)
                 if not check_range(m_val, rule_delay):
                     return False
             except ValueError:
                 # Merchant delay is non-numeric (e.g. 'manual'), rule is numeric range
                 # They don't match.
                 return False
        else:
            # Rule is a specific string (e.g. 'immediate') and didn't match merchant's 'manual'
            return False

    # 5. Monthly Fraud Level (Range)
    if rule.get('monthly_fraud_level'):
        if not check_range(tx_context['monthly_fraud_ratio'], rule['monthly_fraud_level']):
            return False

    # 6. Monthly Volume (Range)
    if rule.get('monthly_volume'):
        if not check_range(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 7. Is Credit (Boolean)
    # JSON null matches both. True/False matches specific.
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 8. ACI (List in rule)
    # Rule has list of ACIs, tx has one.
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 9. Intracountry (Boolean)
    if rule.get('intracountry') is not None:
        # Intracountry means issuer == acquirer
        is_intra = (tx_context['issuing_country'] == tx_context['acquirer_country'])
        # Rule expects boolean
        # Handle string '0.0'/'1.0' if present in JSON (sometimes JSON bools are messy)
        rule_intra = rule['intracountry']
        if isinstance(rule_intra, float):
            rule_intra = bool(rule_intra)
        
        if rule_intra != is_intra:
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Rate is integer to be divided by 10000
    return fixed + (rate * amount / 10000.0)

def main():
    # 1. Load Data
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
    
    # 2. Get Merchant Attributes
    merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print(f"Merchant {target_merchant} not found.")
        return
    
    m_account_type = merchant_info.get('account_type')
    m_mcc = merchant_info.get('merchant_category_code')
    m_capture_delay = merchant_info.get('capture_delay')

    # 3. Filter Transactions for December 2023
    # December starts on day 335 (non-leap year 2023)
    dec_txs = payments[
        (payments['merchant'] == target_merchant) & 
        (payments['year'] == 2023) & 
        (payments['day_of_year'] >= 335)
    ].copy()
    
    if dec_txs.empty:
        print("0.00")
        return
    
    # 4. Calculate Monthly Stats for December
    # "Monthly volumes and rates are computed always in natural months"
    monthly_volume = dec_txs['eur_amount'].sum()
    
    # Fraud calculation: sum of amounts where has_fraudulent_dispute is True
    fraud_txs = dec_txs[dec_txs['has_fraudulent_dispute'] == True]
    monthly_fraud_vol = fraud_txs['eur_amount'].sum()
    
    monthly_fraud_ratio = monthly_fraud_vol / monthly_volume if monthly_volume > 0 else 0.0

    # 5. Calculate Fees
    total_fees = 0.0
    
    # Pre-process fees: ensure numeric types where needed
    # (JSON loading usually handles int/float/bool correctly, but good to be safe)
    
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
                fee = calculate_fee(tx['eur_amount'], rule)
                total_fees += fee
                matched = True
                break # Stop after first match
        
        if not matched:
            # If no rule matches, assume 0 fee (or log warning)
            pass

    # 6. Output Result
    print(f"{total_fees:.2f}")

if __name__ == "__main__":
    main()

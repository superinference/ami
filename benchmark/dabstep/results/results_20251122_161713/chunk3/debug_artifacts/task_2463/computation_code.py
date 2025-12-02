import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float."""
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
            except:
                return 0.0
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
    return 0.0

def parse_volume_range(range_str, value):
    """Check if value falls within a volume range string (e.g., '100k-1m')."""
    if not range_str: return True
    
    def parse_val(s):
        s = str(s).lower().replace('€', '').strip()
        mult = 1
        if 'k' in s: mult = 1000; s = s.replace('k', '')
        if 'm' in s: mult = 1000000; s = s.replace('m', '')
        try:
            return float(s) * mult
        except:
            return 0.0

    try:
        if '-' in range_str:
            low, high = range_str.split('-')
            return parse_val(low) <= value <= parse_val(high)
        if '>' in range_str:
            return value > parse_val(range_str.replace('>', ''))
        if '<' in range_str:
            return value < parse_val(range_str.replace('<', ''))
        return value == parse_val(range_str)
    except:
        return False

def parse_fraud_range(range_str, value):
    """Check if value falls within a fraud percentage range (e.g., '>8.3%')."""
    if not range_str: return True
    
    def parse_pct(s):
        s = str(s).replace('%', '').strip()
        try:
            return float(s) / 100.0
        except:
            return 0.0
    
    try:
        if '-' in range_str:
            low, high = range_str.split('-')
            return parse_pct(low) <= value <= parse_pct(high)
        if '>' in range_str:
            return value > parse_pct(range_str.replace('>', ''))
        if '<' in range_str:
            return value < parse_pct(range_str.replace('<', ''))
        return False
    except:
        return False

def match_capture_delay(rule_val, merch_val):
    """Match capture delay rules against merchant setting."""
    if not rule_val: return True
    # Direct string match
    if str(rule_val).lower() == str(merch_val).lower(): return True
    
    # Try numeric comparison if merchant value is numeric
    try:
        m_float = float(merch_val)
        if '-' in rule_val:
            l, h = map(float, rule_val.split('-'))
            return l <= m_float <= h
        if '>' in rule_val:
            return m_float > float(rule_val.replace('>', ''))
        if '<' in rule_val:
            return m_float < float(rule_val.replace('<', ''))
    except:
        pass
    return False

def match_fee_rule(ctx, rule):
    """
    Check if a fee rule applies to a transaction context.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != ctx['card_scheme']:
        return False

    # 2. Account Type (List match or Wildcard)
    if rule.get('account_type') and ctx['account_type'] not in rule['account_type']:
        return False

    # 3. Merchant Category Code (List match or Wildcard)
    if rule.get('merchant_category_code') and ctx['mcc'] not in rule['merchant_category_code']:
        return False

    # 4. Is Credit (Bool match or Wildcard)
    # Note: rule['is_credit'] can be True, False, or None
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != ctx['is_credit']:
            return False

    # 5. ACI (List match or Wildcard)
    if rule.get('aci') and ctx['aci'] not in rule['aci']:
        return False

    # 6. Intracountry (Bool match or Wildcard)
    # rule['intracountry'] is 0.0, 1.0, or None.
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != ctx['intracountry']:
            return False

    # 7. Capture Delay (Complex match or Wildcard)
    if not match_capture_delay(rule.get('capture_delay'), ctx['capture_delay']):
        return False

    # 8. Monthly Volume (Range match or Wildcard)
    if not parse_volume_range(rule.get('monthly_volume'), ctx['monthly_volume']):
        return False

    # 9. Monthly Fraud Level (Range match or Wildcard)
    if not parse_fraud_range(rule.get('monthly_fraud_level'), ctx['monthly_fraud_rate']):
        return False

    return True

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

def main():
    # 1. Load Data
    payments_path = '/output/chunk3/data/context/payments.csv'
    fees_path = '/output/chunk3/data/context/fees.json'
    merchant_path = '/output/chunk3/data/context/merchant_data.json'

    try:
        df_payments = pd.read_csv(payments_path)
        with open(fees_path, 'r') as f:
            fees = json.load(f)
        with open(merchant_path, 'r') as f:
            merchant_data = json.load(f)
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # 2. Filter for Rafa_AI, December 2023
    # December starts on Day 335 (non-leap year)
    target_merchant = 'Rafa_AI'
    
    # Filter by merchant first to reduce size
    df_merchant = df_payments[df_payments['merchant'] == target_merchant].copy()
    
    # Filter by date (Year 2023, Day >= 335)
    df_filtered = df_merchant[
        (df_merchant['year'] == 2023) & 
        (df_merchant['day_of_year'] >= 335)
    ].copy()

    if df_filtered.empty:
        print("No transactions found for Rafa_AI in Dec 2023.")
        return

    # 3. Calculate Monthly Stats (Volume & Fraud)
    # Manual: "Monthly volumes and rates are computed always in natural months"
    monthly_volume = df_filtered['eur_amount'].sum()
    
    fraud_txs = df_filtered[df_filtered['has_fraudulent_dispute'] == True]
    fraud_volume = fraud_txs['eur_amount'].sum()
    
    # Manual: "Fraud is defined as the ratio of fraudulent volume over total volume"
    monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

    # 4. Get Merchant Attributes
    merch_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merch_info:
        print(f"Merchant {target_merchant} not found in merchant_data.")
        return

    # 5. Get Fee ID 141 Details
    fee_141 = next((f for f in fees if f['ID'] == 141), None)
    if not fee_141:
        print("Fee ID 141 not found.")
        return
    
    old_rate = fee_141['rate']
    new_rate = 1.0  # As per question
    
    # 6. Calculate Delta
    total_delta = 0.0
    
    # Sort fees by ID to ensure deterministic matching order
    fees_sorted = sorted(fees, key=lambda x: x['ID'])

    # Pre-calculate merchant context to avoid repetition
    merch_ctx = {
        'account_type': merch_info['account_type'],
        'mcc': merch_info['merchant_category_code'],
        'capture_delay': merch_info['capture_delay'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate
    }

    # Iterate through transactions
    for _, tx in df_filtered.iterrows():
        # Build transaction context
        ctx = merch_ctx.copy()
        ctx.update({
            'card_scheme': tx['card_scheme'],
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'intracountry': tx['issuing_country'] == tx['acquirer_country'],
            'amount': tx['eur_amount']
        })

        # Find the applicable fee
        matched_fee_id = None
        for rule in fees_sorted:
            if match_fee_rule(ctx, rule):
                matched_fee_id = rule['ID']
                break
        
        # If the applicable fee is ID 141, calculate the delta
        if matched_fee_id == 141:
            # Delta = (new_rate - old_rate) * amt / 10000
            delta = (new_rate - old_rate) * ctx['amount'] / 10000.0
            total_delta += delta

    # 7. Output Result
    # Use high precision for currency calculations
    print(f"{total_delta:.14f}")

if __name__ == "__main__":
    main()
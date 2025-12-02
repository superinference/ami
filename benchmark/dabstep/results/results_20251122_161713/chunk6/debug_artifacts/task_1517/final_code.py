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

def parse_volume_range(range_str):
    """Parses volume strings like '100k-1m', '>10m' into (min, max)."""
    if not range_str:
        return (0, float('inf'))
    
    s = range_str.lower().replace(',', '').replace('€', '').strip()
    
    def parse_val(val_s):
        val_s = val_s.strip()
        mult = 1
        if 'k' in val_s:
            mult = 1000
            val_s = val_s.replace('k', '')
        elif 'm' in val_s:
            mult = 1000000
            val_s = val_s.replace('m', '')
        return float(val_s) * mult

    if '-' in s:
        parts = s.split('-')
        return (parse_val(parts[0]), parse_val(parts[1]))
    elif '>' in s:
        return (parse_val(s.replace('>', '')), float('inf'))
    elif '<' in s:
        return (0, parse_val(s.replace('<', '')))
    else:
        return (0, float('inf'))

def parse_fraud_range(range_str):
    """Parses fraud strings like '0%-0.5%', '>8.3%' into (min, max)."""
    if not range_str:
        return (0, float('inf'))
    
    s = range_str.replace('%', '').strip()
    
    def parse_val(val_s):
        return float(val_s) / 100.0 # Convert 8.3 to 0.083

    if '-' in s:
        parts = s.split('-')
        return (parse_val(parts[0]), parse_val(parts[1]))
    elif '>' in s:
        return (parse_val(s.replace('>', '')), float('inf'))
    elif '<' in s:
        return (0, parse_val(s.replace('<', '')))
    else:
        return (0, float('inf'))

def check_capture_delay(merchant_delay, rule_delay):
    """Checks if merchant capture delay matches the rule."""
    if not rule_delay:
        return True
    if str(rule_delay) == str(merchant_delay):
        return True
    
    # Handle numeric comparisons if delays are numeric strings
    try:
        md = float(merchant_delay)
        if '>' in rule_delay:
            limit = float(rule_delay.replace('>', ''))
            return md > limit
        if '<' in rule_delay:
            limit = float(rule_delay.replace('<', ''))
            return md < limit
        if '-' in rule_delay:
            low, high = map(float, rule_delay.split('-'))
            return low <= md <= high
    except ValueError:
        pass 
    return False

def match_fee_rule(tx_context, rule):
    """Determines if a fee rule applies to the specific transaction context."""
    # 1. Account Type
    if rule.get('account_type') and tx_context['account_type'] not in rule['account_type']:
        return False
    
    # 2. MCC
    if rule.get('merchant_category_code') and tx_context['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 3. is_credit (Boolean or None)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False
            
    # 4. ACI
    if rule.get('aci') and tx_context['aci'] not in rule['aci']:
        return False
        
    # 5. Intracountry (Boolean or None)
    if rule.get('intracountry') is not None:
        # JSON might have 0.0/1.0, convert to bool
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False
            
    # 6. Monthly Volume
    if rule.get('monthly_volume'):
        min_v, max_v = parse_volume_range(rule['monthly_volume'])
        if not (min_v <= tx_context['monthly_volume'] <= max_v):
            return False
            
    # 7. Monthly Fraud Level
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_fraud_range(rule['monthly_fraud_level'])
        if not (min_f <= tx_context['fraud_rate'] <= max_f):
            return False
            
    # 8. Capture Delay
    if rule.get('capture_delay'):
        if not check_capture_delay(tx_context['capture_delay'], rule['capture_delay']):
            return False
            
    return True

def main():
    # Paths
    payments_path = '/output/chunk6/data/context/payments.csv'
    merchant_path = '/output/chunk6/data/context/merchant_data.json'
    fees_path = '/output/chunk6/data/context/fees.json'
    
    # Load Data
    try:
        df_pay = pd.read_csv(payments_path)
        with open(merchant_path, 'r') as f:
            merchants = json.load(f)
        with open(fees_path, 'r') as f:
            fees = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # --- Step 1: Define Average Scenario Parameters ---
    # Mode of is_credit
    mode_is_credit = df_pay['is_credit'].mode()[0]
    
    # Mode of aci
    mode_aci = df_pay['aci'].mode()[0]
    
    # Mode of merchant
    mode_merchant = df_pay['merchant'].mode()[0]
    
    # Mode of intracountry (issuing == acquirer)
    df_pay['is_intracountry'] = df_pay['issuing_country'] == df_pay['acquirer_country']
    mode_intracountry = df_pay['is_intracountry'].mode()[0]
    
    print(f"Average Scenario: Merchant={mode_merchant}, Credit={mode_is_credit}, ACI={mode_aci}, Intra={mode_intracountry}")
    
    # --- Step 2: Get Merchant Specifics ---
    merchant_info = next((m for m in merchants if m['merchant'] == mode_merchant), None)
    if not merchant_info:
        print(f"Merchant {mode_merchant} not found in metadata.")
        return

    mcc = merchant_info['merchant_category_code']
    account_type = merchant_info['account_type']
    capture_delay = merchant_info['capture_delay']
    
    # --- Step 3: Calculate Merchant Metrics (Volume & Fraud) ---
    # Filter for the mode merchant
    df_m = df_pay[df_pay['merchant'] == mode_merchant]
    
    # Monthly Volume (Total 2023 Volume / 12)
    total_vol = df_m['eur_amount'].sum()
    monthly_vol = total_vol / 12.0
    
    # Fraud Rate (Fraud Volume / Total Volume)
    fraud_vol = df_m[df_m['has_fraudulent_dispute']]['eur_amount'].sum()
    fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    print(f"Merchant Metrics: Monthly Vol={monthly_vol:.2f}, Fraud Rate={fraud_rate:.4%}")
    
    # --- Step 4: Calculate Fees for 1234 EUR ---
    tx_amount = 1234.0
    
    # Context for matching rules
    context = {
        'account_type': account_type,
        'mcc': mcc,
        'is_credit': mode_is_credit,
        'aci': mode_aci,
        'intracountry': mode_intracountry,
        'monthly_volume': monthly_vol,
        'fraud_rate': fraud_rate,
        'capture_delay': capture_delay
    }
    
    # Get unique schemes
    schemes = set(f['card_scheme'] for f in fees)
    results = {}
    
    for scheme in schemes:
        # Filter rules for this scheme
        scheme_rules = [f for f in fees if f['card_scheme'] == scheme]
        
        valid_rule = None
        # Find the first matching rule
        for rule in scheme_rules:
            if match_fee_rule(context, rule):
                valid_rule = rule
                break
        
        if valid_rule:
            # Calculate Fee: fixed + (rate * amount / 10000)
            fixed = valid_rule['fixed_amount']
            rate = valid_rule['rate']
            fee = fixed + (rate * tx_amount / 10000.0)
            results[scheme] = fee
            print(f"Scheme: {scheme}, Fee: {fee:.4f} (Fixed: {fixed}, Rate: {rate})")
        else:
            print(f"Scheme: {scheme}, No matching rule found.")
            
    # --- Step 5: Find Cheapest ---
    if results:
        cheapest_scheme = min(results, key=results.get)
        min_fee = results[cheapest_scheme]
        print(f"\nCheapest Scheme: {cheapest_scheme} with fee {min_fee:.4f}")
        print(cheapest_scheme) # Final Answer
    else:
        print("No applicable schemes found.")

if __name__ == "__main__":
    main()
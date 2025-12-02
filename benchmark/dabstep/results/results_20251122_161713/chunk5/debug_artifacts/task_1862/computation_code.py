import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        # Handle inequalities by stripping them for the value, logic handled in check_range
        v_clean = v.lstrip('><≤≥') 
        if '%' in v:
            return float(v_clean.replace('%', '')) / 100
        # Range handling (e.g., "50-60") - return mean if forced to coerce, 
        # but usually check_range handles the string directly.
        if '-' in v_clean and len(v_clean.split('-')) == 2:
            try:
                parts = v_clean.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        try:
            return float(v_clean)
        except ValueError:
            return 0.0
    return float(value)

def parse_volume_string(vol_str):
    """Parses '100k', '1m' into floats."""
    if not isinstance(vol_str, str): return 0.0
    s = vol_str.lower().replace(',', '').replace('€', '').strip()
    if 'k' in s:
        return float(s.replace('k', '')) * 1000
    if 'm' in s:
        return float(s.replace('m', '')) * 1000000
    return float(s)

def check_range(value, range_str, is_percentage=False):
    """Checks if value is within the range string (e.g., '100k-1m', '>5', '>8.3%')."""
    if range_str is None:
        return True
    
    s = str(range_str).strip()
    
    # Handle inequalities
    if s.startswith('>'):
        limit_str = s[1:]
        limit = coerce_to_float(limit_str) if is_percentage else parse_volume_string(limit_str)
        # If percentage, ensure we are comparing apples to apples (0.08 vs 0.08)
        return value > limit
    if s.startswith('<'):
        limit_str = s[1:]
        limit = coerce_to_float(limit_str) if is_percentage else parse_volume_string(limit_str)
        return value < limit
        
    # Handle ranges
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            min_val = coerce_to_float(parts[0]) if is_percentage else parse_volume_string(parts[0])
            max_val = coerce_to_float(parts[1]) if is_percentage else parse_volume_string(parts[1])
            return min_val <= value <= max_val
            
    # Handle exact match
    val = coerce_to_float(s) if is_percentage else parse_volume_string(s)
    return value == val

def match_fee_rule(tx_context, rule):
    """
    Matches a transaction context against a fee rule.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match or Wildcard)
    # If rule['account_type'] is empty list [], it applies to ALL
    if rule.get('account_type') and len(rule['account_type']) > 0:
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Capture Delay (Exact match, Range, or Wildcard)
    if rule.get('capture_delay') is not None:
        rd = str(rule['capture_delay'])
        td = str(tx_context['capture_delay'])
        
        # Direct string match first (e.g. "manual" == "manual")
        if rd == td:
            pass
        # Check for range/inequality logic if not exact match
        elif any(x in rd for x in ['<', '>', '-']):
             # Try to convert transaction delay to number if possible (e.g. "1" -> 1.0)
             # If transaction delay is "manual" or "immediate", it won't match a numeric range like ">5"
             try:
                 td_val = float(td)
                 if not check_range(td_val, rd):
                     return False
             except ValueError:
                 # Transaction delay is non-numeric (e.g. "manual"), rule is numeric range (e.g. ">5")
                 # They don't match.
                 return False
        else:
            # Exact match failed and not a range
            return False

    # 4. Merchant Category Code (List match or Wildcard)
    if rule.get('merchant_category_code') and len(rule['merchant_category_code']) > 0:
        if tx_context['merchant_category_code'] not in rule['merchant_category_code']:
            return False

    # 5. Is Credit (Boolean match or Wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 6. ACI (List match or Wildcard)
    if rule.get('aci') and len(rule['aci']) > 0:
        if tx_context['aci'] not in rule['aci']:
            return False

    # 7. Intracountry (Boolean match or Wildcard)
    if rule.get('intracountry') is not None:
        # JSON bool/float/int handling (0.0 -> False, 1.0 -> True)
        # Convert rule value to bool to compare with tx boolean
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False

    # 8. Monthly Volume (Range check)
    if rule.get('monthly_volume') is not None:
        if not check_range(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range check)
    if rule.get('monthly_fraud_level') is not None:
        if not check_range(tx_context['monthly_fraud_rate'], rule['monthly_fraud_level'], is_percentage=True):
            return False

    return True

# --- Main Execution ---

# Define file paths
payments_file = '/output/chunk5/data/context/payments.csv'
merchant_file = '/output/chunk5/data/context/merchant_data.json'
fees_file = '/output/chunk5/data/context/fees.json'

# Load data
df_payments = pd.read_csv(payments_file)
with open(merchant_file, 'r') as f:
    merchant_data = json.load(f)
with open(fees_file, 'r') as f:
    fees_data = json.load(f)

# Filter for Rafa_AI in April 2023 (Days 91-120)
target_merchant = 'Rafa_AI'
df_merchant = df_payments[df_payments['merchant'] == target_merchant]
df_april = df_merchant[(df_merchant['day_of_year'] >= 91) & (df_merchant['day_of_year'] <= 120)].copy()

# Calculate Monthly Stats (Volume and Fraud)
# Volume: Sum of eur_amount
monthly_volume = df_april['eur_amount'].sum()

# Fraud: Sum of eur_amount where has_fraudulent_dispute is True (Volume-based fraud rate)
fraud_volume = df_april[df_april['has_fraudulent_dispute'] == True]['eur_amount'].sum()
monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# Get Merchant Config
merchant_config = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not merchant_config:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

# Calculate Fees
total_fees = 0.0
matched_count = 0
unmatched_count = 0

for _, tx in df_april.iterrows():
    # Determine if transaction is intracountry (Issuer Country == Acquirer Country)
    is_intra = (tx['issuing_country'] == tx['acquirer_country'])
    
    # Build Context for Rule Matching
    context = {
        'card_scheme': tx['card_scheme'],
        'account_type': merchant_config['account_type'],
        'capture_delay': merchant_config['capture_delay'],
        'merchant_category_code': merchant_config['merchant_category_code'],
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': is_intra,
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate
    }
    
    # Find First Matching Rule
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(context, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        # Calculate Fee: fixed_amount + (rate * amount / 10000)
        fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * tx['eur_amount'] / 10000)
        total_fees += fee
        matched_count += 1
    else:
        unmatched_count += 1

# Output the result
print(f"{total_fees:.2f}")
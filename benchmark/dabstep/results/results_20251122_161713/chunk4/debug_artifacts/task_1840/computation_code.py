import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if pd.isna(value) or value == '':
        return 0.0
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
        except ValueError:
            return 0.0
    return float(value)

def parse_range(range_str):
    """Parses a range string like '100k-1m', '<5%', '>10' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip().lower().replace(',', '')
    # Handle percentages in range definition (e.g. "0%-5%")
    # We will strip % and treat as raw numbers (0-5), expecting input to be scaled similarly
    is_pct = '%' in s
    s = s.replace('%', '')
    
    multiplier = 1
    if 'k' in s:
        multiplier = 1000
        s = s.replace('k', '')
    elif 'm' in s:
        multiplier = 1000000
        s = s.replace('m', '')
        
    try:
        if '-' in s:
            parts = s.split('-')
            return float(parts[0]) * multiplier, float(parts[1]) * multiplier
        elif s.startswith('>'):
            return float(s[1:]) * multiplier, float('inf')
        elif s.startswith('<'):
            return float('-inf'), float(s[1:]) * multiplier
    except:
        pass
    return None, None

def is_in_range(value, range_str):
    """Checks if a value fits within a range string."""
    if range_str is None:
        return True # Wildcard matches all
    
    low, high = parse_range(range_str)
    if low is None: 
        return True
        
    # Logic to handle percentage scaling mismatch
    # If range was "5%-10%" (parsed as 5-10) and value is 0.08, we scale value to 8
    # If range was "0.05-0.10" and value is 0.08, we don't scale
    # Heuristic: if range values are > 1 and value is < 1, scale value by 100
    check_val = value
    if (low > 1.0 or high > 1.0) and -1.0 < value < 1.0 and value != 0:
        check_val = value * 100
        
    return low <= check_val <= high

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    tx_ctx: dict containing transaction details and monthly stats
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List match)
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Boolean match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 5. ACI (List match)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        # Intracountry means Issuer Country == Acquirer Country
        is_intra = (tx_ctx['issuing_country'] == tx_ctx['acquirer_country'])
        # Rule expects boolean or 0.0/1.0
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    # 7. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not is_in_range(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 8. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        # Context has fraud rate as ratio (e.g. 0.05). Helper handles scaling to % if needed.
        if not is_in_range(tx_ctx['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False
            
    # 9. Capture Delay (Complex match)
    if rule.get('capture_delay'):
        rule_delay = rule['capture_delay']
        merch_delay = str(tx_ctx['capture_delay']).lower()
        
        # Handle specific keywords
        if rule_delay == 'manual' or merch_delay == 'manual':
            if rule_delay != merch_delay:
                return False
        elif rule_delay == 'immediate' or merch_delay == 'immediate':
             # If rule is immediate, merch must be immediate.
             # If rule is numeric (e.g. <3), immediate (0) might fit.
             if rule_delay == 'immediate' and merch_delay != 'immediate':
                 return False
             if merch_delay == 'immediate':
                 merch_delay_val = 0.0
             else:
                 try:
                     merch_delay_val = float(merch_delay)
                 except:
                     return False # Cannot compare string to numeric rule
             
             if not is_in_range(merch_delay_val, rule_delay):
                 return False
        else:
            # Both are likely numeric or ranges
            try:
                merch_delay_val = float(merch_delay)
                if not is_in_range(merch_delay_val, rule_delay):
                    return False
            except:
                # Fallback for string mismatch
                if rule_delay != merch_delay:
                    return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate (basis points)."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0)
    # Formula from manual: fee = fixed_amount + rate * transaction_value / 10000
    return fixed + (rate * amount / 10000.0)

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# 1. Load Data
base_path = '/output/chunk4/data/context/'
df_payments = pd.read_csv(base_path + 'payments.csv')
with open(base_path + 'merchant_data.json', 'r') as f:
    merchant_data = json.load(f)
with open(base_path + 'fees.json', 'r') as f:
    fees_data = json.load(f)

# 2. Define Context
target_merchant = 'Golfclub_Baron_Friso'
start_day = 152  # June 1st
end_day = 181    # June 30th

# 3. Get Merchant Attributes
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

# 4. Calculate Monthly Stats (Volume & Fraud) for June
# Filter for the specific merchant and month (June)
df_june = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['day_of_year'] >= start_day) & 
    (df_payments['day_of_year'] <= end_day)
].copy()

if len(df_june) == 0:
    print(f"No transactions found for {target_merchant} in June.")
    exit()

monthly_volume = df_june['eur_amount'].sum()

# Fraud Rate Calculation (Volume based per Manual Section 7 & 5)
# "ratio between monthly total volume and monthly volume notified as fraud"
fraud_volume = df_june[df_june['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 5. Calculate Fees for Each Transaction
total_fees = 0.0
matched_count = 0
unmatched_count = 0

# Sort fees to ensure consistent matching order (though logic should be robust)
# We assume the order in JSON is relevant if multiple rules match, taking the first one.
sorted_fees = fees_data 

for _, tx in df_june.iterrows():
    # Build transaction context
    tx_ctx = {
        'card_scheme': tx['card_scheme'],
        'account_type': account_type,
        'mcc': mcc,
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'issuing_country': tx['issuing_country'],
        'acquirer_country': tx['acquirer_country'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate,
        'capture_delay': capture_delay
    }
    
    # Find matching rule
    match = None
    for rule in sorted_fees:
        if match_fee_rule(tx_ctx, rule):
            match = rule
            break
            
    if match:
        fee = calculate_fee(tx['eur_amount'], match)
        total_fees += fee
        matched_count += 1
    else:
        unmatched_count += 1

# 6. Output Result
print(f"{total_fees:.2f}")
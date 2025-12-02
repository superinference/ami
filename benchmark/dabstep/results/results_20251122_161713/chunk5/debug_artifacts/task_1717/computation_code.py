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

def parse_range_check(value, rule_value, is_percentage=False):
    """
    Checks if value fits in rule_value range.
    rule_value examples: "100k-1m", ">5", "<3", "7.7%-8.3%", "manual", "immediate"
    value: float (volume, fraud_rate) or string (capture_delay)
    """
    if rule_value is None:
        return True
    
    # Handle string exact matches (e.g. capture_delay="manual")
    if isinstance(rule_value, str) and isinstance(value, str):
        if rule_value == value:
            return True
        # If one is numeric string and other is range, continue to parsing
        try:
            float(value)
        except ValueError:
            # value is non-numeric string (e.g. "manual"), rule is not equal (checked above)
            # so return False
            return False

    # Parse rule string
    s = str(rule_value).lower().strip()
    
    # Handle percentages in rule
    rule_is_percent = '%' in s
    s = s.replace('%', '').replace(',', '')
    
    # Handle k/m suffixes
    def parse_num(v):
        v = v.strip()
        mult = 1
        if v.endswith('k'): mult = 1000; v = v[:-1]
        elif v.endswith('m'): mult = 1000000; v = v[:-1]
        return float(v) * mult

    # Determine bounds
    lower = float('-inf')
    upper = float('inf')
    
    if '-' in s:
        parts = s.split('-')
        lower = parse_num(parts[0])
        upper = parse_num(parts[1])
    elif s.startswith('>'):
        lower = parse_num(s[1:])
    elif s.startswith('<'):
        upper = parse_num(s[1:])
    else:
        # Exact numeric match
        try:
            val = parse_num(s)
            lower = val
            upper = val
        except:
            return False # Should have been caught by string match

    # Prepare comparison value
    # If rule was percentage (e.g. "8.3%"), it parsed to 8.3.
    # If value is ratio (0.083), multiply by 100.
    comp_val = value
    if is_percentage:
        comp_val = value * 100
        
    return lower <= comp_val <= upper

def match_fee_rule(tx, rule, merchant_ctx):
    # 1. Card Scheme
    if rule['card_scheme'] != tx['card_scheme']:
        return False
        
    # 2. Account Type (Wildcard [])
    if rule['account_type'] and merchant_ctx['account_type'] not in rule['account_type']:
        return False
        
    # 3. MCC (Wildcard [])
    if rule['merchant_category_code'] and merchant_ctx['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 4. Capture Delay (Wildcard null)
    if rule['capture_delay']:
        if not parse_range_check(merchant_ctx['capture_delay'], rule['capture_delay']):
            return False

    # 5. Monthly Volume (Wildcard null)
    if rule['monthly_volume']:
        if not parse_range_check(merchant_ctx['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 6. Monthly Fraud Level (Wildcard null)
    if rule['monthly_fraud_level']:
        if not parse_range_check(merchant_ctx['monthly_fraud_level'], rule['monthly_fraud_level'], is_percentage=True):
            return False

    # 7. Is Credit (Wildcard null)
    if rule['is_credit'] is not None:
        # tx['is_credit'] is boolean
        # rule['is_credit'] is boolean or None
        if bool(rule['is_credit']) != bool(tx['is_credit']):
            return False

    # 8. ACI (Wildcard [])
    if rule['aci'] and tx['aci'] not in rule['aci']:
        return False
        
    # 9. Intracountry (Wildcard null)
    if rule['intracountry'] is not None:
        is_intra = (tx['issuing_country'] == tx['acquirer_country'])
        # rule['intracountry'] is 0.0 or 1.0
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False
            
    return True

# --- Main Execution ---
# Load Data
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_path = '/output/chunk5/data/context/merchant_data.json'

df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# Target Merchant
target_merchant = "Crossfit_Hanna"
target_day = 10

# Get Merchant Metadata
m_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not m_info:
    print(f"Merchant {target_merchant} not found in merchant_data.json")
    exit()

# Calculate Monthly Stats (January)
# Filter for Jan (Day 1-31)
jan_mask = (df['merchant'] == target_merchant) & (df['day_of_year'] <= 31)
jan_txs = df[jan_mask]

monthly_volume = jan_txs['eur_amount'].sum()
fraud_vol = jan_txs[jan_txs['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_level = (fraud_vol / monthly_volume) if monthly_volume > 0 else 0.0

# Prepare Merchant Context for Matching
merchant_ctx = {
    'account_type': m_info['account_type'],
    'mcc': m_info['merchant_category_code'],
    'capture_delay': m_info['capture_delay'],
    'monthly_volume': monthly_volume,
    'monthly_fraud_level': monthly_fraud_level
}

# Filter Target Transactions (Day 10)
target_mask = (df['merchant'] == target_merchant) & (df['day_of_year'] == target_day)
target_txs = df[target_mask]

# Calculate Fees
total_fees = 0.0
matched_count = 0
unmatched_count = 0

for _, tx in target_txs.iterrows():
    # Find matching rule
    matched_rule = None
    for rule in fees:
        if match_fee_rule(tx, rule, merchant_ctx):
            matched_rule = rule
            break # Take first match
    
    if matched_rule:
        # Fee = Fixed + (Rate * Amount / 10000)
        fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * tx['eur_amount'] / 10000)
        total_fees += fee
        matched_count += 1
    else:
        unmatched_count += 1

# Print result with high precision
print(f"{total_fees:.14f}")
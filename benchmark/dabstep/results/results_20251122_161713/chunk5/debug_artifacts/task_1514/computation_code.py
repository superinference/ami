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
        v = v.lstrip('><≤≥')  # Remove comparison operators
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
    """Parses strings like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max)."""
    if range_str is None:
        return (-float('inf'), float('inf'))
    
    s = str(range_str).strip().lower()
    
    # Handle percentages
    is_pct = '%' in s
    s = s.replace('%', '')
    
    # Handle k/m suffixes
    def parse_val(v):
        v = v.strip()
        mult = 1
        if v.endswith('k'):
            mult = 1000
            v = v[:-1]
        elif v.endswith('m'):
            mult = 1000000
            v = v[:-1]
        try:
            return float(v) * mult
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        low = parse_val(parts[0])
        high = parse_val(parts[1])
        if is_pct:
            low /= 100
            high /= 100
        return (low, high)
    
    if s.startswith('>'):
        val = parse_val(s[1:])
        if is_pct: val /= 100
        return (val + 1e-9, float('inf')) # strictly greater
    
    if s.startswith('<'):
        val = parse_val(s[1:])
        if is_pct: val /= 100
        return (-float('inf'), val - 1e-9) # strictly less
        
    # Exact value fallback
    val = parse_val(s)
    if is_pct: val /= 100
    return (val, val)

def match_fee_rule(tx_profile, rule):
    """Checks if a transaction profile matches a fee rule."""
    # 1. Card Scheme (Must match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_profile['card_scheme']:
        return False
        
    # 2. Account Type (List - Wildcard if empty)
    if rule.get('account_type'): 
        if tx_profile['account_type'] not in rule['account_type']:
            return False
            
    # 3. MCC (List - Wildcard if empty)
    if rule.get('merchant_category_code'):
        if tx_profile['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (String/Range)
    if rule.get('capture_delay'):
        r_cd = rule['capture_delay']
        t_cd = str(tx_profile['capture_delay'])
        
        # Direct string match for non-numeric delays
        if r_cd in ['manual', 'immediate']:
            if t_cd != r_cd: return False
        # Numeric range check
        else:
            if t_cd in ['manual', 'immediate']:
                return False # Mismatch type (rule expects number, got string)
            try:
                days = float(t_cd)
                (min_d, max_d) = parse_range(r_cd)
                if not (min_d <= days <= max_d):
                    return False
            except:
                return False

    # 5. Monthly Fraud Level (Range)
    if rule.get('monthly_fraud_level'):
        (min_f, max_f) = parse_range(rule['monthly_fraud_level'])
        if not (min_f <= tx_profile['monthly_fraud_level'] <= max_f):
            return False

    # 6. Monthly Volume (Range)
    if rule.get('monthly_volume'):
        (min_v, max_v) = parse_range(rule['monthly_volume'])
        if not (min_v <= tx_profile['monthly_volume'] <= max_v):
            return False

    # 7. Is Credit (Bool)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_profile['is_credit']:
            return False

    # 8. ACI (List - Wildcard if empty)
    if rule.get('aci'):
        if tx_profile['aci'] not in rule['aci']:
            return False

    # 9. Intracountry (Bool - 0.0/1.0/None)
    if rule.get('intracountry') is not None:
        rule_intra = bool(float(rule['intracountry'])) # Handle 0.0/1.0 strings
        if rule_intra != tx_profile['intracountry']:
            return False

    return True

# --- MAIN SCRIPT ---

# 1. Load Data
try:
    df_payments = pd.read_csv('/output/chunk5/data/context/payments.csv')
    with open('/output/chunk5/data/context/fees.json', 'r') as f:
        fees_data = json.load(f)
    with open('/output/chunk5/data/context/merchant_data.json', 'r') as f:
        merchant_data = json.load(f)
except Exception as e:
    print(f"Error loading files: {e}")
    exit()

# 2. Construct "Average Scenario" Profile

# A. Find Mode Merchant
mode_merchant = df_payments['merchant'].mode()[0]

# B. Get Merchant Attributes from merchant_data.json
merch_info = next((m for m in merchant_data if m['merchant'] == mode_merchant), None)
if not merch_info:
    print(f"Error: Merchant {mode_merchant} not found in merchant_data.json")
    exit()

profile_account_type = merch_info['account_type']
profile_mcc = merch_info['merchant_category_code']
profile_capture_delay = merch_info['capture_delay']

# C. Calculate Merchant Stats (Volume & Fraud) from payments.csv
# Filter for the specific merchant
merch_txs = df_payments[df_payments['merchant'] == mode_merchant]

# Monthly Volume: Total Volume / 12 (Data is for 2023)
total_vol = merch_txs['eur_amount'].sum()
profile_monthly_vol = total_vol / 12.0

# Fraud Level: Fraud Volume / Total Volume
# Note: Fraud level is ratio of fraudulent volume to total volume
fraud_vol = merch_txs[merch_txs['has_fraudulent_dispute']]['eur_amount'].sum()
profile_fraud_level = fraud_vol / total_vol if total_vol > 0 else 0.0

# D. Find Global Transaction Modes (from entire dataset)
profile_is_credit = df_payments['is_credit'].mode()[0]
profile_aci = df_payments['aci'].mode()[0]

# E. Find Intracountry Mode
# Intracountry means Issuing Country == Acquirer Country
df_payments['is_intracountry'] = df_payments['issuing_country'] == df_payments['acquirer_country']
profile_intracountry = df_payments['is_intracountry'].mode()[0]

# 3. Calculate Fees for Each Scheme
schemes = ['GlobalCard', 'NexPay', 'SwiftCharge', 'TransactPlus']
tx_amount = 1000.0
scheme_fees = {}

for scheme in schemes:
    # Create profile for this specific scheme
    profile = {
        'card_scheme': scheme,
        'account_type': profile_account_type,
        'mcc': profile_mcc,
        'capture_delay': profile_capture_delay,
        'monthly_volume': profile_monthly_vol,
        'monthly_fraud_level': profile_fraud_level,
        'is_credit': profile_is_credit,
        'aci': profile_aci,
        'intracountry': profile_intracountry
    }
    
    # Find matching rule
    # We iterate through all rules. If multiple match, we need a tie-breaking strategy.
    # Usually, fee engines take the first match or the most specific one.
    # Given the structure, we'll assume the first valid match in the list applies.
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(profile, rule):
            matched_rule = rule
            break # Stop at first match
            
    if matched_rule:
        # Fee = Fixed + (Rate * Amount / 10000)
        # Rate is an integer (basis points usually, but formula says /10000)
        fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * tx_amount / 10000.0)
        scheme_fees[scheme] = fee
    else:
        # If no rule matches, we can't determine the fee. 
        # For the purpose of finding the "most expensive", we might ignore it or flag it.
        scheme_fees[scheme] = -1.0

# 4. Identify Most Expensive Scheme
if scheme_fees:
    # Filter out invalid fees (-1.0)
    valid_fees = {k: v for k, v in scheme_fees.items() if v >= 0}
    
    if valid_fees:
        most_expensive_scheme = max(valid_fees, key=valid_fees.get)
        print(most_expensive_scheme)
    else:
        print("No valid fee rules found.")
else:
    print("No fees calculated.")
import pandas as pd
import json
import numpy as np

# --- Helper Functions ---
def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
    if value is None: return None
    if isinstance(value, (int, float)): return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '').replace('_', '')
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100.0
            except:
                return None
        if 'k' in v.lower():
            try:
                return float(v.lower().replace('k', '')) * 1000
            except:
                return None
        if 'm' in v.lower():
            try:
                return float(v.lower().replace('m', '')) * 1000000
            except:
                return None
        try:
            return float(v)
        except:
            return None
    return None

def parse_range(range_str, value):
    """
    Parses a range string (e.g., '100k-1m', '<3', '>5', '3-5') and checks if value is in it.
    Value should be a float.
    """
    if range_str is None:
        return True
    if value is None:
        return False
        
    s = str(range_str).strip()
    
    # Handle inequalities
    if s.startswith('<'):
        limit = coerce_to_float(s[1:])
        if limit is not None:
            return value < limit
    if s.startswith('>'):
        limit = coerce_to_float(s[1:])
        if limit is not None:
            return value > limit
        
    # Handle ranges
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            low = coerce_to_float(parts[0])
            high = coerce_to_float(parts[1])
            if low is not None and high is not None:
                return low <= value <= high
            
    # Handle exact match (numeric)
    val_rule = coerce_to_float(s)
    if val_rule is not None:
        # Use a small epsilon for float comparison if needed, or direct equality
        return abs(value - val_rule) < 1e-9
        
    return False

def check_capture_delay(rule_delay, merchant_delay):
    if rule_delay is None:
        return True
    if merchant_delay is None:
        return False
        
    # Exact string match (e.g., 'manual', 'immediate')
    if str(rule_delay).lower() == str(merchant_delay).lower():
        return True
        
    # Numeric comparison
    # Convert merchant_delay to float if possible
    try:
        m_val = float(merchant_delay)
        return parse_range(rule_delay, m_val)
    except ValueError:
        # Merchant delay is non-numeric (e.g. 'manual'), but rule might be numeric range
        # In this case, they don't match unless handled above
        return False

def match_fee_rule(tx_ctx, rule):
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List containment or wildcard)
    # Rule field is a list. If empty/null, matches all.
    if rule.get('account_type') and len(rule['account_type']) > 0:
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List containment or wildcard)
    if rule.get('merchant_category_code') and len(rule['merchant_category_code']) > 0:
        if tx_ctx['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 4. ACI (List containment or wildcard)
    if rule.get('aci') and len(rule['aci']) > 0:
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 5. Is Credit (Boolean match or wildcard)
    if rule.get('is_credit') is not None:
        # Handle string 'None' or actual None in JSON
        if str(rule['is_credit']).lower() != 'none':
            if bool(rule['is_credit']) != tx_ctx['is_credit']:
                return False

    # 6. Intracountry (Boolean match or wildcard)
    if rule.get('intracountry') is not None:
        if str(rule['intracountry']).lower() != 'none':
            # rule['intracountry'] might be 0.0 or 1.0
            try:
                rule_intra = bool(float(rule['intracountry']))
                if rule_intra != tx_ctx['is_intracountry']:
                    return False
            except:
                pass # If conversion fails, ignore or fail? Assuming valid data.
                
    # 7. Capture Delay (Complex match)
    if not check_capture_delay(rule.get('capture_delay'), tx_ctx['capture_delay']):
        return False
        
    # 8. Monthly Volume (Range match)
    if not parse_range(rule.get('monthly_volume'), tx_ctx['monthly_volume']):
        return False
        
    # 9. Monthly Fraud Level (Range match)
    if not parse_range(rule.get('monthly_fraud_level'), tx_ctx['monthly_fraud_level']):
        return False
        
    return True

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk2/data/context/payments.csv'
fees_path = '/output/chunk2/data/context/fees.json'
merchant_path = '/output/chunk2/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

target_merchant = 'Golfclub_Baron_Friso'
target_day = 200
target_year = 2023

# 2. Get Merchant Profile
merchant_profile = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_profile:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
    exit()

# 3. Calculate Monthly Stats for July (Days 182-212)
# Note: Manual says "monthly volumes and rates are computed always in natural months"
# July is roughly day 182 to 212 (non-leap year).
july_mask = (df_payments['merchant'] == target_merchant) & \
            (df_payments['year'] == target_year) & \
            (df_payments['day_of_year'] >= 182) & \
            (df_payments['day_of_year'] <= 212)

df_july = df_payments[july_mask]

monthly_vol = df_july['eur_amount'].sum()
fraud_vol = df_july[df_july['has_fraudulent_dispute']]['eur_amount'].sum()
# Fraud level is ratio of fraudulent volume over total volume
monthly_fraud_rate = fraud_vol / monthly_vol if monthly_vol > 0 else 0.0

# 4. Filter Target Transactions (Day 200)
target_mask = (df_payments['merchant'] == target_merchant) & \
              (df_payments['year'] == target_year) & \
              (df_payments['day_of_year'] == target_day)
              
target_txs = df_payments[target_mask]

# 5. Find Applicable Fees
applicable_fee_ids = set()

for _, tx in target_txs.iterrows():
    # Build Context
    is_intra = (tx['issuing_country'] == tx['acquirer_country'])
    
    ctx = {
        'card_scheme': tx['card_scheme'],
        'account_type': merchant_profile['account_type'],
        'merchant_category_code': merchant_profile['merchant_category_code'],
        'aci': tx['aci'],
        'is_credit': tx['is_credit'],
        'is_intracountry': is_intra,
        'capture_delay': merchant_profile['capture_delay'],
        'monthly_volume': monthly_vol,
        'monthly_fraud_level': monthly_fraud_rate
    }
    
    # Check against all rules
    for rule in fees_data:
        if match_fee_rule(ctx, rule):
            applicable_fee_ids.add(rule['ID'])

# 6. Output Result
sorted_ids = sorted(list(applicable_fee_ids))
print(sorted_ids)
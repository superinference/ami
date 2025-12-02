import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# Helper Functions
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return 0.0
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
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value)

def parse_volume_check(vol_str, actual_vol):
    """Check if actual volume falls within the rule's volume range string."""
    if not vol_str:  # None or empty matches all
        return True
    
    # Normalize string
    s = str(vol_str).lower().strip()
    
    # Helper to parse '100k', '1m'
    def parse_val(val_s):
        val_s = val_s.replace('k', '000').replace('m', '000000')
        return float(val_s)

    try:
        if '-' in s:
            low, high = s.split('-')
            return parse_val(low) <= actual_vol <= parse_val(high)
        elif s.startswith('>'):
            return actual_vol > parse_val(s[1:])
        elif s.startswith('<'):
            return actual_vol < parse_val(s[1:])
        else:
            # Exact match unlikely for volume, but handle it
            return actual_vol == parse_val(s)
    except:
        return False

def parse_fraud_check(fraud_str, actual_fraud_rate):
    """Check if actual fraud rate (0.0-1.0) falls within rule's range."""
    if not fraud_str:
        return True
    
    s = str(fraud_str).strip()
    
    # Helper to parse '8.3%' -> 0.083
    def parse_pct(val_s):
        val_s = val_s.replace('%', '')
        return float(val_s) / 100

    try:
        if '-' in s:
            low, high = s.split('-')
            return parse_pct(low) <= actual_fraud_rate <= parse_pct(high)
        elif s.startswith('>'):
            return actual_fraud_rate > parse_pct(s[1:])
        elif s.startswith('<'):
            return actual_fraud_rate < parse_pct(s[1:])
        else:
            return actual_fraud_rate == parse_pct(s)
    except:
        return False

def check_capture_delay(merchant_delay, rule_delay):
    """Check if merchant capture delay matches rule."""
    if rule_delay is None:
        return True
    
    m_val = str(merchant_delay).lower()
    r_val = str(rule_delay).lower()
    
    # Exact match
    if m_val == r_val:
        return True
    
    # Logic for 'immediate' matching '<3'
    # Map categorical to numeric days where possible
    # immediate = 0, manual = 999
    def to_days(val):
        if val == 'immediate': return 0
        if val == 'manual': return 999
        try:
            return float(val)
        except:
            return None

    m_days = to_days(m_val)
    
    if m_days is not None:
        if '-' in r_val:
            try:
                low, high = map(float, r_val.split('-'))
                return low <= m_days <= high
            except:
                pass
        elif r_val.startswith('<'):
            try:
                limit = float(r_val[1:])
                return m_days < limit
            except:
                pass
        elif r_val.startswith('>'):
            try:
                limit = float(r_val[1:])
                return m_days > limit
            except:
                pass
                
    return False

def match_fee_rule(tx_context, rule):
    """
    Check if a fee rule applies to a transaction context.
    tx_context must contain:
    - account_type, mcc, capture_delay, monthly_volume, monthly_fraud_level
    - is_credit, aci, intracountry
    """
    # 1. Account Type (List match)
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 2. MCC (List match)
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False

    # 3. Capture Delay (Complex match)
    if rule.get('capture_delay'):
        if not check_capture_delay(tx_context['capture_delay'], rule['capture_delay']):
            return False

    # 4. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not parse_volume_check(rule['monthly_volume'], tx_context['monthly_volume']):
            return False

    # 5. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not parse_fraud_check(rule['monthly_fraud_level'], tx_context['monthly_fraud_level']):
            return False

    # 6. Is Credit (Exact match)
    # rule['is_credit'] can be True, False, or None
    if rule.get('is_credit') is not None:
        # Ensure boolean comparison
        if bool(rule['is_credit']) != bool(tx_context['is_credit']):
            return False

    # 7. ACI (List match)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 8. Intracountry (Boolean match)
    # rule['intracountry'] can be 0.0, 1.0, or None
    if rule.get('intracountry') is not None:
        rule_intra = bool(float(rule['intracountry']))
        if rule_intra != tx_context['intracountry']:
            return False

    return True

# ═══════════════════════════════════════════════════════════
# Main Analysis Script
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk2/data/context/payments.csv'
fees_path = '/output/chunk2/data/context/fees.json'
merchant_data_path = '/output/chunk2/data/context/merchant_data.json'

df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Merchant and July
target_merchant = 'Martinis_Fine_Steakhouse'
# July 2023: Day 182 to 212
df_merchant = df[df['merchant'] == target_merchant]
df_july = df_merchant[(df_merchant['day_of_year'] >= 182) & (df_merchant['day_of_year'] <= 212)].copy()

# 3. Get Merchant Context
# Find merchant metadata
m_meta = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not m_meta:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

mcc = m_meta['merchant_category_code']
account_type = m_meta['account_type']
capture_delay = m_meta['capture_delay']

# Calculate Monthly Stats (Volume and Fraud)
# Note: These stats determine which fee tier the merchant falls into for the WHOLE month
monthly_volume = df_july['eur_amount'].sum()
fraud_count = df_july['has_fraudulent_dispute'].sum()
tx_count = len(df_july)
monthly_fraud_rate = (fraud_count / tx_count) if tx_count > 0 else 0.0

# 4. Simulate Fees for Each Scheme
candidate_schemes = ['GlobalCard', 'NexPay', 'SwiftCharge', 'TransactPlus']
results = {}

# Pre-calculate static context parts
base_context = {
    'account_type': account_type,
    'mcc': mcc,
    'capture_delay': capture_delay,
    'monthly_volume': monthly_volume,
    'monthly_fraud_level': monthly_fraud_rate
}

for scheme in candidate_schemes:
    total_fees = 0.0
    
    # Filter rules for this scheme to speed up matching
    scheme_rules = [r for r in fees_data if r['card_scheme'] == scheme]
    
    # Iterate through every transaction in July
    for _, row in df_july.iterrows():
        # Build full context for this transaction
        tx_context = base_context.copy()
        tx_context['is_credit'] = row['is_credit']
        tx_context['aci'] = row['aci']
        # Intracountry: Issuer == Acquirer
        # Use safe_get or direct access if columns guaranteed
        issuer = row['issuing_country']
        acquirer = row['acquirer_country']
        tx_context['intracountry'] = (issuer == acquirer)
        
        # Find the first matching rule
        matched_rule = None
        for rule in scheme_rules:
            if match_fee_rule(tx_context, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            # Calculate Fee: Fixed + (Rate * Amount / 10000)
            # Rate is in basis points (per 10k) usually, or specified as integer to divide by 10000
            # Manual says: "rate * transaction_value / 10000"
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * row['eur_amount'] / 10000)
            total_fees += fee
        else:
            # If no rule matches, this is a data gap. 
            # For robustness, we might assume a default or log it. 
            # In this synthetic set, we expect matches.
            # print(f"No match for tx {row['psp_reference']} on scheme {scheme}")
            pass
            
    results[scheme] = total_fees

# 5. Determine Winner
# We want the minimum fees
best_scheme = min(results, key=results.get)
min_fee = results[best_scheme]

# Output the answer
print(best_scheme)
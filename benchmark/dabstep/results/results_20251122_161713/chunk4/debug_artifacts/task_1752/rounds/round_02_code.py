# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1752
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8890 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
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
        # Range handling (e.g., "50-60") - return mean for simple coercion, 
        # but specific logic usually handles ranges separately.
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
    """Parses a range string like '100k-1m', '<3', '>5%', '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().replace(',', '').replace('_', '')
    is_percent = '%' in s
    s = s.replace('%', '')
    
    # Handle k/m suffixes
    def parse_val(val_s):
        val_s = val_s.strip()
        mult = 1
        if 'k' in val_s:
            mult = 1000
            val_s = val_s.replace('k', '')
        elif 'm' in val_s:
            mult = 1000000
            val_s = val_s.replace('m', '')
        try:
            return float(val_s) * mult
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        min_val = parse_val(parts[0])
        max_val = parse_val(parts[1])
    elif '>' in s:
        min_val = parse_val(s.replace('>', '').replace('=', ''))
        max_val = float('inf')
    elif '<' in s:
        min_val = float('-inf')
        max_val = parse_val(s.replace('<', '').replace('=', ''))
    else:
        # Exact match treated as range [val, val]
        val = parse_val(s)
        min_val = val
        max_val = val
        
    if is_percent:
        min_val /= 100.0
        if max_val != float('inf') and max_val != float('-inf'):
            max_val /= 100.0
            
    return min_val, max_val

def check_capture_delay(merchant_delay, rule_delay):
    """Checks if merchant capture delay matches the rule."""
    if rule_delay is None:
        return True
    
    # Normalize inputs
    m_delay = str(merchant_delay).lower()
    r_delay = str(rule_delay).lower()
    
    # Direct string match (e.g., "immediate", "manual")
    if m_delay == r_delay:
        return True
        
    # Numeric comparison
    # Try to convert merchant delay to int (e.g., "1" -> 1)
    try:
        m_val = float(m_delay)
    except ValueError:
        return False # Merchant delay is "manual"/"immediate" but rule is numeric/range
        
    # Parse rule range
    min_val, max_val = parse_range(r_delay)
    
    # Handle edge case where parse_range returns None (shouldn't happen with valid strings)
    if min_val is None: 
        return False
        
    return min_val < m_val <= max_val if '>' in r_delay else min_val <= m_val < max_val if '<' in r_delay else min_val <= m_val <= max_val

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    tx_context must contain:
    - card_scheme, is_credit, aci, intracountry (Transaction specific)
    - account_type, mcc, capture_delay (Merchant specific)
    - monthly_volume, monthly_fraud_rate (Monthly aggregates)
    """
    
    # 1. Card Scheme (Exact match required)
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List containment or Wildcard)
    if rule['account_type'] and tx_context['account_type'] not in rule['account_type']:
        return False
        
    # 3. Merchant Category Code (List containment or Wildcard)
    if rule['merchant_category_code'] and tx_context['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 4. Capture Delay (Complex logic or Wildcard)
    if rule['capture_delay'] and not check_capture_delay(tx_context['capture_delay'], rule['capture_delay']):
        return False
        
    # 5. Monthly Volume (Range match or Wildcard)
    if rule['monthly_volume']:
        min_vol, max_vol = parse_range(rule['monthly_volume'])
        if not (min_vol <= tx_context['monthly_volume'] <= max_vol):
            return False
            
    # 6. Monthly Fraud Level (Range match or Wildcard)
    if rule['monthly_fraud_level']:
        min_fraud, max_fraud = parse_range(rule['monthly_fraud_level'])
        # Note: tx_context['monthly_fraud_rate'] is 0.061 for 6.1%
        if not (min_fraud <= tx_context['monthly_fraud_rate'] <= max_fraud):
            return False
            
    # 7. Is Credit (Boolean match or Wildcard)
    if rule['is_credit'] is not None and rule['is_credit'] != tx_context['is_credit']:
        return False
        
    # 8. ACI (List containment or Wildcard)
    if rule['aci'] and tx_context['aci'] not in rule['aci']:
        return False
        
    # 9. Intracountry (Boolean/Float match or Wildcard)
    # In fees.json, intracountry is often 0.0 (False) or 1.0 (True) or null
    if rule['intracountry'] is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False
            
    return True

# ═══════════════════════════════════════════════════════════
# MAIN ANALYSIS
# ═══════════════════════════════════════════════════════════

# Define file paths
payments_path = '/output/chunk4/data/context/payments.csv'
merchant_data_path = '/output/chunk4/data/context/merchant_data.json'
fees_path = '/output/chunk4/data/context/fees.json'

# 1. Load Data
print("Loading data...")
df = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees = json.load(f)

# 2. Filter for Merchant and Time Period
merchant_name = 'Belles_cookbook_store'
print(f"Filtering for {merchant_name} in Feb 2023...")

# Filter Merchant
df_merchant = df[df['merchant'] == merchant_name].copy()

# Filter Date (Feb 2023)
# Feb 1 is Day 32, Feb 28 is Day 59
df_feb = df_merchant[
    (df_merchant['year'] == 2023) & 
    (df_merchant['day_of_year'] >= 32) & 
    (df_merchant['day_of_year'] <= 59)
].copy()

if df_feb.empty:
    print("No transactions found for this period.")
    exit()

# 3. Calculate Monthly Aggregates
total_volume = df_feb['eur_amount'].sum()
fraud_volume = df_feb[df_feb['has_fraudulent_dispute'] == True]['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

print(f"Total Volume: {total_volume:.2f}")
print(f"Fraud Rate: {fraud_rate:.4%}")

# 4. Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
if not merchant_info:
    print("Merchant metadata not found.")
    exit()

mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

print(f"Merchant Metadata - MCC: {mcc}, Account: {account_type}, Delay: {capture_delay}")

# 5. Identify Unique Transaction Profiles
# We need to check fees for every transaction, but many transactions are identical 
# regarding fee attributes. We group them to optimize.
df_feb['intracountry'] = df_feb['issuing_country'] == df_feb['acquirer_country']

# Attributes that vary per transaction and affect fees
tx_attributes = ['card_scheme', 'is_credit', 'aci', 'intracountry']
unique_profiles = df_feb[tx_attributes].drop_duplicates().to_dict('records')

print(f"Found {len(unique_profiles)} unique transaction profiles.")

# 6. Match Rules
applicable_fee_ids = set()

for profile in unique_profiles:
    # Construct full context for matching
    context = {
        # Transaction specific
        'card_scheme': profile['card_scheme'],
        'is_credit': profile['is_credit'],
        'aci': profile['aci'],
        'intracountry': profile['intracountry'],
        
        # Merchant specific
        'mcc': mcc,
        'account_type': account_type,
        'capture_delay': capture_delay,
        
        # Monthly aggregates
        'monthly_volume': total_volume,
        'monthly_fraud_rate': fraud_rate
    }
    
    # Check against all rules
    for rule in fees:
        if match_fee_rule(context, rule):
            applicable_fee_ids.add(rule['ID'])

# 7. Output Result
sorted_ids = sorted(list(applicable_fee_ids))
print("\nApplicable Fee IDs:")
print(sorted_ids)

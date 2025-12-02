# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1810
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 6940 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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

def parse_range_check(value, rule_value):
    """
    Checks if 'value' (float) fits into 'rule_value' (string range like '100k-1m', '>8.3%', etc.)
    Returns True if rule_value is None (wildcard) or value fits.
    """
    if rule_value is None:
        return True
    
    s = str(rule_value).strip().lower()
    
    # Handle percentages in rule
    is_pct = '%' in s
    s_clean = s.replace('%', '').replace(',', '')
    
    # Handle k/m suffixes
    multiplier = 1
    if 'k' in s_clean:
        multiplier = 1000
        s_clean = s_clean.replace('k', '')
    elif 'm' in s_clean:
        multiplier = 1000000
        s_clean = s_clean.replace('m', '')
        
    # Adjust multiplier for percentages (e.g. 8.3% -> 0.083)
    if is_pct:
        multiplier /= 100.0

    try:
        if '-' in s_clean:
            parts = s_clean.split('-')
            low = float(parts[0]) * multiplier
            high = float(parts[1]) * multiplier
            return low <= value <= high
        elif s.startswith('>'):
            limit = float(s_clean.replace('>', '')) * multiplier
            return value > limit
        elif s.startswith('<'):
            limit = float(s_clean.replace('<', '')) * multiplier
            return value < limit
        else:
            # Exact match
            return value == float(s_clean) * multiplier
    except:
        return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    """
    # 1. Card Scheme (Exact match required)
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List in rule, single in tx)
    # Rule: [] or None -> Wildcard
    if rule['account_type'] and tx_context['account_type'] not in rule['account_type']:
        return False
        
    # 3. Capture Delay (String in rule, String in tx)
    if rule['capture_delay']:
        rd = str(rule['capture_delay'])
        td = str(tx_context['capture_delay'])
        if rd == td:
            pass # Exact match (e.g., 'manual' == 'manual')
        elif rd == '>5':
            if td.isdigit() and int(td) > 5: pass
            else: return False
        elif rd == '<3':
            if td.isdigit() and int(td) < 3: pass
            else: return False
        elif '-' in rd:
            try:
                low, high = map(int, rd.split('-'))
                if td.isdigit() and low <= int(td) <= high: pass
                else: return False
            except:
                return False
        else:
            return False

    # 4. Merchant Category Code (List in rule, int in tx)
    if rule['merchant_category_code'] and tx_context['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 5. Is Credit (Bool in rule, Bool in tx)
    if rule['is_credit'] is not None and rule['is_credit'] != tx_context['is_credit']:
        return False
        
    # 6. ACI (List in rule, String in tx)
    if rule['aci'] and tx_context['aci'] not in rule['aci']:
        return False
        
    # 7. Intracountry (Bool in rule, Bool in tx)
    if rule['intracountry'] is not None:
        # fees.json uses 0.0/1.0 for False/True
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False

    # 8. Monthly Volume (Range in rule, Float in tx)
    if not parse_range_check(tx_context['monthly_volume'], rule['monthly_volume']):
        return False
        
    # 9. Monthly Fraud Level (Range in rule, Float in tx)
    if not parse_range_check(tx_context['monthly_fraud_rate'], rule['monthly_fraud_level']):
        return False
        
    return True

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk1/data/context/payments.csv'
fees_path = '/output/chunk1/data/context/fees.json'
merchant_path = '/output/chunk1/data/context/merchant_data.json'

df = pd.read_csv(payments_path)
with open(fees_path) as f:
    fees = json.load(f)
with open(merchant_path) as f:
    merchant_data = json.load(f)

# 2. Filter for Rafa_AI in December 2023
target_merchant = 'Rafa_AI'
# December 2023: Day of year 335 to 365
df_dec = df[
    (df['merchant'] == target_merchant) &
    (df['year'] == 2023) &
    (df['day_of_year'] >= 335) &
    (df['day_of_year'] <= 365)
].copy()

# 3. Get Merchant Static Data
m_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not m_info:
    print("Merchant not found")
    exit()

# 4. Calculate Monthly Statistics (Volume & Fraud Rate)
# Manual: "Fraud is defined as the ratio of fraudulent volume over total volume."
total_volume = df_dec['eur_amount'].sum()
fraud_volume = df_dec[df_dec['has_fraudulent_dispute']]['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

# 5. Determine Intracountry Status for Transactions
# Manual: "Local acquiring... issuer country is the same as the acquirer country."
df_dec['intracountry'] = df_dec['issuing_country'] == df_dec['acquirer_country']

# 6. Identify Unique Transaction Profiles
# We need to find Fee IDs for ALL transaction types that occurred in Dec.
# Relevant varying columns: card_scheme, is_credit, aci, intracountry
profile_cols = ['card_scheme', 'is_credit', 'aci', 'intracountry']
unique_profiles = df_dec[profile_cols].drop_duplicates()

applicable_ids = set()

# 7. Match Rules
for _, row in unique_profiles.iterrows():
    # Build context for this specific transaction profile
    ctx = {
        'card_scheme': row['card_scheme'],
        'account_type': m_info['account_type'],
        'capture_delay': m_info['capture_delay'],
        'mcc': m_info['merchant_category_code'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': row['intracountry'],
        'monthly_volume': total_volume,
        'monthly_fraud_rate': fraud_rate
    }
    
    # Check against all fee rules
    for rule in fees:
        if match_fee_rule(ctx, rule):
            applicable_ids.add(rule['ID'])

# 8. Output Result
sorted_ids = sorted(list(applicable_ids))
print(", ".join(map(str, sorted_ids)))

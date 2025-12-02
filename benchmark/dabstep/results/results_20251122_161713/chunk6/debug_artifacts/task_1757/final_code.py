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
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range(range_str):
    """Parses a string range like '100k-1m', '<3', '>5', '7.7%-8.3%' into (min, max)."""
    if range_str is None:
        return (-float('inf'), float('inf'))
    
    s = str(range_str).strip().lower()
    
    # Helper to parse values with k/m suffixes
    def parse_val(v):
        v = v.replace('%', '')
        mult = 1
        if 'k' in v:
            mult = 1000
            v = v.replace('k', '')
        elif 'm' in v:
            mult = 1000000
            v = v.replace('m', '')
        try:
            return float(v) * mult
        except:
            return 0.0

    is_percent = '%' in s

    if '-' in s:
        parts = s.split('-')
        low = parse_val(parts[0])
        high = parse_val(parts[1])
        if is_percent:
            low /= 100
            high /= 100
        return (low, high)
    elif s.startswith('<'):
        val = parse_val(s[1:])
        if is_percent: val /= 100
        return (-float('inf'), val)
    elif s.startswith('>'):
        val = parse_val(s[1:])
        if is_percent: val /= 100
        return (val, float('inf'))
    else:
        # Exact match treated as range [val, val]
        val = parse_val(s)
        if is_percent: val /= 100
        return (val, val)

def check_capture_delay(merchant_delay, rule_delay):
    """Checks if merchant capture delay matches rule."""
    if rule_delay is None:
        return True
    
    m_delay = str(merchant_delay).lower()
    r_delay = str(rule_delay).lower()
    
    # Direct string match (e.g., 'immediate', 'manual')
    if m_delay == r_delay:
        return True
    
    # Numeric comparison
    try:
        # Handle 'immediate' as 0 for numeric comparison if needed
        m_days = 0.0 if m_delay == 'immediate' else float(m_delay)
        
        if r_delay.startswith('<'):
            limit = float(r_delay[1:])
            return m_days < limit
        elif r_delay.startswith('>'):
            limit = float(r_delay[1:])
            return m_days > limit
        elif '-' in r_delay:
            parts = r_delay.split('-')
            return float(parts[0]) <= m_days <= float(parts[1])
        else:
            # Try exact numeric match
            return m_days == float(r_delay)
    except ValueError:
        return False

def match_fee_rule(tx_profile, merchant_profile, rule):
    """
    Checks if a fee rule applies to a transaction profile + merchant profile.
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != tx_profile['card_scheme']:
        return False
        
    # 2. Account Type (List contains)
    if rule['account_type'] and merchant_profile['account_type'] not in rule['account_type']:
        return False
        
    # 3. Merchant Category Code (List contains)
    if rule['merchant_category_code'] and merchant_profile['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 4. Capture Delay (Complex match)
    if not check_capture_delay(merchant_profile['capture_delay'], rule['capture_delay']):
        return False
        
    # 5. Monthly Volume (Range match)
    if rule['monthly_volume']:
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= merchant_profile['monthly_volume'] <= max_v):
            return False
            
    # 6. Monthly Fraud Level (Range match)
    if rule['monthly_fraud_level']:
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        if not (min_f <= merchant_profile['monthly_fraud_rate'] <= max_f):
            return False
            
    # 7. Is Credit (Bool match, None=Wildcard)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_profile['is_credit']:
            return False
            
    # 8. ACI (List contains, Empty=Wildcard)
    if rule['aci'] and tx_profile['aci'] not in rule['aci']:
        return False
        
    # 9. Intracountry (Bool match, None=Wildcard)
    if rule['intracountry'] is not None:
        # Convert rule value to bool (0.0 -> False, 1.0 -> True)
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_profile['intracountry']:
            return False
            
    return True

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
merchant_path = '/output/chunk6/data/context/merchant_data.json'
fees_path = '/output/chunk6/data/context/fees.json'

print("Loading data...")
df = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Filter for Belles_cookbook_store, July 2023 (Days 182-212)
target_merchant = 'Belles_cookbook_store'
target_year = 2023
start_day = 182
end_day = 212

print(f"Filtering for {target_merchant} in July {target_year}...")
df_filtered = df[
    (df['merchant'] == target_merchant) &
    (df['year'] == target_year) &
    (df['day_of_year'] >= start_day) &
    (df['day_of_year'] <= end_day)
].copy()

if len(df_filtered) == 0:
    print("No transactions found for this merchant in the specified period.")
    exit()

# 3. Calculate Monthly Stats
# Volume
total_volume = df_filtered['eur_amount'].sum()

# Fraud Rate (Volume based as per manual)
fraud_volume = df_filtered[df_filtered['has_fraudulent_dispute']]['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

print(f"  Total Volume: €{total_volume:,.2f}")
print(f"  Fraud Volume: €{fraud_volume:,.2f}")
print(f"  Fraud Rate: {fraud_rate:.4%}")

# 4. Get Merchant Static Attributes
m_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not m_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

merchant_profile = {
    'account_type': m_info['account_type'],
    'mcc': m_info['merchant_category_code'],
    'capture_delay': m_info['capture_delay'],
    'monthly_volume': total_volume,
    'monthly_fraud_rate': fraud_rate
}
print(f"  Merchant Profile: {merchant_profile}")

# 5. Identify Unique Transaction Profiles
# Calculate intracountry for each transaction
df_filtered['intracountry'] = df_filtered['issuing_country'] == df_filtered['acquirer_country']

# Get unique combinations of attributes that affect fees
unique_profiles = df_filtered[['card_scheme', 'is_credit', 'aci', 'intracountry']].drop_duplicates()

print(f"  Unique Transaction Profiles found: {len(unique_profiles)}")

# 6. Find Applicable Fees
applicable_fee_ids = set()

for _, row in unique_profiles.iterrows():
    tx_profile = {
        'card_scheme': row['card_scheme'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': row['intracountry']
    }
    
    for rule in fees_data:
        if match_fee_rule(tx_profile, merchant_profile, rule):
            applicable_fee_ids.add(rule['ID'])

# 7. Output Result
sorted_ids = sorted(list(applicable_fee_ids))
print("\n" + "="*30)
print("APPLICABLE FEE IDs")
print("="*30)
print(sorted_ids)
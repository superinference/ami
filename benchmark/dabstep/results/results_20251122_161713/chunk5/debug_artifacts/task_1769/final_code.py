import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

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
            try:
                return float(v.replace('%', '')) / 100
            except ValueError:
                return 0.0
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

def parse_range_check(value, range_str):
    """
    Checks if a numeric value falls within a range string.
    Range strings can be: "100-200", ">100", "<200", "10%", "10%-20%".
    Handles 'k' (thousands) and 'm' (millions) suffixes.
    """
    if range_str is None:
        return True
    
    # Normalize string
    s = str(range_str).lower().strip()
    
    # Handle percentages in range string (convert to ratio if value is ratio)
    is_percentage = '%' in s
    s = s.replace('%', '')
    
    # Handle k/m suffixes
    def parse_num(n_str):
        n_str = n_str.strip()
        mult = 1
        if n_str.endswith('k'):
            mult = 1000
            n_str = n_str[:-1]
        elif n_str.endswith('m'):
            mult = 1000000
            n_str = n_str[:-1]
        try:
            return float(n_str) * mult
        except ValueError:
            return 0.0

    # Adjust scale based on percentage presence
    scale = 0.01 if is_percentage else 1.0

    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            low = parse_num(parts[0]) * scale
            high = parse_num(parts[1]) * scale
            # Handle precision issues with slight tolerance if needed, but direct comparison usually fine
            return low <= value <= high
    elif s.startswith('>'):
        limit = parse_num(s[1:]) * scale
        return value > limit
    elif s.startswith('<'):
        limit = parse_num(s[1:]) * scale
        return value < limit
    elif s.startswith('≥'):
        limit = parse_num(s[1:]) * scale
        return value >= limit
    elif s.startswith('≤'):
        limit = parse_num(s[1:]) * scale
        return value <= limit
    
    # Exact match fallback
    try:
        return value == (parse_num(s) * scale)
    except:
        return False

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    """
    
    # 1. Card Scheme (Exact Match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List Match - Wildcard if empty/null)
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List Match - Wildcard if empty/null)
    if rule.get('merchant_category_code'):
        if tx_ctx['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (String Match - Wildcard if null)
    if rule.get('capture_delay') is not None:
        # Exact match for categorical values (e.g. "manual")
        if rule['capture_delay'] != tx_ctx['capture_delay']:
            return False

    # 5. Is Credit (Bool Match - Wildcard if null)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 6. ACI (List Match - Wildcard if empty/null)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 7. Intracountry (Bool Match - Wildcard if null)
    if rule.get('intracountry') is not None:
        # Handle 0.0/1.0 or boolean types in JSON
        rule_intra = rule['intracountry']
        if isinstance(rule_intra, (int, float)):
            rule_intra = bool(rule_intra)
        
        if rule_intra != tx_ctx['intracountry']:
            return False
            
    # 8. Monthly Volume (Range Match - Wildcard if null)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 9. Monthly Fraud Level (Range Match - Wildcard if null)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_ctx['monthly_fraud_ratio'], rule['monthly_fraud_level']):
            return False
            
    return True

# ---------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------

# File paths
payments_path = '/output/chunk5/data/context/payments.csv'
merchant_path = '/output/chunk5/data/context/merchant_data.json'
fees_path = '/output/chunk5/data/context/fees.json'

# 1. Load Data
df_payments = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Filter for Merchant and Time Period (July 2023)
target_merchant = 'Crossfit_Hanna'
# July 2023 -> Year 2023, Day of Year 182 to 212
df_filtered = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == 2023) &
    (df_payments['day_of_year'] >= 182) &
    (df_payments['day_of_year'] <= 212)
].copy()

if df_filtered.empty:
    print("No transactions found for this merchant in July 2023.")
    exit()

# 3. Get Merchant Static Data
merchant_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

# 4. Calculate Merchant Aggregates (Volume & Fraud)
# Volume: Sum of eur_amount
monthly_volume = df_filtered['eur_amount'].sum()

# Fraud: Ratio of (Volume of Fraudulent Txs) / (Total Volume)
fraud_volume = df_filtered[df_filtered['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_ratio = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 5. Identify Unique Transaction Profiles
# Relevant transaction-level columns: card_scheme, is_credit, aci, issuing_country, acquirer_country
# Derived: intracountry
df_filtered['intracountry'] = df_filtered['issuing_country'] == df_filtered['acquirer_country']

# Get unique combinations
unique_profiles = df_filtered[['card_scheme', 'is_credit', 'aci', 'intracountry']].drop_duplicates()

# 6. Match Rules
applicable_fee_ids = set()

for _, profile in unique_profiles.iterrows():
    # Construct context for this profile
    tx_ctx = {
        'card_scheme': profile['card_scheme'],
        'is_credit': profile['is_credit'],
        'aci': profile['aci'],
        'intracountry': profile['intracountry'],
        'merchant_category_code': mcc,
        'account_type': account_type,
        'capture_delay': capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_ratio': monthly_fraud_ratio
    }
    
    # Check against all rules
    for rule in fees_data:
        if match_fee_rule(tx_ctx, rule):
            applicable_fee_ids.add(rule['ID'])

# 7. Output Results
sorted_ids = sorted(list(applicable_fee_ids))
print(", ".join(map(str, sorted_ids)))
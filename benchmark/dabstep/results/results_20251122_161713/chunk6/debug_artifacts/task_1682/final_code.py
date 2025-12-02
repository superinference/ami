import pandas as pd
import json
import numpy as np

# --- HELPER FUNCTIONS ---
def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except ValueError:
                return 0.0
        if 'k' in v:
            try:
                return float(v.replace('k', '')) * 1000
            except ValueError:
                return 0.0
        if 'm' in v:
            try:
                return float(v.replace('m', '')) * 1000000
            except ValueError:
                return 0.0
        try:
            return float(v)
        except ValueError:
            return v # Return original string if not a number
    return value

def check_range(value, rule_value):
    """Check if a numeric value fits within a rule range string (e.g. '>5', '100k-1m')."""
    if rule_value is None:
        return True
    
    # Handle exact string matches for non-numeric rules (like capture_delay 'immediate')
    str_rule = str(rule_value).strip().lower()
    
    # Special case for capture_delay 'immediate', 'manual'
    if str_rule in ['immediate', 'manual']:
        return str(value).lower() == str_rule

    # If value is string (e.g. 'manual') and rule is numeric/range, it won't match unless exact string
    if isinstance(value, str) and not value.replace('.','',1).isdigit():
         return value.lower() == str_rule

    try:
        val = float(value)
    except (ValueError, TypeError):
        return str(value).lower() == str_rule

    # Clean rule string for numeric parsing
    s_rule = str_rule.replace(',', '').replace('_', '').replace('%', '')
    
    # Parse bounds
    if '-' in s_rule:
        parts = s_rule.split('-')
        try:
            low = coerce_to_float(parts[0].strip())
            high = coerce_to_float(parts[1].strip())
            return low <= val <= high
        except:
            return False
    
    if s_rule.startswith('>'):
        try:
            limit = coerce_to_float(s_rule[1:])
            return val > limit
        except:
            return False
    
    if s_rule.startswith('<'):
        try:
            limit = coerce_to_float(s_rule[1:])
            return val < limit
        except:
            return False
            
    # Exact match numeric
    try:
        return val == coerce_to_float(s_rule)
    except:
        return False

def match_fee_rule(tx_ctx, rule):
    """
    Check if a transaction context matches a fee rule.
    tx_ctx: dict containing transaction and merchant details
    rule: dict containing fee rule details
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx.get('card_scheme'):
        return False
        
    # 2. Account Type (List match - Wildcard if empty)
    if rule.get('account_type'):
        if tx_ctx.get('account_type') not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match - Wildcard if empty)
    if rule.get('merchant_category_code'):
        if tx_ctx.get('merchant_category_code') not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Boolean match - Wildcard if None)
    if rule.get('is_credit') is not None:
        if bool(rule['is_credit']) != bool(tx_ctx.get('is_credit')):
            return False
            
    # 5. ACI (List match - Wildcard if empty)
    if rule.get('aci'):
        if tx_ctx.get('aci') not in rule['aci']:
            return False
            
    # 6. Intracountry (Boolean match - Wildcard if None)
    # fees.json uses 0.0 (False/International) / 1.0 (True/Domestic) or null
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        tx_intra = bool(tx_ctx.get('intracountry'))
        if rule_intra != tx_intra:
            return False
            
    # 7. Capture Delay (Range/String match - Wildcard if None)
    if rule.get('capture_delay'):
        if not check_range(tx_ctx.get('capture_delay'), rule['capture_delay']):
            return False
            
    # 8. Monthly Volume (Range match - Wildcard if None)
    if rule.get('monthly_volume'):
        if not check_range(tx_ctx.get('monthly_volume'), rule['monthly_volume']):
            return False
            
    # 9. Monthly Fraud Level (Range match - Wildcard if None)
    if rule.get('monthly_fraud_level'):
        if not check_range(tx_ctx.get('monthly_fraud_level'), rule['monthly_fraud_level']):
            return False
            
    return True

# --- MAIN SCRIPT ---

# Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'
fees_path = '/output/chunk6/data/context/fees.json'

df_payments = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees = json.load(f)

target_merchant = 'Belles_cookbook_store'
target_day = 12
target_year = 2023

# 1. Get Merchant Static Attributes
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    print("Merchant not found")
    exit()

mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

# 2. Calculate Monthly Stats (January 2023)
# Day 12 is in January.
jan_txs = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] <= 31) # January
]

monthly_volume = jan_txs['eur_amount'].sum()
# Fraud level is ratio of fraudulent volume over total volume
fraud_txs = jan_txs[jan_txs['has_fraudulent_dispute'] == True]
fraud_volume = fraud_txs['eur_amount'].sum()

if monthly_volume > 0:
    monthly_fraud_level = fraud_volume / monthly_volume
else:
    monthly_fraud_level = 0.0

# 3. Get Target Transactions (Day 12)
day_txs = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] == target_day)
].copy()

if day_txs.empty:
    print("No transactions found for this day")
    exit()

# Calculate intracountry for these transactions
day_txs['intracountry'] = day_txs['issuing_country'] == day_txs['acquirer_country']

# 4. Find Applicable Fees
applicable_fee_ids = set()

# We iterate through unique transaction profiles to save time
unique_tx_profiles = day_txs[['card_scheme', 'is_credit', 'aci', 'intracountry']].drop_duplicates()

for _, tx in unique_tx_profiles.iterrows():
    # Construct transaction context
    tx_ctx = {
        'card_scheme': tx['card_scheme'],
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': tx['intracountry'],
        'merchant_category_code': mcc,
        'account_type': account_type,
        'capture_delay': capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level
    }
    
    # Check against all rules
    for rule in fees:
        if match_fee_rule(tx_ctx, rule):
            applicable_fee_ids.add(rule['ID'])

# 5. Output Result
sorted_ids = sorted(list(applicable_fee_ids))
print(", ".join(map(str, sorted_ids)))
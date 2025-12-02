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
            return float(v.replace('%', '')) / 100
        # Range handling (e.g., "50-60") - return mean if forced, but usually handled by range parsers
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

def parse_volume_string(vol_str):
    """Parses volume strings like '100k', '1m' into floats."""
    if not isinstance(vol_str, str):
        return float(vol_str)
    s = vol_str.lower().strip()
    multiplier = 1.0
    if s.endswith('k'):
        multiplier = 1_000.0
        s = s[:-1]
    elif s.endswith('m'):
        multiplier = 1_000_000.0
        s = s[:-1]
    try:
        return float(s) * multiplier
    except ValueError:
        return 0.0

def check_range(value, range_str, is_percentage=False):
    """
    Checks if a value falls within a range string.
    range_str examples: '100k-1m', '>5', '<3', '7.7%-8.3%', 'manual'
    """
    if range_str is None:
        return True
    
    # Handle exact string matches for non-numeric rules (like 'manual')
    if isinstance(value, str) and not is_percentage:
        return value.lower() == range_str.lower()

    # Parse value if it's a string (e.g. from data) but we expect numeric comparison
    if isinstance(value, str):
        try:
            value = float(value)
        except:
            pass

    s = str(range_str).strip()
    
    # Handle Percentage Ranges
    if '%' in s:
        # Convert value to float (0.083) if it isn't already
        # Convert range parts to floats (8.3% -> 0.083)
        if '-' in s:
            parts = s.split('-')
            low = coerce_to_float(parts[0])
            high = coerce_to_float(parts[1])
            return low <= value <= high
        elif s.startswith('>'):
            limit = coerce_to_float(s[1:])
            return value > limit
        elif s.startswith('<'):
            limit = coerce_to_float(s[1:])
            return value < limit
        else:
            # Exact match? Unlikely for floats, but possible
            return abs(value - coerce_to_float(s)) < 1e-9

    # Handle Volume/Numeric Ranges
    if '-' in s:
        parts = s.split('-')
        low = parse_volume_string(parts[0])
        high = parse_volume_string(parts[1])
        return low <= value <= high
    elif s.startswith('>'):
        limit = parse_volume_string(s[1:])
        return value > limit
    elif s.startswith('<'):
        limit = parse_volume_string(s[1:])
        return value < limit
    
    # Exact match for numeric strings
    try:
        target = parse_volume_string(s)
        return value == target
    except:
        return False

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    tx_ctx: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
    
    # 2. Account Type (List match)
    # If rule['account_type'] is empty/null, it applies to all. 
    # If not empty, merchant's account_type must be in list.
    if rule['account_type'] and tx_ctx['account_type'] not in rule['account_type']:
        return False
        
    # 3. Merchant Category Code (List match)
    if rule['merchant_category_code'] and tx_ctx['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 4. Is Credit (Bool match)
    # If rule['is_credit'] is None, applies to both.
    if rule['is_credit'] is not None and rule['is_credit'] != tx_ctx['is_credit']:
        return False
        
    # 5. ACI (List match)
    if rule['aci'] and tx_ctx['aci'] not in rule['aci']:
        return False
        
    # 6. Intracountry (Bool match)
    if rule['intracountry'] is not None and rule['intracountry'] != tx_ctx['intracountry']:
        return False
        
    # 7. Capture Delay (String/Range match)
    # Merchant attribute vs Rule requirement
    if not check_range(tx_ctx['capture_delay'], rule['capture_delay']):
        return False
        
    # 8. Monthly Volume (Range match)
    if not check_range(tx_ctx['monthly_volume'], rule['monthly_volume']):
        return False
        
    # 9. Monthly Fraud Level (Range match)
    # Note: Manual says fraud level is ratio of fraud_vol / total_vol
    if not check_range(tx_ctx['monthly_fraud_rate'], rule['monthly_fraud_level'], is_percentage=True):
        return False
        
    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    # fee = fixed_amount + rate * transaction_value / 10000
    # rate is an integer, amount is in euros
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000.0)

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
merchant_path = '/output/chunk6/data/context/merchant_data.json'
fees_path = '/output/chunk6/data/context/fees.json'

df_payments = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_rules = json.load(f)

# 2. Filter for Crossfit_Hanna in 2023
target_merchant = 'Crossfit_Hanna'
df = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == 2023)
].copy()

# 3. Get Merchant Attributes
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

m_account_type = merchant_info['account_type']
m_mcc = merchant_info['merchant_category_code']
m_capture_delay = merchant_info['capture_delay']

# 4. Calculate Monthly Stats (Volume and Fraud Rate)
# Create a month column. Since year is 2023, we can use day_of_year to determine month.
# Using pandas to_datetime is robust.
df['date'] = pd.to_datetime(df['year'] * 1000 + df['day_of_year'], format='%Y%j')
df['month'] = df['date'].dt.month

# Group by month to calculate stats
monthly_stats = {}
for month, group in df.groupby('month'):
    total_vol = group['eur_amount'].sum()
    # Fraud rate is defined as Fraud Volume / Total Volume (per manual)
    fraud_vol = group[group['has_fraudulent_dispute']]['eur_amount'].sum()
    fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    monthly_stats[month] = {
        'volume': total_vol,
        'fraud_rate': fraud_rate
    }

# 5. Calculate Fees per Transaction
total_fees = 0.0
match_count = 0
no_match_count = 0

# Pre-calculate intracountry for efficiency
df['intracountry'] = df['issuing_country'] == df['acquirer_country']

for idx, row in df.iterrows():
    # Build Transaction Context
    month = row['month']
    stats = monthly_stats.get(month, {'volume': 0, 'fraud_rate': 0})
    
    tx_ctx = {
        'card_scheme': row['card_scheme'],
        'account_type': m_account_type,
        'mcc': m_mcc,
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': row['intracountry'],
        'capture_delay': m_capture_delay,
        'monthly_volume': stats['volume'],
        'monthly_fraud_rate': stats['fraud_rate']
    }
    
    # Find matching rule
    matched_rule = None
    for rule in fees_rules:
        if match_fee_rule(tx_ctx, rule):
            matched_rule = rule
            break # Stop at first match
            
    if matched_rule:
        fee = calculate_fee(row['eur_amount'], matched_rule)
        total_fees += fee
        match_count += 1
    else:
        # If no rule matches, we assume 0 fee or log it. 
        # In this context, we should probably have matches for all.
        no_match_count += 1
        # print(f"No match for tx {row['psp_reference']}") # Debugging

# 6. Output Result
print(f"Total transactions processed: {len(df)}")
print(f"Transactions with matching fee rule: {match_count}")
print(f"Transactions without matching fee rule: {no_match_count}")
print(f"Total fees paid by {target_merchant} in 2023: {total_fees:.2f}")

# Final answer format for the system
print(f"{total_fees:.2f}")
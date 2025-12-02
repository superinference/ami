import pandas as pd
import json
import numpy as np

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
        # Range handling (e.g., "50-60") - return mean for single value conversion, 
        # but for range parsing we handle separately. Here we just try basic float.
        try:
            return float(v)
        except ValueError:
            return None
    return None

def parse_range(range_str):
    """Parses a range string like '100k-1m', '>5', '<3' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    # Handle k/m suffixes
    def parse_val(x):
        x = x.strip()
        mult = 1
        if x.endswith('k'):
            mult = 1000
            x = x[:-1]
        elif x.endswith('m'):
            mult = 1000000
            x = x[:-1]
        elif '%' in x:
            mult = 0.01
            x = x.replace('%', '')
        return float(x) * mult

    try:
        if '-' in s:
            parts = s.split('-')
            return parse_val(parts[0]), parse_val(parts[1])
        elif s.startswith('>'):
            return parse_val(s[1:]), float('inf')
        elif s.startswith('<'):
            return 0.0, parse_val(s[1:])
        else:
            # Exact match treated as range [val, val]
            val = parse_val(s)
            return val, val
    except:
        return None, None

def check_range_match(value, rule_range_str):
    """Checks if a numeric value falls within a rule's range string."""
    if rule_range_str is None:
        return True
    if value is None:
        return False
        
    # Handle categorical ranges like "immediate", "manual" for capture_delay
    if isinstance(rule_range_str, str) and not any(c.isdigit() for c in rule_range_str):
        return value == rule_range_str

    min_val, max_val = parse_range(rule_range_str)
    if min_val is None: # Parsing failed or not a range
        return str(value) == str(rule_range_str)
        
    return min_val <= value <= max_val

def match_fee_rule(tx, rule):
    """
    Determines if a transaction matches a fee rule.
    tx: dict containing transaction details + merchant details + monthly stats
    rule: dict containing fee rule definition
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx.get('card_scheme'):
        return False

    # 2. Account Type (List match)
    if rule.get('account_type'):
        if tx.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx.get('merchant_category_code') not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx.get('is_credit'):
            return False

    # 5. ACI (List match)
    if rule.get('aci'):
        if tx.get('aci') not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        # tx['intracountry'] should be boolean
        if rule['intracountry'] != tx.get('intracountry'):
            return False

    # 7. Capture Delay (Range/Value match)
    if rule.get('capture_delay'):
        # capture_delay in merchant data is string (e.g., "manual", "1")
        # rule can be "manual" or range "3-5"
        if not check_range_match(tx.get('capture_delay'), rule['capture_delay']):
            return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not check_range_match(tx.get('monthly_volume'), rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not check_range_match(tx.get('monthly_fraud_level'), rule['monthly_fraud_level']):
            return False

    return True

# ==========================================
# MAIN SCRIPT
# ==========================================

# Define file paths
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'

# 1. Load Data
print("Loading data...")
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Setup Target Variables
target_merchant = 'Golfclub_Baron_Friso'
target_fee_id = 595
target_year = 2023
new_rate = 1

# 3. Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
    exit()

print(f"Merchant Info: {merchant_info}")

# 4. Get Target Fee Rule
fee_rule = next((f for f in fees_data if f['ID'] == target_fee_id), None)
if not fee_rule:
    print(f"Error: Fee ID {target_fee_id} not found in fees.json")
    exit()

print(f"Fee Rule {target_fee_id}: {fee_rule}")
old_rate = fee_rule['rate']

# 5. Filter Transactions
# Filter for merchant and year
df_tx = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
].copy()

print(f"Found {len(df_tx)} transactions for {target_merchant} in {target_year}")

# 6. Calculate Derived Columns
# Intracountry: True if issuing_country == acquirer_country
df_tx['intracountry'] = df_tx['issuing_country'] == df_tx['acquirer_country']

# 7. Calculate Monthly Stats (Volume and Fraud)
# We need these to check against the fee rule's monthly_volume and monthly_fraud_level
# Note: Volume is sum of eur_amount. Fraud is ratio of fraudulent volume / total volume (as per manual).
# Manual says: "Fraud is defined as the ratio of fraudulent volume over total volume." (Section 7)
# Wait, Section 5 says: "monthly_fraud_level... ratio between monthly total volume and monthly volume notified as fraud."
# Let's calculate fraud ratio based on VOLUME, not count, to be safe, though often count is used if volume not specified.
# Manual Section 7 explicitly says "ratio of fraudulent volume over total volume".

# Add month column
# Assuming 'day_of_year' is available. We can approximate month or just group by it if we had it.
# We don't have explicit month column, but we have 'day_of_year'.
# We can map day_of_year to month.
def get_month(doy):
    # Simple approximation for 2023 (non-leap)
    days_in_months = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    cumulative = 0
    for i, days in enumerate(days_in_months):
        cumulative += days
        if doy <= cumulative:
            return i + 1
    return 12

df_tx['month'] = df_tx['day_of_year'].apply(get_month)

# Calculate stats per month
monthly_stats = {}
for month in df_tx['month'].unique():
    month_txs = df_tx[df_tx['month'] == month]
    total_vol = month_txs['eur_amount'].sum()
    fraud_vol = month_txs[month_txs['has_fraudulent_dispute']]['eur_amount'].sum()
    
    fraud_ratio = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    monthly_stats[month] = {
        'monthly_volume': total_vol,
        'monthly_fraud_level': fraud_ratio
    }

# 8. Identify Matching Transactions and Calculate Delta
matching_amount_sum = 0.0
matching_count = 0

for idx, row in df_tx.iterrows():
    # Build transaction context dictionary
    tx_ctx = row.to_dict()
    
    # Add merchant static data
    tx_ctx['account_type'] = merchant_info['account_type']
    tx_ctx['merchant_category_code'] = merchant_info['merchant_category_code']
    tx_ctx['capture_delay'] = merchant_info['capture_delay']
    
    # Add monthly dynamic data
    m_stats = monthly_stats.get(row['month'], {'monthly_volume': 0, 'monthly_fraud_level': 0})
    tx_ctx['monthly_volume'] = m_stats['monthly_volume']
    tx_ctx['monthly_fraud_level'] = m_stats['monthly_fraud_level']
    
    # Check match
    if match_fee_rule(tx_ctx, fee_rule):
        matching_amount_sum += row['eur_amount']
        matching_count += 1

print(f"Transactions matching Fee ID {target_fee_id}: {matching_count}")
print(f"Total Volume matching Fee ID {target_fee_id}: {matching_amount_sum}")

# 9. Calculate Delta
# Formula: Fee = Fixed + (Rate * Amount / 10000)
# Delta = New_Fee - Old_Fee
# Delta = (Fixed + 1 * Amount / 10000) - (Fixed + Old_Rate * Amount / 10000)
# Delta = (1 - Old_Rate) * Amount / 10000

delta = (new_rate - old_rate) * matching_amount_sum / 10000

# 10. Output Result
# Use high precision
print(f"{delta:.14f}")
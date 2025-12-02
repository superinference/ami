# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2494
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9323 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if pd.isna(value) or value is None:
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

def parse_range(range_str):
    """Parses a range string like '100k-1m', '<3', '>5', '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    # Handle percentages
    is_percent = '%' in s
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
            val = float(v) * mult
            return val / 100 if is_percent else val
        except:
            return 0

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif s.startswith('>'):
        return parse_val(s[1:]), float('inf')
    elif s.startswith('<'):
        return float('-inf'), parse_val(s[1:])
    else:
        # Exact match treated as range [val, val]
        val = parse_val(s)
        return val, val

def check_range_match(value, range_str):
    """Checks if a value falls within a range string."""
    if range_str is None:
        return True
    min_v, max_v = parse_range(range_str)
    if min_v is None: 
        return True
    return min_v <= value <= max_v

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a transaction context matches a fee rule.
    tx_ctx: dict containing transaction and merchant details
    rule: dict representing a fee rule
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx.get('card_scheme'):
        return False

    # 2. Account Type (Rule has list of allowed types, or wildcard)
    if rule.get('account_type'):
        if tx_ctx.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (Rule has list of allowed MCCs, or wildcard)
    if rule.get('merchant_category_code'):
        if tx_ctx.get('mcc') not in rule['merchant_category_code']:
            return False

    # 4. Capture Delay (Rule has range/value, or wildcard)
    if rule.get('capture_delay'):
        # Exact string match for non-numeric delays like 'immediate', 'manual'
        if rule['capture_delay'] in ['immediate', 'manual']:
            if tx_ctx.get('capture_delay') != rule['capture_delay']:
                return False
        else:
            # Numeric range check for days
            try:
                delay_val = float(tx_ctx.get('capture_delay', 0))
                if not check_range_match(delay_val, rule['capture_delay']):
                    return False
            except:
                # If tx delay is 'manual' but rule expects number, no match
                return False

    # 5. Is Credit
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx.get('is_credit'):
            return False

    # 6. ACI (Rule has list of allowed ACIs, or wildcard)
    if rule.get('aci'):
        if tx_ctx.get('aci') not in rule['aci']:
            return False

    # 7. Intracountry
    if rule.get('intracountry') is not None:
        # Rule expects boolean (True/False) or 1.0/0.0
        rule_intra = bool(rule['intracountry'])
        tx_intra = tx_ctx.get('intracountry', False)
        if rule_intra != tx_intra:
            return False

    # 8. Monthly Volume (Range check)
    if rule.get('monthly_volume'):
        if not check_range_match(tx_ctx.get('monthly_volume', 0), rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range check)
    if rule.get('monthly_fraud_level'):
        if not check_range_match(tx_ctx.get('monthly_fraud_level', 0), rule['monthly_fraud_level']):
            return False

    return True

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk3/data/context/payments.csv'
merchant_path = '/output/chunk3/data/context/merchant_data.json'
fees_path = '/output/chunk3/data/context/fees.json'

df_payments = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Setup Target Merchant and Fee
target_merchant = "Golfclub_Baron_Friso"
target_year = 2023
target_fee_id = 64
new_rate = 99

# Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

# Get Fee Rule
fee_rule = next((f for f in fees_data if f['ID'] == target_fee_id), None)
if not fee_rule:
    raise ValueError(f"Fee ID {target_fee_id} not found in fees.json")

original_rate = fee_rule['rate']
print(f"Analyzing Fee ID: {target_fee_id}")
print(f"Original Rate: {original_rate}")
print(f"New Rate: {new_rate}")
print(f"Merchant Info: {merchant_info}")

# 3. Filter Transactions
# Filter for merchant and year
df_merchant = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
].copy()

# 4. Calculate Monthly Stats (Volume & Fraud)
# Manual says: "Monthly volumes and rates are computed always in natural months"
# We need these to check if the fee rule applies to specific transactions based on that month's stats.

# Create a month column (derived from day_of_year, approximation or if month exists? 
# payments.csv has 'day_of_year'. We can approximate month or just group by it if needed, 
# but usually 'month' is not in schema. 
# Let's assume standard calendar:
# Jan: 1-31, Feb: 32-59, etc.
# However, simpler approach: The dataset is synthetic. 
# Let's check if 'month' column exists. Schema says: year, hour_of_day, minute_of_hour, day_of_year.
# We will map day_of_year to month.
def get_month(day_of_year):
    # 2023 is not a leap year
    days_in_months = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    cumulative = 0
    for i, days in enumerate(days_in_months):
        cumulative += days
        if day_of_year <= cumulative:
            return i + 1
    return 12

df_merchant['month'] = df_merchant['day_of_year'].apply(get_month)

# Calculate stats per month
monthly_stats = {}
for month in range(1, 13):
    month_txs = df_merchant[df_merchant['month'] == month]
    total_vol = month_txs['eur_amount'].sum()
    
    # Fraud volume: sum of amounts where has_fraudulent_dispute is True
    fraud_vol = month_txs[month_txs['has_fraudulent_dispute']]['eur_amount'].sum()
    
    fraud_level = (fraud_vol / total_vol) if total_vol > 0 else 0.0
    
    monthly_stats[month] = {
        'volume': total_vol,
        'fraud_level': fraud_level
    }

# 5. Identify Matching Transactions
matching_amount_sum = 0.0
matching_count = 0

for idx, row in df_merchant.iterrows():
    # Build Transaction Context
    tx_ctx = {
        'card_scheme': row['card_scheme'],
        'account_type': merchant_info['account_type'],
        'mcc': merchant_info['merchant_category_code'],
        'capture_delay': merchant_info['capture_delay'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        # Intracountry: Issuer Country == Acquirer Country
        'intracountry': row['issuing_country'] == row['acquirer_country'],
        # Monthly stats for the transaction's month
        'monthly_volume': monthly_stats[row['month']]['volume'],
        'monthly_fraud_level': monthly_stats[row['month']]['fraud_level']
    }
    
    # Check if this transaction matches Fee ID 64
    if match_fee_rule(tx_ctx, fee_rule):
        matching_amount_sum += row['eur_amount']
        matching_count += 1

print(f"Matching Transactions: {matching_count}")
print(f"Total Volume of Matching Transactions: {matching_amount_sum:.2f}")

# 6. Calculate Delta
# Fee formula: fee = fixed_amount + rate * transaction_value / 10000
# Delta = (New_Fee - Old_Fee)
# Delta = (fixed + new_rate * val / 10000) - (fixed + old_rate * val / 10000)
# Delta = (new_rate - old_rate) * val / 10000

rate_diff = new_rate - original_rate
total_delta = (rate_diff * matching_amount_sum) / 10000

# 7. Output Result
# High precision required for financial deltas
print(f"{total_delta:.14f}")

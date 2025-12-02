# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2568
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 10651 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import json
import pandas as pd
import numpy as np

# ==========================================
# HELPER FUNCTIONS
# ==========================================

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

def parse_range(range_str):
    """Parses a range string like '100k-1m' or '>5' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().replace(',', '').replace('%', '').strip()
    multiplier = 1
    if 'k' in s:
        multiplier = 1000
        s = s.replace('k', '')
    elif 'm' in s:
        multiplier = 1000000
        s = s.replace('m', '')
        
    if '>' in s:
        return float(s.replace('>', '')) * multiplier, float('inf')
    if '<' in s:
        return float('-inf'), float(s.replace('<', '')) * multiplier
    if '-' in s:
        parts = s.split('-')
        try:
            return float(parts[0]) * multiplier, float(parts[1]) * multiplier
        except:
            return None, None
    return None, None

def check_range(value, range_str):
    """Checks if a value falls within a string range."""
    if range_str is None:
        return True
    min_val, max_val = parse_range(range_str)
    if min_val is None: # Could not parse, assume True or handle specific cases
        return True
    return min_val <= value <= max_val

def match_fee_rule(tx_data, rule):
    """
    Determines if a transaction matches a fee rule.
    tx_data must contain: card_scheme, is_credit, aci, merchant_category_code, 
                          account_type, monthly_volume, monthly_fraud_level, capture_delay, etc.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_data.get('card_scheme'):
        return False

    # 2. Account Type (List match)
    # If rule has account types, merchant's type must be in list
    if rule.get('account_type'):
        if tx_data.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_data.get('mcc') not in rule['merchant_category_code']:
            return False

    # 4. ACI (List match)
    if rule.get('aci'):
        if tx_data.get('aci') not in rule['aci']:
            return False

    # 5. Is Credit (Boolean match)
    # If rule specifies is_credit (True/False), it must match. None/Null in rule means both.
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_data.get('is_credit'):
            return False

    # 6. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not check_range(tx_data.get('monthly_volume', 0), rule['monthly_volume']):
            return False

    # 7. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        # Fraud level in rule is usually %, e.g. "0-1%". tx_data should have ratio 0.0-1.0 or %
        # Helper check_range handles parsing, but we need to ensure units match.
        # Assuming tx_data['monthly_fraud_level'] is a float ratio (e.g., 0.01 for 1%)
        # and rule is string "1%". parse_range handles %.
        if not check_range(tx_data.get('monthly_fraud_level', 0), rule['monthly_fraud_level']):
            return False
            
    # 8. Capture Delay (String match)
    if rule.get('capture_delay'):
        # This is often a range like '>5' or exact 'immediate'. 
        # For simplicity in this specific task, we check exact or simple logic if needed.
        # If rule is range-like and data is numeric string, use range.
        # If rule is 'manual'/'immediate', use exact match.
        r_delay = str(rule['capture_delay'])
        t_delay = str(tx_data.get('capture_delay', ''))
        
        if r_delay in ['manual', 'immediate']:
            if r_delay != t_delay:
                return False
        else:
            # Try numeric comparison if possible
            try:
                val = float(t_delay)
                if not check_range(val, r_delay):
                    return False
            except:
                # Fallback to exact string match if not numeric
                if r_delay != t_delay:
                    return False

    return True

# ==========================================
# MAIN EXECUTION
# ==========================================

# 1. Load Data
fees_path = '/output/chunk4/data/context/fees.json'
merchant_path = '/output/chunk4/data/context/merchant_data.json'
payments_path = '/output/chunk4/data/context/payments.csv'

with open(fees_path, 'r') as f:
    fees_data = json.load(f)

with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

df_payments = pd.read_csv(payments_path)

# 2. Get Fee 17
fee_17 = next((f for f in fees_data if f['ID'] == 17), None)
if not fee_17:
    print("Error: Fee 17 not found.")
    exit()

print(f"Fee 17 Original Config: {json.dumps(fee_17, indent=2)}")

# 3. Prepare Merchant Context (Stats & Metadata)
# We need to calculate monthly volume and fraud rates for each merchant to match rules correctly.
# Manual: "Monthly volumes and rates are computed always in natural months... starting day 1..."
# For this exercise, we'll calculate the 2023 average monthly stats or total/12.
# Manual: "monthly total volume... 100k-1m".
# We will calculate total 2023 volume / 12 for 'monthly_volume'.
# We will calculate total 2023 fraud value / total 2023 volume for 'monthly_fraud_level'.

merchant_stats = {}

# Group by merchant to get aggregates
grp = df_payments.groupby('merchant')
for merchant_name, group in grp:
    total_vol = group['eur_amount'].sum()
    fraud_vol = group[group['has_fraudulent_dispute']]['eur_amount'].sum()
    
    # Get static data from merchant_data.json
    m_info = next((m for m in merchant_data if m['merchant'] == merchant_name), {})
    
    merchant_stats[merchant_name] = {
        'merchant': merchant_name,
        'mcc': m_info.get('merchant_category_code'),
        'account_type': m_info.get('account_type'),
        'capture_delay': m_info.get('capture_delay'),
        'monthly_volume': total_vol / 12.0, # Average monthly volume
        'monthly_fraud_level': (fraud_vol / total_vol) if total_vol > 0 else 0.0
    }

# 4. Identify Affected Merchants
# We will iterate through all merchants.
# For each merchant, we check if they have ANY transaction that matches Fee 17.
# We check two scenarios:
#   A. Matches Original Fee 17
#   B. Matches Modified Fee 17 (account_type = ['F'])

affected_merchants = set()

# Optimization: Filter payments to potential candidates first (match static fields of Fee 17)
# This speeds up the loop.
potential_txs = df_payments.copy()
if fee_17.get('card_scheme'):
    potential_txs = potential_txs[potential_txs['card_scheme'] == fee_17['card_scheme']]
if fee_17.get('is_credit') is not None:
    potential_txs = potential_txs[potential_txs['is_credit'] == fee_17['is_credit']]
if fee_17.get('aci'): # If aci is a list, check if tx aci is in it
    potential_txs = potential_txs[potential_txs['aci'].isin(fee_17['aci'])]

# Group potential transactions by merchant
tx_by_merchant = potential_txs.groupby('merchant')

# Create the Modified Fee 17 rule
fee_17_modified = fee_17.copy()
fee_17_modified['account_type'] = ['F']

print("\nAnalyzing merchants...")

for merchant_name, stats in merchant_stats.items():
    # Get sample transaction data for this merchant (or use stats + generic tx fields)
    # Since fee matching depends on transaction-specific fields (like aci, is_credit) 
    # AND merchant-specific fields (volume, mcc), we need to see if *any* transaction matches.
    
    if merchant_name not in tx_by_merchant.groups:
        # No transactions match the basic static criteria (scheme, etc.), so never matches either rule.
        continue
        
    # Get unique transaction profiles for this merchant to test against rules
    # (e.g., unique combinations of aci, is_credit, etc. that exist for this merchant)
    # This avoids checking every single row.
    unique_tx_profiles = tx_by_merchant.get_group(merchant_name)[
        ['card_scheme', 'is_credit', 'aci']
    ].drop_duplicates().to_dict('records')
    
    matches_original = False
    matches_modified = False
    
    for tx_profile in unique_tx_profiles:
        # Combine transaction profile with merchant stats
        full_context = {**tx_profile, **stats}
        
        if match_fee_rule(full_context, fee_17):
            matches_original = True
        
        if match_fee_rule(full_context, fee_17_modified):
            matches_modified = True
            
        if matches_original and matches_modified:
            break # Matches both, so status didn't change for at least one tx type? 
                  # Wait, "affected" means the SET of applicable fees changes.
                  # If they paid it before (matches_original) and don't now (not matches_modified), affected.
                  # If they didn't pay before and do now, affected.
                  # If they paid before and STILL pay it, they are NOT affected (for that transaction).
                  # But if they have *some* transactions that stop matching, they are affected.
    
    # Logic for "Affected":
    # If the merchant had the fee applied (matches_original is True) 
    # AND under the new rule it is NOT applied (matches_modified is False), they are affected.
    # OR if they didn't have it and now do.
    
    # Note: If matches_original is True and matches_modified is True, it means they are Account Type F
    # and continue to pay the fee. They are NOT affected by the change (no change in fee status).
    
    if matches_original != matches_modified:
        affected_merchants.add(merchant_name)
        print(f" -> {merchant_name}: Original={matches_original}, Modified={matches_modified} (Affected)")

# 5. Output Result
print("\n" + "="*30)
print("AFFECTED MERCHANTS")
print("="*30)
result_list = sorted(list(affected_merchants))
print(", ".join(result_list))

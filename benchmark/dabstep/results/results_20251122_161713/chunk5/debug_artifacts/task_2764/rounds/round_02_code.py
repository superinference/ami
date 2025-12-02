# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2764
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9093 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------
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

def parse_volume_range(vol_str):
    """Parses volume strings like '100k-1m' into (min, max)."""
    if not vol_str:
        return (0, float('inf'))
    
    s = vol_str.lower().replace(',', '').replace('€', '')
    
    def parse_val(x):
        x = x.strip()
        mult = 1
        if 'k' in x: mult = 1000; x = x.replace('k', '')
        elif 'm' in x: mult = 1000000; x = x.replace('m', '')
        return float(x) * mult

    if '>' in s:
        return (parse_val(s.replace('>', '')), float('inf'))
    if '<' in s:
        return (0, parse_val(s.replace('<', '')))
    if '-' in s:
        parts = s.split('-')
        return (parse_val(parts[0]), parse_val(parts[1]))
    
    return (0, float('inf'))

def parse_fraud_range(fraud_str):
    """Parses fraud strings like '7.7%-8.3%' into (min, max)."""
    if not fraud_str:
        return (0, float('inf'))
    
    s = fraud_str.replace('%', '')
    
    def parse_val(x):
        return float(x.strip()) / 100.0

    if '>' in s:
        return (parse_val(s.replace('>', '')), float('inf'))
    if '<' in s:
        return (0, parse_val(s.replace('<', '')))
    if '-' in s:
        parts = s.split('-')
        return (parse_val(parts[0]), parse_val(parts[1]))
        
    return (0, float('inf'))

def check_rule_match(rule, merchant_profile, transaction):
    """
    Checks if a fee rule applies to a specific transaction given the merchant profile.
    """
    # 1. Static Merchant Checks (Pre-validated in main loop, but good for safety)
    # (Skipping here as we will pre-filter for efficiency)

    # 2. Transaction Specific Checks
    # Is Credit
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != transaction['is_credit']:
            return False
            
    # ACI (Authorization Characteristics Indicator)
    # Rule ACI is a list of allowed values. If None/Empty, it applies to all.
    if rule.get('aci'):
        if transaction['aci'] not in rule['aci']:
            return False
            
    # Intracountry
    # Rule intracountry is boolean. If None, applies to all.
    if rule.get('intracountry') is not None:
        # Intracountry definition: Issuer Country == Acquirer Country
        is_intra = transaction['issuing_country'] == transaction['acquirer_country']
        # Note: fees.json uses 0.0/1.0 or boolean for this field sometimes? 
        # Let's handle the type safely.
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    # Formula: fee = fixed_amount + rate * transaction_value / 10000
    fixed = float(rule.get('fixed_amount', 0))
    rate = float(rule.get('rate', 0))
    return fixed + (rate * amount / 10000.0)

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# File Paths
payments_path = '/output/chunk5/data/context/payments.csv'
merchant_data_path = '/output/chunk5/data/context/merchant_data.json'
fees_path = '/output/chunk5/data/context/fees.json'

# 1. Load Data
print("Loading data...")
df = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Filter for Merchant and Year
target_merchant = 'Martinis_Fine_Steakhouse'
target_year = 2023

df_merchant = df[(df['merchant'] == target_merchant) & (df['year'] == target_year)].copy()
print(f"Transactions found: {len(df_merchant)}")

# 3. Build Merchant Profile
# Get Metadata
merchant_meta = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_meta:
    raise ValueError(f"Merchant {target_merchant} not found in metadata.")

mcc = merchant_meta.get('merchant_category_code')
account_type = merchant_meta.get('account_type')
capture_delay = merchant_meta.get('capture_delay')

# Calculate Dynamic Metrics
# Volume: Monthly Average
total_volume = df_merchant['eur_amount'].sum()
avg_monthly_volume = total_volume / 12.0

# Fraud: Ratio of Fraud Volume / Total Volume
fraud_txs = df_merchant[df_merchant['has_fraudulent_dispute'] == True]
fraud_volume = fraud_txs['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

print(f"Profile - MCC: {mcc}, Account: {account_type}, Capture: {capture_delay}")
print(f"Profile - Avg Monthly Vol: €{avg_monthly_volume:,.2f}, Fraud Rate: {fraud_rate:.4%}")

# 4. Pre-filter Fee Rules based on Merchant Profile
# We only keep rules that *could* apply to this merchant (ignoring tx-specific fields for now)
applicable_rules = []

for rule in fees_data:
    # Check MCC (Rule list must contain MCC or be empty)
    if rule.get('merchant_category_code') and mcc not in rule['merchant_category_code']:
        continue
        
    # Check Account Type (Rule list must contain Account Type or be empty)
    if rule.get('account_type') and account_type not in rule['account_type']:
        continue
        
    # Check Capture Delay (Rule must match or be null)
    if rule.get('capture_delay') and rule['capture_delay'] != capture_delay:
        continue
        
    # Check Monthly Volume (Merchant vol must be in range or rule is null)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_volume_range(rule['monthly_volume'])
        if not (min_v <= avg_monthly_volume <= max_v):
            continue
            
    # Check Monthly Fraud Level (Merchant fraud must be in range or rule is null)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_fraud_range(rule['monthly_fraud_level'])
        if not (min_f <= fraud_rate <= max_f):
            continue
            
    applicable_rules.append(rule)

print(f"Applicable rules found: {len(applicable_rules)}")

# 5. Simulate Costs per Scheme
# Identify available schemes in the filtered rules
schemes = set(r['card_scheme'] for r in applicable_rules)
scheme_costs = {}

# Pre-calculate transaction fields needed for matching to speed up loop
# We need: is_credit, aci, issuing_country, acquirer_country, eur_amount
# Note: acquirer_country is in the CSV.
tx_records = df_merchant[['is_credit', 'aci', 'issuing_country', 'acquirer_country', 'eur_amount']].to_dict('records')

for scheme in schemes:
    # Get rules for this specific scheme
    scheme_rules = [r for r in applicable_rules if r['card_scheme'] == scheme]
    
    total_scheme_fee = 0.0
    covered_transactions = 0
    
    for tx in tx_records:
        # Find matching rule for this transaction
        matched_rule = None
        for rule in scheme_rules:
            if check_rule_match(rule, None, tx):
                matched_rule = rule
                break # Assume first match wins (standard rule engine logic)
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_scheme_fee += fee
            covered_transactions += 1
        else:
            # If a scheme cannot process a transaction (no rule), it's technically not a valid option
            # for "steering ALL traffic". However, for comparison, we might penalize or ignore.
            # Given the problem type, we assume the valid scheme covers the traffic.
            # If coverage is low, we'll note it.
            pass

    # Only consider schemes that cover 100% of transactions (or very close)
    # to ensure a fair comparison.
    if covered_transactions == len(tx_records):
        scheme_costs[scheme] = total_scheme_fee
        print(f"Scheme: {scheme} | Total Fee: €{total_scheme_fee:,.2f} | Coverage: 100%")
    else:
        print(f"Scheme: {scheme} | Incomplete Coverage ({covered_transactions}/{len(tx_records)}) - Discarded")

# 6. Determine Winner
if not scheme_costs:
    print("No schemes found that cover all transactions.")
else:
    best_scheme = min(scheme_costs, key=scheme_costs.get)
    min_fee = scheme_costs[best_scheme]
    print(f"\nRecommended Scheme: {best_scheme}")
    print(f"Minimum Total Fees: €{min_fee:,.2f}")
    
    # Output just the name as requested by the goal
    print(best_scheme)

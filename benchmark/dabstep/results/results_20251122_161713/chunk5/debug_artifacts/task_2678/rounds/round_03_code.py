# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2678
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 9247 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------
def coerce_to_float(value):
    """Convert string with %, $, commas to float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if v.startswith('>'):
            return float(v[1:]) + 0.000001
        if v.startswith('<'):
            return float(v[1:]) - 0.000001
        return float(v)
    return 0.0

def parse_range_match(range_str, value):
    """
    Check if value fits in range_str.
    Formats: '100k-1m', '>5', '<3', '7.7%-8.3%', '0.0', '1.0', 'None'
    """
    if range_str is None:
        return True
    
    s = str(range_str).strip()
    
    # Handle simple boolean/numeric strings first
    if s.lower() == 'none': return True
    
    try:
        # Handle k/m suffixes for volume
        def parse_val(x):
            x = x.lower().replace('%', '')
            mult = 1
            if 'k' in x:
                mult = 1000
                x = x.replace('k', '')
            elif 'm' in x:
                mult = 1000000
                x = x.replace('m', '')
            return float(x) * mult

        # Check for ranges
        if '-' in s:
            low_s, high_s = s.split('-')
            low = parse_val(low_s)
            high = parse_val(high_s)
            
            # If inputs were percentages, scale them
            if '%' in s:
                low /= 100.0
                high /= 100.0
                
            return low <= value <= high
        
        if s.startswith('>'):
            limit = parse_val(s[1:])
            if '%' in s: limit /= 100.0
            return value > limit
            
        if s.startswith('<'):
            limit = parse_val(s[1:])
            if '%' in s: limit /= 100.0
            return value < limit
            
        # Exact match
        val = parse_val(s)
        if '%' in s: val /= 100.0
        # Float comparison tolerance
        return abs(value - val) < 1e-9
        
    except Exception:
        return False

def check_capture_delay(rule_delay, merchant_delay):
    """
    Compare rule capture delay (often a range/condition) with merchant's specific delay.
    Merchant delay is typically a specific string: '1', 'manual', 'immediate'.
    Rule delay: '3-5', '>5', '<3', 'immediate', 'manual', or null.
    """
    if rule_delay is None:
        return True
    
    md = str(merchant_delay).lower()
    rd = str(rule_delay).lower()
    
    # Direct string matches
    if rd == md:
        return True
        
    # Numeric comparisons if merchant delay is a number (e.g., '1')
    if md.isdigit():
        md_val = float(md)
        if rd.startswith('<'):
            return md_val < float(rd[1:])
        if rd.startswith('>'):
            return md_val > float(rd[1:])
        if '-' in rd:
            try:
                low, high = map(float, rd.split('-'))
                return low <= md_val <= high
            except:
                pass
                
    return False

def calculate_fee(amount, rule):
    """Calculate fee based on fixed amount and rate (basis points)."""
    # Fee = Fixed + (Rate * Amount / 10000)
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000.0)

# ---------------------------------------------------------
# MAIN ANALYSIS
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_path = '/output/chunk5/data/context/merchant_data.json'
acquirer_path = '/output/chunk5/data/context/acquirer_countries.csv'

df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
df_acq = pd.read_csv(acquirer_path)

# 2. Filter for Merchant and November
target_merchant = 'Belles_cookbook_store'
# November is roughly day 305 to 334 (non-leap year)
df_nov = df[
    (df['merchant'] == target_merchant) & 
    (df['day_of_year'] >= 305) & 
    (df['day_of_year'] <= 334)
].copy()

if df_nov.empty:
    print("No transactions found for merchant in November.")
    exit()

# 3. Get Merchant Attributes
m_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not m_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

mcc = m_info['merchant_category_code']
account_type = m_info['account_type']
capture_delay = m_info['capture_delay']
acquirers = m_info['acquirer'] # List of acquirers

# Determine Acquirer Country
# We assume the merchant uses their configured acquirer.
# If multiple, we check the first one or map from the dataset if consistent.
# Belles_cookbook_store has 'lehman_brothers'.
acquirer_name = acquirers[0]
acquirer_country_row = df_acq[df_acq['acquirer'] == acquirer_name]
if not acquirer_country_row.empty:
    merchant_acquirer_country = acquirer_country_row.iloc[0]['country_code']
else:
    # Fallback: use the most common acquirer country in the actual data for this merchant
    merchant_acquirer_country = df_nov['acquirer_country'].mode()[0]

# 4. Calculate Monthly Stats (Volume & Fraud)
# These define the "Tier" the merchant falls into for the whole month
total_vol = df_nov['eur_amount'].sum()
fraud_vol = df_nov[df_nov['has_fraudulent_dispute']]['eur_amount'].sum()
fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0

# 5. Simulate Fees for Each Scheme
schemes = ['GlobalCard', 'NexPay', 'SwiftCharge', 'TransactPlus']
scheme_costs = {}

# Pre-process transactions for speed
# We need: amount, is_credit, aci, issuing_country
tx_list = []
for _, row in df_nov.iterrows():
    # Determine intracountry based on Merchant's Acquirer Country vs Issuing Country
    is_intra = (row['issuing_country'] == merchant_acquirer_country)
    
    tx_list.append({
        'amount': row['eur_amount'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': is_intra
    })

for scheme in schemes:
    total_fee = 0.0
    
    # Filter rules for this scheme
    scheme_rules = [r for r in fees_data if r['card_scheme'] == scheme]
    
    # Filter rules that match the Merchant-Level attributes (Static for all txs)
    # MCC, Account Type, Capture Delay, Monthly Volume, Monthly Fraud
    # This optimization reduces the inner loop complexity significantly
    applicable_rules = []
    for rule in scheme_rules:
        # 1. MCC
        if rule['merchant_category_code'] and mcc not in rule['merchant_category_code']:
            continue
        # 2. Account Type
        if rule['account_type'] and account_type not in rule['account_type']:
            continue
        # 3. Capture Delay
        if not check_capture_delay(rule['capture_delay'], capture_delay):
            continue
        # 4. Monthly Volume
        if not parse_range_match(rule['monthly_volume'], total_vol):
            continue
        # 5. Monthly Fraud
        if not parse_range_match(rule['monthly_fraud_level'], fraud_rate):
            continue
        
        applicable_rules.append(rule)
    
    # Now loop through transactions and match Transaction-Level attributes
    # is_credit, aci, intracountry
    for tx in tx_list:
        matched_rule = None
        
        for rule in applicable_rules:
            # 6. Is Credit
            if rule['is_credit'] is not None:
                if rule['is_credit'] != tx['is_credit']:
                    continue
            
            # 7. ACI
            if rule['aci'] and tx['aci'] not in rule['aci']:
                continue
                
            # 8. Intracountry
            if rule['intracountry'] is not None:
                # Rule intracountry might be float 0.0/1.0 or bool
                r_intra = rule['intracountry']
                if isinstance(r_intra, (int, float)):
                    r_intra = bool(r_intra)
                elif isinstance(r_intra, str):
                    r_intra = (r_intra.lower() == 'true' or r_intra == '1.0')
                
                if r_intra != tx['intracountry']:
                    continue
            
            # If we get here, it's a match
            matched_rule = rule
            break
        
        if matched_rule:
            fee = calculate_fee(tx['amount'], matched_rule)
            total_fee += fee
        else:
            # Fallback: If no rule matches, this scheme might be invalid or very expensive.
            # For this exercise, we assume valid configuration is possible or assign a penalty.
            # However, usually the dataset is complete.
            # We'll assign a high default fee to discourage selecting this scheme if rules are missing.
            total_fee += 9999.0 

    scheme_costs[scheme] = total_fee

# 6. Find Minimum Cost Scheme
min_scheme = min(scheme_costs, key=scheme_costs.get)
min_cost = scheme_costs[min_scheme]

# Output the result
print(min_scheme)

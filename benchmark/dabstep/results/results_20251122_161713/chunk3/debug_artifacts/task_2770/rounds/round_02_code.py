# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2770
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9062 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
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
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
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

def parse_range_value(val_str, is_percentage=False):
    """Parses a single value string like '100k', '1m', '8.3%' into a float."""
    if not isinstance(val_str, str):
        return float(val_str)
    
    s = val_str.lower().strip()
    multiplier = 1.0
    
    if 'k' in s:
        multiplier = 1_000.0
        s = s.replace('k', '')
    elif 'm' in s:
        multiplier = 1_000_000.0
        s = s.replace('m', '')
        
    if '%' in s:
        is_percentage = True
        s = s.replace('%', '')
        
    try:
        val = float(s) * multiplier
        return val / 100.0 if is_percentage else val
    except ValueError:
        return 0.0

def check_range_match(value, rule_range_str, is_percentage=False):
    """Checks if a value falls within a rule's range string (e.g., '100k-1m', '>5')."""
    if not rule_range_str:
        return True # Wildcard matches all
        
    s = str(rule_range_str).strip()
    
    # Handle inequalities
    if s.startswith('>'):
        limit = parse_range_value(s[1:], is_percentage)
        return value > limit
    if s.startswith('<'):
        limit = parse_range_value(s[1:], is_percentage)
        return value < limit
        
    # Handle ranges
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            low = parse_range_value(parts[0], is_percentage)
            high = parse_range_value(parts[1], is_percentage)
            return low <= value <= high
            
    # Handle exact match (rare for these fields, but possible)
    target = parse_range_value(s, is_percentage)
    return value == target

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_ctx must contain: card_scheme, account_type, mcc, is_credit, aci, 
                         intracountry, capture_delay, monthly_volume, monthly_fraud_level
    """
    # 1. Card Scheme (Exact match required)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List containment or Wildcard)
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List containment or Wildcard)
    if rule.get('merchant_category_code'):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Exact match or Wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 5. ACI (List containment or Wildcard)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Exact match 1.0/0.0 or Wildcard)
    if rule.get('intracountry') is not None:
        # tx_ctx['intracountry'] is boolean, rule is float 1.0/0.0
        tx_intra = 1.0 if tx_ctx['intracountry'] else 0.0
        if float(rule['intracountry']) != tx_intra:
            return False
            
    # 7. Capture Delay (Exact match or Wildcard)
    if rule.get('capture_delay'):
        # Handle inequality strings in rule vs string in ctx
        # But usually capture_delay in merchant_data is a specific value like 'immediate'
        # and rule is 'immediate' or '>5'.
        r_cd = str(rule['capture_delay'])
        t_cd = str(tx_ctx['capture_delay'])
        
        if r_cd == t_cd:
            pass # Match
        elif r_cd == '>5':
            # Check if t_cd is numeric and > 5
            if t_cd.isdigit() and int(t_cd) > 5: pass
            else: return False
        elif r_cd == '<3':
            if t_cd.isdigit() and int(t_cd) < 3: pass
            elif t_cd == 'immediate': pass # immediate is < 3
            else: return False
        elif r_cd == '3-5':
            if t_cd.isdigit() and 3 <= int(t_cd) <= 5: pass
            else: return False
        elif r_cd != t_cd:
            return False

    # 8. Monthly Volume (Range check)
    if not check_range_match(tx_ctx['monthly_volume'], rule.get('monthly_volume'), is_percentage=False):
        return False
        
    # 9. Monthly Fraud Level (Range check)
    if not check_range_match(tx_ctx['monthly_fraud_level'], rule.get('monthly_fraud_level'), is_percentage=True):
        return False
        
    return True

def calculate_fee(amount, rule):
    """Calculates fee amount based on fixed_amount and rate."""
    fixed = float(rule.get('fixed_amount', 0.0) or 0.0)
    rate = float(rule.get('rate', 0.0) or 0.0)
    return fixed + (rate * amount / 10000.0)

# ==========================================
# MAIN LOGIC
# ==========================================

# 1. Load Data
payments_path = '/output/chunk3/data/context/payments.csv'
merchant_data_path = '/output/chunk3/data/context/merchant_data.json'
fees_path = '/output/chunk3/data/context/fees.json'

df_payments = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Identify Merchant Context
target_merchant = "Martinis_Fine_Steakhouse"
target_year = 2023

# Get Merchant Config
merchant_config = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_config:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

mcc = merchant_config['merchant_category_code']
account_type = merchant_config['account_type']
capture_delay = merchant_config['capture_delay']

# 3. Calculate Merchant Metrics (Volume & Fraud) for 2023
# These metrics determine the fee tier for ALL transactions
merchant_txs_2023 = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
]

total_volume_2023 = merchant_txs_2023['eur_amount'].sum()
avg_monthly_volume = total_volume_2023 / 12.0

fraud_volume_2023 = merchant_txs_2023[merchant_txs_2023['has_fraudulent_dispute'] == True]['eur_amount'].sum()
# Fraud rate is typically Fraud Volume / Total Volume
fraud_rate = fraud_volume_2023 / total_volume_2023 if total_volume_2023 > 0 else 0.0

print(f"Merchant: {target_merchant}")
print(f"Avg Monthly Volume: €{avg_monthly_volume:,.2f}")
print(f"Fraud Rate: {fraud_rate:.4%}")

# 4. Select Target Transactions (Fraudulent ones to be moved)
fraud_txs = merchant_txs_2023[merchant_txs_2023['has_fraudulent_dispute'] == True].copy()
print(f"Transactions to simulate: {len(fraud_txs)}")

# 5. Simulate Moving ACIs
possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
results = {}

for aci in possible_acis:
    total_fee_for_aci = 0.0
    
    for _, tx in fraud_txs.iterrows():
        # Build context for this transaction with the SIMULATED ACI
        ctx = {
            'card_scheme': tx['card_scheme'],
            'account_type': account_type,
            'mcc': mcc,
            'is_credit': bool(tx['is_credit']),
            'aci': aci, # <--- The variable we are changing
            'intracountry': tx['issuing_country'] == tx['acquirer_country'],
            'capture_delay': capture_delay,
            'monthly_volume': avg_monthly_volume,
            'monthly_fraud_level': fraud_rate
        }
        
        # Find matching fee rule
        # We iterate through fees and take the first match (standard rule engine logic)
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_fee_for_aci += fee
        else:
            # Fallback if no rule matches (should not happen in this dataset)
            # print(f"Warning: No fee rule found for tx {tx['psp_reference']} with ACI {aci}")
            pass
            
    results[aci] = total_fee_for_aci

# 6. Determine Preferred Choice
# Find ACI with minimum total fee
best_aci = min(results, key=results.get)
min_fee = results[best_aci]

print("\nSimulation Results (Total Fees per ACI):")
for aci, fee in results.items():
    print(f"ACI {aci}: €{fee:,.2f}")

print(f"\nPreferred ACI: {best_aci}")

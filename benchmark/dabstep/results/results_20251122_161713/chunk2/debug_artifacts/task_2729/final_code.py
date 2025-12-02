import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

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

def parse_volume_range(vol_str):
    """Parses volume strings like '100k-1m' into (min, max)."""
    if not vol_str:
        return (0, float('inf'))
    
    def parse_val(s):
        s = s.lower().strip()
        mult = 1
        if 'k' in s:
            mult = 1000
            s = s.replace('k', '')
        elif 'm' in s:
            mult = 1000000
            s = s.replace('m', '')
        return float(s) * mult

    try:
        if '-' in vol_str:
            parts = vol_str.split('-')
            return (parse_val(parts[0]), parse_val(parts[1]))
        elif '>' in vol_str:
            return (parse_val(vol_str.replace('>', '')), float('inf'))
        elif '<' in vol_str:
            return (0, parse_val(vol_str.replace('<', '')))
        else:
            val = parse_val(vol_str)
            return (val, val) # Exact match treated as point
    except:
        return (0, float('inf'))

def parse_fraud_range(fraud_str):
    """Parses fraud strings like '0%-8.3%' into (min, max)."""
    if not fraud_str:
        return (0, float('inf'))
    
    def parse_val(s):
        s = s.strip().replace('%', '')
        return float(s) / 100

    try:
        if '-' in fraud_str:
            parts = fraud_str.split('-')
            return (parse_val(parts[0]), parse_val(parts[1]))
        elif '>' in fraud_str:
            return (parse_val(fraud_str.replace('>', '')), float('inf'))
        elif '<' in fraud_str:
            return (0, parse_val(fraud_str.replace('<', '')))
        else:
            val = parse_val(fraud_str)
            return (val, val)
    except:
        return (0, float('inf'))

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    tx_ctx must contain: 
      card_scheme, account_type, capture_delay, monthly_fraud_rate, monthly_volume,
      mcc, is_credit, aci, intracountry
    """
    # 1. Card Scheme (Exact match required)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False

    # 2. Account Type (List match or Wildcard)
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False

    # 3. Capture Delay (Exact match or Wildcard)
    if rule.get('capture_delay'):
        # Handle range logic if necessary, but usually these are categorical strings in this dataset
        # The manual lists specific values: '3-5', '>5', '<3', 'immediate', 'manual'
        if rule['capture_delay'] != tx_ctx['capture_delay']:
             # Simple string comparison for now as per dataset inspection
             return False

    # 4. Merchant Category Code (List match or Wildcard)
    if rule.get('merchant_category_code'):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False

    # 5. Is Credit (Boolean match or Wildcard)
    if rule.get('is_credit') is not None: # Check explicitly for None as False is a valid value
        # Convert rule value to bool to be safe, though JSON loads as bool
        rule_credit = str(rule['is_credit']).lower() == 'true'
        tx_credit = tx_ctx['is_credit']
        if rule_credit != tx_credit:
            return False

    # 6. ACI (List match or Wildcard)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 7. Intracountry (Boolean match or Wildcard)
    if rule.get('intracountry') is not None:
        # JSON might have "0.0" or "1.0" strings or actual bools
        rule_intra = str(rule['intracountry']).replace('.0', '')
        is_rule_true = rule_intra in ['1', 'true', 'True']
        is_rule_false = rule_intra in ['0', 'false', 'False']
        
        if is_rule_true and not tx_ctx['intracountry']:
            return False
        if is_rule_false and tx_ctx['intracountry']:
            return False

    # 8. Monthly Volume (Range match or Wildcard)
    if rule.get('monthly_volume'):
        min_vol, max_vol = parse_volume_range(rule['monthly_volume'])
        if not (min_vol <= tx_ctx['monthly_volume'] <= max_vol):
            return False

    # 9. Monthly Fraud Level (Range match or Wildcard)
    if rule.get('monthly_fraud_level'):
        min_fraud, max_fraud = parse_fraud_range(rule['monthly_fraud_level'])
        # Use a small epsilon for float comparison if needed, but <= is usually fine
        if not (min_fraud <= tx_ctx['monthly_fraud_rate'] <= max_fraud):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0)
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000.0)

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# File paths
payments_path = '/output/chunk2/data/context/payments.csv'
merchant_data_path = '/output/chunk2/data/context/merchant_data.json'
fees_path = '/output/chunk2/data/context/fees.json'

# 1. Load Data
print("Loading data...")
df_payments = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Filter for Merchant and Time Period (July: Day 182-212)
target_merchant = 'Golfclub_Baron_Friso'
start_day = 182
end_day = 212

# Get all transactions for this merchant in July to calculate stats
july_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['day_of_year'] >= start_day) &
    (df_payments['day_of_year'] <= end_day)
].copy()

if len(july_txs) == 0:
    print("No transactions found for this merchant in July.")
    exit()

# 3. Calculate Merchant Stats for July (Volume and Fraud Rate)
# Manual: "Fraud is defined as the ratio of fraudulent volume over total volume."
total_volume = july_txs['eur_amount'].sum()
fraud_volume = july_txs[july_txs['has_fraudulent_dispute'] == True]['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

print(f"Merchant: {target_merchant}")
print(f"July Total Volume: €{total_volume:,.2f}")
print(f"July Fraud Volume: €{fraud_volume:,.2f}")
print(f"July Fraud Rate: {fraud_rate:.4%}")

# 4. Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    print(f"Merchant info not found for {target_merchant}")
    exit()

account_type = merchant_info.get('account_type')
mcc = merchant_info.get('merchant_category_code')
capture_delay = merchant_info.get('capture_delay')

print(f"Account Type: {account_type}, MCC: {mcc}, Capture Delay: {capture_delay}")

# 5. Identify Fraudulent Transactions to "Move"
# The question asks: "if we were to move the fraudulent transactions towards a different ACI"
fraud_txs = july_txs[july_txs['has_fraudulent_dispute'] == True].copy()
print(f"Number of fraudulent transactions to simulate: {len(fraud_txs)}")

# 6. Simulate Fees for each ACI
possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
aci_fees = {}

print("\nSimulating fees for different ACIs...")

for test_aci in possible_acis:
    total_fee_for_aci = 0.0
    match_count = 0
    
    for _, tx in fraud_txs.iterrows():
        # Construct transaction context
        # Note: intracountry is True if issuing_country == acquirer_country
        is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
        
        tx_ctx = {
            'card_scheme': tx['card_scheme'],
            'account_type': account_type,
            'capture_delay': capture_delay,
            'monthly_fraud_rate': fraud_rate,
            'monthly_volume': total_volume,
            'mcc': mcc,
            'is_credit': bool(tx['is_credit']),
            'aci': test_aci, # The variable we are changing
            'intracountry': is_intracountry
        }
        
        # Find matching rule
        # We iterate through fees_data and take the first match.
        # Ideally, we should look for the most specific match, but without specific precedence rules,
        # first match is the standard approach for this type of rule engine simulation.
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(tx_ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_fee_for_aci += fee
            match_count += 1
        else:
            # If no rule matches, we might assume a default or log it.
            # For this exercise, we assume coverage exists.
            # print(f"No rule match for tx {tx['psp_reference']} with ACI {test_aci}")
            pass
            
    aci_fees[test_aci] = total_fee_for_aci
    # print(f"ACI {test_aci}: €{total_fee_for_aci:.2f} (Matched {match_count}/{len(fraud_txs)} txs)")

# 7. Determine Preferred Choice
# We want the lowest possible fees.
if not aci_fees:
    print("No fees calculated.")
else:
    best_aci = min(aci_fees, key=aci_fees.get)
    min_fee = aci_fees[best_aci]
    
    print("\nResults:")
    for aci, fee in aci_fees.items():
        print(f"ACI {aci}: €{fee:.2f}")
        
    print(f"\nPreferred ACI: {best_aci} with total fees of €{min_fee:.2f}")
    print(best_aci)
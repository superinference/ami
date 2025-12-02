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

def parse_volume_string(vol_str):
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

    if '-' in vol_str:
        parts = vol_str.split('-')
        return (parse_val(parts[0]), parse_val(parts[1]))
    elif '>' in vol_str:
        return (parse_val(vol_str.replace('>', '')), float('inf'))
    elif '<' in vol_str:
        return (0, parse_val(vol_str.replace('<', '')))
    return (0, float('inf'))

def parse_fraud_string(fraud_str):
    """Parses fraud strings like '>8.3%' into (min, max)."""
    if not fraud_str:
        return (0.0, 1.0) # 0% to 100%
    
    def parse_val(s):
        s = s.strip().replace('%', '')
        return float(s) / 100.0

    if '-' in fraud_str:
        parts = fraud_str.split('-')
        return (parse_val(parts[0]), parse_val(parts[1]))
    elif '>' in fraud_str:
        return (parse_val(fraud_str.replace('>', '')), 1.0)
    elif '<' in fraud_str:
        return (0.0, parse_val(fraud_str.replace('<', '')))
    return (0.0, 1.0)

def check_capture_delay(rule_delay, merchant_delay):
    """Checks if merchant capture delay matches rule."""
    if rule_delay is None:
        return True
    
    # Normalize merchant delay
    md = str(merchant_delay).lower()
    rd = str(rule_delay).lower()
    
    if rd == 'immediate':
        return md == 'immediate'
    if rd == 'manual':
        return md == 'manual'
    
    # Numeric handling
    try:
        days = float(md)
    except ValueError:
        return False # Can't compare string merchant delay (e.g. 'manual') with numeric rule
        
    if '-' in rd:
        low, high = map(float, rd.split('-'))
        return low <= days <= high
    if '>' in rd:
        val = float(rd.replace('>', ''))
        return days > val
    if '<' in rd:
        val = float(rd.replace('<', ''))
        return days < val
        
    return False

def match_fee_rule(tx_context, rule):
    """
    Matches a transaction context against a fee rule.
    tx_context requires: 
        card_scheme, account_type, mcc, is_credit, aci, 
        monthly_volume, monthly_fraud_rate, capture_delay, 
        issuing_country, acquirer_country
    """
    # 1. Card Scheme
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (Wildcard allowed)
    if rule['account_type'] and tx_context['account_type'] not in rule['account_type']:
        return False

    # 3. MCC (Wildcard allowed)
    if rule['merchant_category_code'] and tx_context['mcc'] not in rule['merchant_category_code']:
        return False

    # 4. Is Credit (Wildcard allowed)
    if rule['is_credit'] is not None:
        # Convert rule bool to match tx bool
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 5. ACI (Wildcard allowed)
    if rule['aci'] and tx_context['aci'] not in rule['aci']:
        return False

    # 6. Capture Delay (Wildcard allowed)
    if not check_capture_delay(rule['capture_delay'], tx_context['capture_delay']):
        return False

    # 7. Monthly Volume (Wildcard allowed)
    if rule['monthly_volume']:
        min_vol, max_vol = parse_volume_string(rule['monthly_volume'])
        if not (min_vol <= tx_context['monthly_volume'] <= max_vol):
            return False

    # 8. Monthly Fraud Level (Wildcard allowed)
    if rule['monthly_fraud_level']:
        min_fraud, max_fraud = parse_fraud_string(rule['monthly_fraud_level'])
        # Use a small epsilon for float comparison if needed, but direct comparison usually ok here
        if not (min_fraud <= tx_context['monthly_fraud_rate'] <= max_fraud):
            return False

    # 9. Intracountry (Wildcard allowed)
    if rule['intracountry'] is not None:
        is_intra = (tx_context['issuing_country'] == tx_context['acquirer_country'])
        # rule['intracountry'] might be 0.0/1.0 or boolean
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    return True

def calculate_fee_amount(amount, rule):
    """Calculates fee: fixed + (rate * amount / 10000)"""
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000.0)

# ═══════════════════════════════════════════════════════════
# MAIN LOGIC
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk2/data/context/payments.csv'
merchant_path = '/output/chunk2/data/context/merchant_data.json'
fees_path = '/output/chunk2/data/context/fees.json'

df_payments = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Filter for Merchant and Year
target_merchant = 'Belles_cookbook_store'
target_year = 2023

df_txs = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
].copy()

# 3. Get Merchant Profile
merchant_profile = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_profile:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

actual_mcc = merchant_profile['merchant_category_code']
hypothetical_mcc = 5911
account_type = merchant_profile['account_type']
capture_delay = merchant_profile['capture_delay']

# 4. Calculate Monthly Stats (Volume and Fraud Rate)
# Map day_of_year to month (approximate is fine, but let's be precise for 2023 non-leap)
# 2023 is not a leap year.
# Jan: 1-31, Feb: 32-59, Mar: 60-90, etc.
# Easier: Use pandas to convert to datetime
df_txs['date'] = pd.to_datetime(df_txs['year'] * 1000 + df_txs['day_of_year'], format='%Y%j')
df_txs['month'] = df_txs['date'].dt.month

# Group by month to get stats
monthly_stats = {}
for month in df_txs['month'].unique():
    month_data = df_txs[df_txs['month'] == month]
    total_vol = month_data['eur_amount'].sum()
    fraud_count = month_data['has_fraudulent_dispute'].sum()
    tx_count = len(month_data)
    # Fraud rate is fraud_volume / total_volume according to manual section 7?
    # Manual says: "Fraud is defined as the ratio of fraudulent volume over total volume." (Section 7)
    # Wait, let's check Section 5: "monthly_fraud_level... ratio between monthly total volume and monthly volume notified as fraud."
    # Actually, usually it's count or volume. Manual Section 5 says "monthly volume notified as fraud".
    # Let's calculate fraud volume.
    fraud_vol = month_data[month_data['has_fraudulent_dispute']]['eur_amount'].sum()
    
    fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    monthly_stats[month] = {
        'volume': total_vol,
        'fraud_rate': fraud_rate
    }

# 5. Calculate Fees for both scenarios
total_fee_actual = 0.0
total_fee_hypothetical = 0.0

# Pre-filter fees to optimize (optional but good practice)
# We can't easily pre-filter by MCC because of the hypothetical scenario, 
# but we can organize them or just iterate. 1000 rules is small enough to iterate.

count_matched_actual = 0
count_matched_hypo = 0

for idx, tx in df_txs.iterrows():
    month = tx['month']
    stats = monthly_stats.get(month, {'volume': 0, 'fraud_rate': 0})
    
    # Context for matching
    context = {
        'card_scheme': tx['card_scheme'],
        'account_type': account_type,
        'mcc': actual_mcc, # Will change for hypothetical
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'monthly_volume': stats['volume'],
        'monthly_fraud_rate': stats['fraud_rate'],
        'capture_delay': capture_delay,
        'issuing_country': tx['issuing_country'],
        'acquirer_country': tx['acquirer_country']
    }
    
    # --- Scenario A: Actual MCC ---
    fee_actual = 0.0
    found_actual = False
    for rule in fees_data:
        if match_fee_rule(context, rule):
            fee_actual = calculate_fee_amount(tx['eur_amount'], rule)
            found_actual = True
            break # Stop at first match
    
    if found_actual:
        total_fee_actual += fee_actual
        count_matched_actual += 1
    else:
        # Fallback or error? Assuming data is complete, there should be a match.
        # If no match, fee is 0 (or we could log it).
        pass

    # --- Scenario B: Hypothetical MCC ---
    context['mcc'] = hypothetical_mcc # Update MCC
    fee_hypo = 0.0
    found_hypo = False
    for rule in fees_data:
        if match_fee_rule(context, rule):
            fee_hypo = calculate_fee_amount(tx['eur_amount'], rule)
            found_hypo = True
            break # Stop at first match
            
    if found_hypo:
        total_fee_hypothetical += fee_hypo
        count_matched_hypo += 1

# 6. Calculate Delta
# Question: "what amount delta will it have to pay"
# If Hypo > Actual, it pays more (positive delta).
delta = total_fee_hypothetical - total_fee_actual

print(f"Transactions Processed: {len(df_txs)}")
print(f"Actual MCC: {actual_mcc}, Matched Rules: {count_matched_actual}")
print(f"Hypothetical MCC: {hypothetical_mcc}, Matched Rules: {count_matched_hypo}")
print(f"Total Fee (Actual): {total_fee_actual:.4f}")
print(f"Total Fee (Hypothetical): {total_fee_hypothetical:.4f}")
print(f"Fee Delta: {delta:.14f}")
# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1746
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9495 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ==========================================
# HELPER FUNCTIONS
# ==========================================

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
        # Range handling (e.g., "50-60") - return mean for coercion, but parsing logic handles ranges separately
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

def parse_range_check(value, range_str):
    """
    Checks if a numeric value falls within a range string.
    Range strings can be: '100k-1m', '>5', '<3', '7.7%-8.3%', 'immediate', 'manual'
    """
    if range_str is None:
        return True
        
    # Handle string literals (e.g., 'immediate', 'manual')
    if isinstance(range_str, str) and not any(c.isdigit() for c in range_str):
        return str(value) == range_str

    # Normalize value
    val = float(value)
    
    # Normalize range string
    s = str(range_str).lower().replace(',', '').replace('%', '')
    
    # Handle k/m suffixes
    def parse_num(n_str):
        n_str = n_str.strip()
        mult = 1
        if 'k' in n_str:
            mult = 1000
            n_str = n_str.replace('k', '')
        elif 'm' in n_str:
            mult = 1000000
            n_str = n_str.replace('m', '')
        return float(n_str) * mult

    try:
        if '-' in s:
            low, high = s.split('-')
            # Check if it's percentage range (already stripped %)
            # If the input value is a ratio (0-1) and range was %, we need to align.
            # Assuming input 'value' for fraud is a ratio (e.g., 0.08), and range was '7.7-8.3' (now 7.7-8.3).
            # We should convert the range numbers to ratio if they look like percentages? 
            # Actually, let's assume the caller handles unit consistency. 
            # For fraud: input 0.08, range "8%" -> parse_num("8")=8. Mismatch.
            # Let's handle % specifically in the caller or here.
            # Better approach: The caller passes raw strings.
            
            # Re-parsing with specific logic for this dataset's known formats
            l = parse_num(low)
            h = parse_num(high)
            
            # Heuristic: if range is 0-100 and value is 0-1, scale value? 
            # Or if range string had '%', scale range?
            if '%' in str(range_str):
                l /= 100
                h /= 100
            
            return l <= val <= h
            
        if '>' in s:
            limit = parse_num(s.replace('>', ''))
            if '%' in str(range_str): limit /= 100
            return val > limit
            
        if '<' in s:
            limit = parse_num(s.replace('<', ''))
            if '%' in str(range_str): limit /= 100
            return val < limit
            
        # Exact match numeric
        target = parse_num(s)
        if '%' in str(range_str): target /= 100
        return val == target
        
    except Exception as e:
        # Fallback for non-numeric matching
        return str(value) == str(range_str)

def match_fee_rule(tx_context, rule):
    """
    Matches a transaction context against a fee rule.
    tx_context must contain:
    - card_scheme, account_type, merchant_category_code, is_credit, aci, 
    - intracountry, capture_delay, monthly_volume, monthly_fraud_level
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match)
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_context['merchant_category_code'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 5. ACI (List match)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        # Intracountry in rule is boolean, tx_context['intracountry'] is boolean
        # However, sometimes rule uses 0.0/1.0 for boolean in JSON?
        # The schema says boolean or null.
        rule_intra = rule['intracountry']
        # Handle case where json has 0.0 for False
        if isinstance(rule_intra, float):
            rule_intra = bool(rule_intra)
        if rule_intra != tx_context['intracountry']:
            return False

    # 7. Capture Delay (Range/String match)
    if rule.get('capture_delay'):
        # capture_delay in merchant data is string (e.g. "1", "manual")
        # rule is string (e.g. "<3", "manual")
        if not parse_range_check(tx_context['capture_delay'], rule['capture_delay']):
            return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_context['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Formula: fixed + (rate * amount / 10000)
    return fixed + (rate * amount / 10000.0)

# ==========================================
# MAIN LOGIC
# ==========================================

# 1. Load Data
base_path = '/output/chunk4/data/context/'
df_payments = pd.read_csv(base_path + 'payments.csv')
with open(base_path + 'fees.json', 'r') as f:
    fees_data = json.load(f)
with open(base_path + 'merchant_data.json', 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Target Merchant and Year
target_merchant = 'Belles_cookbook_store'
target_year = 2023

df_tx = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
].copy()

# 3. Get Merchant Attributes
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

m_account_type = merchant_info['account_type']
m_mcc = merchant_info['merchant_category_code']
m_capture_delay = merchant_info['capture_delay']

# 4. Pre-calculate Monthly Stats
# Map day_of_year to month (2023 is not a leap year)
# Create a date column
df_tx['date'] = pd.to_datetime(df_tx['year'] * 1000 + df_tx['day_of_year'], format='%Y%j')
df_tx['month'] = df_tx['date'].dt.month

# Calculate Monthly Volume and Fraud Rate (Volume based)
# Manual: "Fraud is defined as the ratio of fraudulent volume over total volume."
monthly_stats = {}
for month in range(1, 13):
    month_txs = df_tx[df_tx['month'] == month]
    if month_txs.empty:
        monthly_stats[month] = {'vol': 0, 'fraud_rate': 0}
        continue
    
    total_vol = month_txs['eur_amount'].sum()
    fraud_vol = month_txs[month_txs['has_fraudulent_dispute']]['eur_amount'].sum()
    
    fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0
    monthly_stats[month] = {
        'vol': total_vol,
        'fraud_rate': fraud_rate
    }

# 5. Calculate Fees
total_fees = 0.0
matched_count = 0
unmatched_count = 0

for idx, row in df_tx.iterrows():
    # Build Transaction Context
    month = row['month']
    stats = monthly_stats.get(month, {'vol': 0, 'fraud_rate': 0})
    
    tx_context = {
        'card_scheme': row['card_scheme'],
        'account_type': m_account_type,
        'merchant_category_code': m_mcc,
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': row['issuing_country'] == row['acquirer_country'],
        'capture_delay': m_capture_delay,
        'monthly_volume': stats['vol'],
        'monthly_fraud_level': stats['fraud_rate']
    }
    
    # Find Matching Rule
    # We iterate through fees_data and take the first match.
    # Assuming fees.json is ordered by priority or specificity is handled by the order.
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            matched_rule = rule
            break
    
    if matched_rule:
        fee = calculate_fee(row['eur_amount'], matched_rule)
        total_fees += fee
        matched_count += 1
    else:
        # Fallback or error logging
        unmatched_count += 1
        # print(f"No rule found for tx {row['psp_reference']}")

# 6. Output Result
# Question asks for total fees in euros.
print(f"{total_fees:.2f}")

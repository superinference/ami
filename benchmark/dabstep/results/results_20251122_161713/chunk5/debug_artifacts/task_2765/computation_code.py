import pandas as pd
import json
import numpy as np

# --- Helper Functions ---
def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        
        # Handle percentages
        if '%' in v:
            return float(v.replace('%', '')) / 100.0
            
        # Handle k/m suffixes
        if 'k' in v:
            return float(v.replace('k', '')) * 1000.0
        if 'm' in v:
            return float(v.replace('m', '')) * 1000000.0
            
        # Range handling (e.g., "50-60") - return mean for coercion, but parsing logic handles ranges separately
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                # Recursively coerce parts to handle 100k-1m
                p1 = coerce_to_float(parts[0])
                p2 = coerce_to_float(parts[1])
                return (p1 + p2) / 2
            except:
                pass
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value)

def parse_range(rule_val, actual_val):
    """
    Parses rule strings like '100k-1m', '>5', '<3', '7.7%-8.3%'.
    Returns True if actual_val fits, False otherwise.
    Handles None/Null in rule as wildcard (True).
    """
    if rule_val is None:
        return True
    
    s = str(rule_val).strip().lower()
    
    # Helper to parse number from string part
    def parse_num(n_str):
        return coerce_to_float(n_str)

    try:
        if '-' in s:
            low_str, high_str = s.split('-')
            low = parse_num(low_str)
            high = parse_num(high_str)
            # Precision tolerance
            return (low - 1e-9) <= actual_val <= (high + 1e-9)
        elif s.startswith('>'):
            return actual_val > parse_num(s[1:])
        elif s.startswith('<'):
            return actual_val < parse_num(s[1:])
        elif s == 'immediate' or s == 'manual':
            # String match handled in main logic, but if passed here:
            return s == str(actual_val).lower()
        else:
            # Exact match for numbers
            return abs(actual_val - parse_num(s)) < 1e-9
    except:
        # Fallback for non-numeric strings (like 'manual') if passed as range
        return str(rule_val).lower() == str(actual_val).lower()

def calculate_fee(amount, rule):
    """Calculates the fee amount based on the rule."""
    fixed = float(rule['fixed_amount'])
    rate = float(rule['rate'])
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000.0)

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_data_path = '/output/chunk5/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Merchant 'Rafa_AI' and Year 2023
merchant_name = 'Rafa_AI'
df_rafa = df_payments[(df_payments['merchant'] == merchant_name) & (df_payments['year'] == 2023)].copy()

if df_rafa.empty:
    print("No transactions found for Rafa_AI in 2023")
    exit()

# 3. Calculate Merchant Static Stats (Volume, Fraud, etc.)
total_volume_year = df_rafa['eur_amount'].sum()
avg_monthly_volume = total_volume_year / 12.0

fraud_volume = df_rafa[df_rafa['has_fraudulent_dispute'] == True]['eur_amount'].sum()
# Fraud Rate = Fraud Volume / Total Volume
fraud_rate = fraud_volume / total_volume_year if total_volume_year > 0 else 0.0

# Get Merchant Metadata
m_info = next((item for item in merchant_data if item["merchant"] == merchant_name), None)
if not m_info:
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

# Prepare Merchant Context
merchant_context = {
    'mcc': m_info['merchant_category_code'],
    'account_type': m_info['account_type'],
    'monthly_volume': avg_monthly_volume,
    'monthly_fraud_rate': fraud_rate,
    'capture_delay': m_info['capture_delay']
}

# 4. Pre-calculate Dynamic Transaction Attributes
# Intracountry: True if issuing_country == acquirer_country
# Note: acquirer_country is in payments.csv
df_rafa['is_intra'] = df_rafa['issuing_country'] == df_rafa['acquirer_country']

# Convert transactions to list of dicts for faster iteration
transactions = df_rafa[['eur_amount', 'is_credit', 'aci', 'is_intra']].to_dict('records')

# 5. Simulate Fees for Each Scheme
schemes = ['GlobalCard', 'NexPay', 'SwiftCharge', 'TransactPlus']
results = {}

for scheme in schemes:
    # A. Filter rules for this scheme AND static merchant attributes
    # This optimization reduces the inner loop complexity
    applicable_rules = []
    
    for rule in fees:
        # Scheme Match
        if rule['card_scheme'] != scheme:
            continue
            
        # Static: MCC (List in rule)
        if rule['merchant_category_code'] is not None:
            if merchant_context['mcc'] not in rule['merchant_category_code']:
                continue
                
        # Static: Account Type (List in rule)
        if rule['account_type'] is not None and len(rule['account_type']) > 0:
            if merchant_context['account_type'] not in rule['account_type']:
                continue
                
        # Static: Capture Delay (String in rule)
        if rule['capture_delay'] is not None:
            if str(rule['capture_delay']).lower() != str(merchant_context['capture_delay']).lower():
                continue
                
        # Static: Monthly Volume (Range string in rule)
        if rule['monthly_volume'] is not None:
            if not parse_range(rule['monthly_volume'], merchant_context['monthly_volume']):
                continue
                
        # Static: Monthly Fraud Level (Range string in rule)
        if rule['monthly_fraud_level'] is not None:
            if not parse_range(rule['monthly_fraud_level'], merchant_context['monthly_fraud_rate']):
                continue
        
        applicable_rules.append(rule)
    
    # Sort rules by ID to ensure deterministic matching (lower ID first)
    applicable_rules.sort(key=lambda x: x['ID'])
    
    # B. Calculate Total Fee for this Scheme
    total_scheme_fee = 0.0
    
    for tx in transactions:
        matched_rule = None
        for rule in applicable_rules:
            # Dynamic: Is Credit (Bool/None)
            if rule['is_credit'] is not None:
                if rule['is_credit'] != tx['is_credit']:
                    continue
                    
            # Dynamic: ACI (List/None)
            if rule['aci'] is not None and len(rule['aci']) > 0:
                if tx['aci'] not in rule['aci']:
                    continue
                    
            # Dynamic: Intracountry (Bool/None - mapped to 0.0/1.0 in JSON)
            if rule['intracountry'] is not None:
                # JSON has 0.0 or 1.0 or None. Convert to bool.
                rule_intra_bool = (float(rule['intracountry']) == 1.0)
                if rule_intra_bool != tx['is_intra']:
                    continue
            
            # If we get here, all conditions match
            matched_rule = rule
            break
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_scheme_fee += fee
        else:
            # If no rule matches, we assume 0 fee or skip. 
            # In this context, we assume coverage exists or it's not the optimal path.
            pass
            
    results[scheme] = total_scheme_fee

# 6. Identify the Maximum
max_scheme = max(results, key=results.get)
max_fee = results[max_scheme]

# Output the result
print(max_scheme)
# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1808
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 6357 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, k, m to float. Handles common formats."""
    if pd.isna(value): return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        # Remove operators first for clean number parsing
        v_clean = v.lstrip('><≤≥')
        
        # Handle percentages
        if '%' in v:
            return float(v_clean.replace('%', '')) / 100.0
        
        # Handle k/m suffixes
        if 'k' in v_clean.lower():
            return float(v_clean.lower().replace('k', '')) * 1000
        if 'm' in v_clean.lower():
            return float(v_clean.lower().replace('m', '')) * 1000000
            
        try:
            return float(v_clean)
        except:
            return 0.0
    return float(value)

def parse_range_check(value, rule_str):
    """Check if value fits in rule_str range (e.g. '100k-1m', '>5', '<3', '>8.3%')."""
    if rule_str is None: return True
    
    s = str(rule_str).strip()
    
    # Helper to parse number from string using coerce_to_float
    def parse_num(n_str):
        return coerce_to_float(n_str)

    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            low = parse_num(parts[0])
            high = parse_num(parts[1])
            return low <= value <= high
    
    if s.startswith('>'):
        limit = parse_num(s[1:])
        return value > limit
        
    if s.startswith('<'):
        limit = parse_num(s[1:])
        return value < limit
        
    # Exact match fallback
    return value == parse_num(s)

def check_capture_delay(merchant_delay, rule_delay):
    """Check if merchant delay matches rule delay logic."""
    if rule_delay is None: return True
    
    # Direct string match (e.g. 'manual' == 'manual')
    if str(merchant_delay).lower() == str(rule_delay).lower(): return True
    
    # Map keywords to numbers for comparison
    def delay_to_int(d):
        d = str(d).lower()
        if d == 'immediate': return 0
        if d == 'manual': return 999 # Treat manual as very long delay
        try: return float(d)
        except: return 999 # Fallback
        
    m_val = delay_to_int(merchant_delay)
    
    # Parse rule
    r_str = str(rule_delay).strip()
    if '-' in r_str:
        parts = r_str.split('-')
        low = delay_to_int(parts[0])
        high = delay_to_int(parts[1])
        return low <= m_val <= high
    
    if r_str.startswith('>'):
        limit = delay_to_int(r_str[1:])
        return m_val > limit
        
    if r_str.startswith('<'):
        limit = delay_to_int(r_str[1:])
        return m_val < limit
        
    return False

# --- Main Execution ---

# 1. Load Data
df_payments = pd.read_csv('/output/chunk3/data/context/payments.csv')
with open('/output/chunk3/data/context/merchant_data.json', 'r') as f:
    merchant_data = json.load(f)
with open('/output/chunk3/data/context/fees.json', 'r') as f:
    fees = json.load(f)

# 2. Filter Transactions (Rafa_AI, October 2023)
# October is days 274 to 304 (inclusive)
rafa_oct_txs = df_payments[
    (df_payments['merchant'] == 'Rafa_AI') & 
    (df_payments['year'] == 2023) &
    (df_payments['day_of_year'] >= 274) & 
    (df_payments['day_of_year'] <= 304)
].copy()

if rafa_oct_txs.empty:
    print("No transactions found for Rafa_AI in October 2023.")
    exit()

# 3. Calculate Monthly Stats (Volume and Fraud Rate)
monthly_volume = rafa_oct_txs['eur_amount'].sum()
fraud_txs = rafa_oct_txs[rafa_oct_txs['has_fraudulent_dispute'] == True]
monthly_fraud_vol = fraud_txs['eur_amount'].sum()
# Fraud rate is defined as fraud volume / total volume
monthly_fraud_rate = monthly_fraud_vol / monthly_volume if monthly_volume > 0 else 0.0

# 4. Get Merchant Attributes
merchant_info = next((m for m in merchant_data if m['merchant'] == 'Rafa_AI'), None)
if not merchant_info:
    print("Error: Merchant Rafa_AI not found in merchant_data.json")
    exit()

m_mcc = merchant_info['merchant_category_code']
m_acct = merchant_info['account_type']
m_delay = merchant_info['capture_delay']

# 5. Identify Unique Transaction Profiles
# Calculate intracountry (Issuer == Acquirer)
rafa_oct_txs['intracountry'] = rafa_oct_txs['issuing_country'] == rafa_oct_txs['acquirer_country']

# Select relevant columns and drop duplicates to get unique scenarios
unique_profiles = rafa_oct_txs[['card_scheme', 'is_credit', 'aci', 'intracountry']].drop_duplicates().to_dict('records')

# 6. Match Fees
applicable_ids = set()

for profile in unique_profiles:
    for fee in fees:
        # --- Merchant Static Checks ---
        # MCC (List in rule, empty/None = wildcard)
        if fee['merchant_category_code']:
            if m_mcc not in fee['merchant_category_code']: continue
            
        # Account Type (List in rule, empty/None = wildcard)
        if fee['account_type']: 
            if m_acct not in fee['account_type']: continue
            
        # Capture Delay
        if not check_capture_delay(m_delay, fee['capture_delay']): continue
        
        # --- Monthly Stats Checks ---
        # Volume
        if not parse_range_check(monthly_volume, fee['monthly_volume']): continue
        
        # Fraud
        if not parse_range_check(monthly_fraud_rate, fee['monthly_fraud_level']): continue
        
        # --- Transaction Dynamic Checks ---
        # Card Scheme
        if fee['card_scheme'] != profile['card_scheme']: continue
        
        # Is Credit (Null = wildcard)
        if fee['is_credit'] is not None:
            if fee['is_credit'] != profile['is_credit']: continue
            
        # ACI (List in rule, empty = wildcard)
        if fee['aci']:
            if profile['aci'] not in fee['aci']: continue
            
        # Intracountry (Null = wildcard)
        if fee['intracountry'] is not None:
            # Convert fee value (0.0/1.0) to bool for comparison
            fee_intra = bool(fee['intracountry'])
            if fee_intra != profile['intracountry']: continue
            
        # If all checks pass, this fee is applicable
        applicable_ids.add(fee['ID'])

# 7. Output Result
result_list = sorted(list(applicable_ids))
print(", ".join(map(str, result_list)))

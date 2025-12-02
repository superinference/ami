import json
import pandas as pd
import numpy as np

# --- Helper Functions ---
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
        try:
            return float(v)
        except:
            return 0.0
    return 0.0

def is_not_empty(array):
    """Check if array/list is not empty. Handles numpy arrays safely."""
    if array is None:
        return False
    if hasattr(array, 'size'):  # numpy array
        return array.size > 0
    try:
        return len(array) > 0
    except TypeError:
        return False

def check_capture_delay(merchant_delay, rule_delay):
    """Matches merchant capture delay against rule criteria."""
    if rule_delay is None:
        return True
    if str(merchant_delay) == str(rule_delay):
        return True
    
    try:
        # Convert merchant delay to float if possible (handles '1', '2', '7')
        m_val = float(merchant_delay)
        
        if rule_delay.startswith('>'):
            limit = float(rule_delay[1:])
            return m_val > limit
        elif rule_delay.startswith('<'):
            limit = float(rule_delay[1:])
            return m_val < limit
        elif '-' in rule_delay:
            low, high = map(float, rule_delay.split('-'))
            return low <= m_val <= high
    except:
        pass
    return False

def parse_range_string(s):
    """Parses range strings like '100k-1m' or '>5%' into min/max floats."""
    if not s: return None, None
    s = s.replace('%', '').replace('k', '000').replace('m', '000000').strip()
    
    if '-' in s:
        parts = s.split('-')
        return float(parts[0]), float(parts[1])
    if s.startswith('>'):
        return float(s[1:]), float('inf')
    if s.startswith('<'):
        return float('-inf'), float(s[1:])
    return None, None

# --- Main Execution ---

# 1. Load Data
fees_path = '/output/chunk6/data/context/fees.json'
payments_path = '/output/chunk6/data/context/payments.csv'
merchants_path = '/output/chunk6/data/context/merchant_data.json'

with open(fees_path, 'r') as f:
    fees = json.load(f)

payments = pd.read_csv(payments_path)

with open(merchants_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Prepare Merchant Data
# Convert to DataFrame for easy merging
m_df = pd.DataFrame(merchant_data)
# Keep only necessary columns to avoid clutter
m_cols = ['merchant', 'merchant_category_code', 'account_type', 'capture_delay']
m_df = m_df[[c for c in m_cols if c in m_df.columns]]

# 3. Merge Payments with Merchant Data
# This enriches transactions with MCC, Account Type, etc.
df = pd.merge(payments, m_df, on='merchant', how='left')

# 4. Find Fee Rule 65
rule_65 = next((f for f in fees if f['ID'] == 65), None)

if not rule_65:
    print("Fee Rule 65 not found.")
else:
    print(f"Analyzing Fee Rule 65: {json.dumps(rule_65, indent=2)}")
    
    # 5. Apply Filters based on Rule 65
    
    # Filter by Card Scheme
    if rule_65.get('card_scheme'):
        df = df[df['card_scheme'] == rule_65['card_scheme']]
        
    # Filter by Credit/Debit
    if rule_65.get('is_credit') is not None:
        df = df[df['is_credit'] == rule_65['is_credit']]
        
    # Filter by ACI (Authorization Characteristics Indicator)
    if is_not_empty(rule_65.get('aci')):
        df = df[df['aci'].isin(rule_65['aci'])]
        
    # Filter by Intracountry (Domestic vs International)
    # Calculate transaction status: True if issuing == acquirer
    df['tx_intracountry'] = df['issuing_country'] == df['acquirer_country']
    
    if rule_65.get('intracountry') is not None:
        # Rule value: 0.0 (False) or 1.0 (True)
        required_intra = bool(rule_65['intracountry'])
        df = df[df['tx_intracountry'] == required_intra]
        
    # Filter by Merchant Category Code (MCC)
    if is_not_empty(rule_65.get('merchant_category_code')):
        df = df[df['merchant_category_code'].isin(rule_65['merchant_category_code'])]
        
    # Filter by Account Type
    if is_not_empty(rule_65.get('account_type')):
        df = df[df['account_type'].isin(rule_65['account_type'])]
        
    # Filter by Capture Delay
    if rule_65.get('capture_delay'):
        df = df[df['capture_delay'].apply(lambda x: check_capture_delay(x, rule_65['capture_delay']))]

    # Filter by Monthly Volume / Fraud Level (if applicable)
    # Note: This requires calculating merchant stats. Only done if rule specifies it.
    if rule_65.get('monthly_volume') or rule_65.get('monthly_fraud_level'):
        # Calculate stats per merchant (using 2023 totals as proxy for monthly avg if needed, or total/12)
        # Manual says: "Monthly volumes... computed always in natural months".
        # We'll estimate using annual/12 for volume.
        
        # Calculate fraud volume (sum of eur_amount where has_fraudulent_dispute is True)
        fraud_vols = payments[payments['has_fraudulent_dispute']].groupby('merchant')['eur_amount'].sum()
        
        stats = payments.groupby('merchant').agg(
            total_vol=('eur_amount', 'sum')
        ).reset_index()
        
        stats['fraud_vol'] = stats['merchant'].map(fraud_vols).fillna(0)
        stats['fraud_rate_pct'] = (stats['fraud_vol'] / stats['total_vol']) * 100
        stats['monthly_vol_est'] = stats['total_vol'] / 12
        
        valid_merchants = set(stats['merchant'])
        
        # Check Volume
        if rule_65.get('monthly_volume'):
            v_min, v_max = parse_range_string(rule_65['monthly_volume'])
            if v_min is not None:
                valid_merchants = {m for m in valid_merchants 
                                   if v_min <= stats.loc[stats['merchant']==m, 'monthly_vol_est'].values[0] <= v_max}
        
        # Check Fraud Level
        if rule_65.get('monthly_fraud_level'):
            f_min, f_max = parse_range_string(rule_65['monthly_fraud_level'])
            if f_min is not None:
                valid_merchants = {m for m in valid_merchants 
                                   if f_min <= stats.loc[stats['merchant']==m, 'fraud_rate_pct'].values[0] <= f_max}
                                   
        df = df[df['merchant'].isin(valid_merchants)]

    # 6. Extract and Print Results
    affected_merchants = sorted(df['merchant'].unique())
    
    print(f"\nFound {len(affected_merchants)} merchants affected by Fee 65.")
    print("Merchants:", ", ".join(affected_merchants))
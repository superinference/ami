import pandas as pd
import json
import re

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float. Handles common formats."""
    if value is None: return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except:
                return None
        if 'k' in v.lower():
            try:
                v = v.lower().replace('k', '')
                return float(v) * 1000
            except:
                return None
        if 'm' in v.lower():
            try:
                v = v.lower().replace('m', '')
                return float(v) * 1000000
            except:
                return None
        try:
            return float(v)
        except:
            return None
    return None

def parse_range(range_str):
    """Parses strings like '100k-1m', '<3', '>5', '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    # Handle inequalities
    if range_str.startswith('>'):
        val = coerce_to_float(range_str[1:])
        return val, float('inf')
    if range_str.startswith('<'):
        val = coerce_to_float(range_str[1:])
        return float('-inf'), val
        
    # Handle ranges
    if '-' in range_str:
        parts = range_str.split('-')
        if len(parts) == 2:
            return coerce_to_float(parts[0]), coerce_to_float(parts[1])
            
    # Handle exact matches (treated as min=max)
    val = coerce_to_float(range_str)
    if val is not None:
        return val, val
        
    return None, None

def check_capture_delay(merchant_val, rule_val):
    """Checks if merchant capture delay matches the rule."""
    if rule_val is None: return True
    
    # Direct string match (e.g., "immediate" == "immediate")
    if str(merchant_val).lower() == str(rule_val).lower(): 
        return True
    
    # Handle numeric comparisons if merchant_val is numeric-like (e.g. "1" vs "<3")
    try:
        m_float = float(merchant_val)
        min_v, max_v = parse_range(rule_val)
        if min_v is not None and max_v is not None:
            # Range check
            if '-' in rule_val:
                return min_v <= m_float <= max_v
            # Inequality check
            if rule_val.startswith('>'):
                return m_float > min_v # Strictly greater usually for >
            if rule_val.startswith('<'):
                return m_float < max_v # Strictly less usually for <
    except (ValueError, TypeError):
        # merchant_val was not numeric (e.g. "immediate"), and didn't match string above
        pass
    return False

def match_fee_rule(ctx, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    ctx: dict containing transaction and merchant attributes
    rule: dict containing fee rule definition
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != ctx['card_scheme']:
        return False
        
    # 2. Account Type (List in rule, must contain merchant's type)
    if rule.get('account_type'): # If list is not empty
        if ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List in rule, must contain merchant's MCC)
    if rule.get('merchant_category_code'):
        if ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (Category or Range)
    if rule.get('capture_delay'):
        if not check_capture_delay(ctx['capture_delay'], rule['capture_delay']):
            return False
            
    # 5. Is Credit (Boolean match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != ctx['is_credit']:
            return False
            
    # 6. ACI (List in rule, must contain transaction's ACI)
    if rule.get('aci'):
        if ctx['aci'] not in rule['aci']:
            return False
            
    # 7. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        # Rule value might be 0.0/1.0 or False/True
        rule_intra = bool(rule['intracountry'])
        if rule_intra != ctx['is_intracountry']:
            return False
            
    # 8. Monthly Volume (Range check)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if min_v is not None:
            if not (min_v <= ctx['monthly_volume'] <= max_v):
                return False
                
    # 9. Monthly Fraud Level (Range check)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        if min_f is not None:
            # Context fraud is ratio (0.08), Rule is usually % (8%) handled by parse_range
            if not (min_f <= ctx['monthly_fraud_rate'] <= max_f):
                return False

    return True

# --- Main Execution ---

# Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
merchant_path = '/output/chunk6/data/context/merchant_data.json'
fees_path = '/output/chunk6/data/context/fees.json'

df_payments = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees = json.load(f)

# Configuration
target_merchant = "Martinis_Fine_Steakhouse"
target_year = 2023
target_day = 10

# 1. Get Merchant Attributes
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

# 2. Calculate Monthly Stats for January 2023
# Manual states: "Monthly volumes and rates are computed always in natural months"
# We use the full month of January (days 1-31) to determine the volume/fraud tier applicable.
jan_mask = (
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= 1) & 
    (df_payments['day_of_year'] <= 31)
)
jan_txs = df_payments[jan_mask]

monthly_volume = jan_txs['eur_amount'].sum()
fraud_count = jan_txs['has_fraudulent_dispute'].sum()
total_count = len(jan_txs)
monthly_fraud_rate = (fraud_count / total_count) if total_count > 0 else 0.0

# 3. Get Transactions for the Specific Day (Day 10)
day_mask = (
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] == target_day)
)
day_txs = df_payments[day_mask]

if day_txs.empty:
    print("No transactions found for this merchant on the specified day.")
else:
    # 4. Identify Unique Transaction Profiles
    # Fees depend on: card_scheme, is_credit, aci, intracountry (issuing vs acquirer)
    cols_of_interest = ['card_scheme', 'is_credit', 'aci', 'issuing_country', 'acquirer_country']
    unique_profiles = day_txs[cols_of_interest].drop_duplicates().to_dict('records')

    applicable_fee_ids = set()

    # 5. Match Fees
    for profile in unique_profiles:
        # Construct context for this specific transaction type
        context = {
            'card_scheme': profile['card_scheme'],
            'is_credit': profile['is_credit'],
            'aci': profile['aci'],
            'is_intracountry': profile['issuing_country'] == profile['acquirer_country'],
            'account_type': merchant_info['account_type'],
            'mcc': merchant_info['merchant_category_code'],
            'capture_delay': merchant_info['capture_delay'],
            'monthly_volume': monthly_volume,
            'monthly_fraud_rate': monthly_fraud_rate
        }
        
        # Check against all fee rules
        for rule in fees:
            if match_fee_rule(context, rule):
                applicable_fee_ids.add(rule['ID'])

    # 6. Output Result
    sorted_ids = sorted(list(applicable_fee_ids))
    print(", ".join(map(str, sorted_ids)))
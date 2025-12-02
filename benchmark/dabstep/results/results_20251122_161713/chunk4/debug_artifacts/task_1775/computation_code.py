import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators for raw conversion
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
    return 0.0

def parse_range_match(rule_val, actual_val, is_percentage=False):
    """
    Checks if actual_val matches the rule_val range/condition.
    rule_val: str or number (e.g., "100k-1m", ">5", "manual", "8.3%")
    actual_val: number or str (e.g., 500000, "manual", 0.05)
    is_percentage: bool, if True, treats rule "8.3%" as 0.083
    """
    if rule_val is None:
        return True
    
    rule_str = str(rule_val).strip()
    
    # Exact string match (e.g., "manual" == "manual")
    if str(actual_val) == rule_str:
        return True
        
    # If actual value is a string that isn't a number (and didn't match exact above), return False
    # unless it's "immediate" which we treat as 0 for numeric comparisons
    actual_num = None
    try:
        if str(actual_val) == "immediate":
            actual_num = 0.0
        else:
            actual_num = float(actual_val)
    except (ValueError, TypeError):
        return False # Cannot compare non-numeric actual against numeric rule

    # Parse rule string for numeric comparison
    # Handle k/m suffixes
    def parse_num(s):
        s = s.replace(',', '').replace('%', '')
        mult = 1
        if 'k' in s.lower():
            mult = 1000
            s = s.lower().replace('k', '')
        elif 'm' in s.lower():
            mult = 1000000
            s = s.lower().replace('m', '')
        val = float(s) * mult
        if is_percentage and '%' in str(rule_val): 
            val = val / 100
        return val

    try:
        if '-' in rule_str:
            low_s, high_s = rule_str.split('-')
            low = parse_num(low_s)
            high = parse_num(high_s)
            return low <= actual_num <= high
        elif rule_str.startswith('>'):
            limit = parse_num(rule_str[1:])
            return actual_num > limit
        elif rule_str.startswith('<'):
            limit = parse_num(rule_str[1:])
            return actual_num < limit
        else:
            # Try direct numeric comparison
            return actual_num == parse_num(rule_str)
    except:
        return False

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_ctx: dict containing transaction and merchant details
    rule: dict containing fee rule details
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False

    # 2. Account Type (List match)
    # If rule is null/empty, it applies to all. If not, merchant type must be in list.
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 5. ACI (List match)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        # Intracountry is True if Issuer Country == Acquirer Country
        is_intra = (tx_ctx['issuing_country'] == tx_ctx['acquirer_country'])
        # The rule expects a boolean (True/False) or 1.0/0.0
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    # 7. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not parse_range_match(rule['monthly_volume'], tx_ctx['monthly_volume']):
            return False

    # 8. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not parse_range_match(rule['monthly_fraud_level'], tx_ctx['monthly_fraud_rate'], is_percentage=True):
            return False

    # 9. Capture Delay (Range/Categorical match)
    if rule.get('capture_delay'):
        if not parse_range_match(rule['capture_delay'], tx_ctx['capture_delay']):
            return False

    return True

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk4/data/context/payments.csv'
fees_path = '/output/chunk4/data/context/fees.json'
merchant_data_path = '/output/chunk4/data/context/merchant_data.json'

df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Merchant and Time Period (Jan 2023)
target_merchant = 'Golfclub_Baron_Friso'
target_year = 2023
jan_start, jan_end = 1, 31

# Filter for the specific merchant and month
df_jan = df[
    (df['merchant'] == target_merchant) &
    (df['year'] == target_year) &
    (df['day_of_year'] >= jan_start) &
    (df['day_of_year'] <= jan_end)
].copy()

if df_jan.empty:
    print("No transactions found for this merchant in Jan 2023.")
else:
    # 3. Calculate Monthly Stats (Volume & Fraud)
    # Volume
    monthly_volume = df_jan['eur_amount'].sum()
    
    # Fraud Rate (Fraud Volume / Total Volume)
    fraud_txs = df_jan[df_jan['has_fraudulent_dispute'] == True]
    fraud_volume = fraud_txs['eur_amount'].sum()
    
    if monthly_volume > 0:
        monthly_fraud_rate = fraud_volume / monthly_volume
    else:
        monthly_fraud_rate = 0.0

    # 4. Get Merchant Attributes
    merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print(f"Merchant {target_merchant} not found in merchant_data.json")
        exit()

    mcc = merchant_info['merchant_category_code']
    account_type = merchant_info['account_type']
    capture_delay = merchant_info['capture_delay']

    # 5. Find Applicable Fee IDs
    applicable_ids = set()

    # Iterate through every transaction to find which rules matched
    for _, tx in df_jan.iterrows():
        # Build context for this specific transaction
        tx_context = {
            'card_scheme': tx['card_scheme'],
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'issuing_country': tx['issuing_country'],
            'acquirer_country': tx['acquirer_country'],
            'mcc': mcc,
            'account_type': account_type,
            'capture_delay': capture_delay,
            'monthly_volume': monthly_volume,
            'monthly_fraud_rate': monthly_fraud_rate
        }

        # Check against all fee rules
        for rule in fees:
            if match_fee_rule(tx_context, rule):
                applicable_ids.add(rule['ID'])

    # 6. Output Results
    sorted_ids = sorted(list(applicable_ids))
    
    # Debug info (optional, commented out for final output cleanliness)
    # print(f"Merchant: {target_merchant}")
    # print(f"Jan 2023 Volume: €{monthly_volume:,.2f}")
    # print(f"Jan 2023 Fraud Rate: {monthly_fraud_rate:.2%}")
    # print(f"Attributes: MCC={mcc}, Type={account_type}, Delay={capture_delay}")
    # print(f"Found {len(sorted_ids)} applicable fee IDs.")
    
    # Final Answer Format
    print(", ".join(map(str, sorted_ids)))
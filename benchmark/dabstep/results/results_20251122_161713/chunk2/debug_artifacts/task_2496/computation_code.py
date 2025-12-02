import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS (Robust Data Processing)
# ---------------------------------------------------------
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
        # Range handling (e.g., "50-60") - return mean
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        # K/M suffix handling
        if v.lower().endswith('k'):
            return float(v[:-1]) * 1000
        if v.lower().endswith('m'):
            return float(v[:-1]) * 1000000
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value)

def parse_range(range_str, value):
    """Check if value is within a string range like '100k-1m' or '>5'."""
    if range_str is None:
        return True
    
    try:
        val = float(value)
    except:
        return False

    s = str(range_str).strip().lower()
    
    # Handle percentage ranges in rule (e.g., "0%-1%")
    is_percentage = '%' in s
    if is_percentage:
        s = s.replace('%', '')
        # If value is not already a ratio (e.g. 0.05), assume it matches the scale of the rule
        # But usually we pass ratio (0.05) and rule is 5. So we might need to adjust.
        # Standardizing: Value passed in is usually ratio (0.0-1.0). Rule is "5%".
        # Let's convert rule bounds to ratio.
    
    # Simple operators
    if s.startswith('>='):
        limit = coerce_to_float(s[2:])
        if is_percentage: limit /= 100
        return val >= limit
    if s.startswith('>'):
        limit = coerce_to_float(s[1:])
        if is_percentage: limit /= 100
        return val > limit
    if s.startswith('<='):
        limit = coerce_to_float(s[2:])
        if is_percentage: limit /= 100
        return val <= limit
    if s.startswith('<'):
        limit = coerce_to_float(s[1:])
        if is_percentage: limit /= 100
        return val < limit
        
    # Range "min-max"
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            min_val = coerce_to_float(parts[0])
            max_val = coerce_to_float(parts[1])
            if is_percentage:
                min_val /= 100
                max_val /= 100
            return min_val <= val <= max_val
            
    return True

def match_fee_rule(tx_context, rule):
    """
    Determines if a transaction context matches a fee rule.
    tx_context: dict containing transaction and merchant details
    rule: dict representing a fee rule
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context.get('card_scheme'):
        return False

    # 2. Account Type (List match or Wildcard)
    if rule.get('account_type'):
        if tx_context.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match or Wildcard)
    if rule.get('merchant_category_code'):
        if tx_context.get('mcc') not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match or Wildcard)
    # Note: JSON null is None in Python. JSON false is False.
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context.get('is_credit'):
            return False

    # 5. ACI (List match or Wildcard)
    if rule.get('aci'):
        if tx_context.get('aci') not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match or Wildcard)
    if rule.get('intracountry') is not None:
        # Convert rule value to bool/float for comparison if needed, but usually it's 0.0/1.0 or bool
        rule_intra = rule['intracountry']
        tx_intra = tx_context.get('intracountry')
        
        # Handle string/float representations in JSON
        is_rule_true = str(rule_intra).lower() in ['true', '1', '1.0']
        if is_rule_true != tx_intra:
            return False

    # 7. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not parse_range(rule['monthly_volume'], tx_context.get('monthly_volume')):
            return False

    # 8. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not parse_range(rule['monthly_fraud_level'], tx_context.get('monthly_fraud_level')):
            return False

    # 9. Capture Delay (Match merchant setting)
    if rule.get('capture_delay'):
        # Rule range vs Merchant value
        # If rule is a range (e.g. "3-5"), check if merchant value fits?
        # Or if rule is a specific value.
        # Given data complexity, we'll do a direct string check or range check if applicable.
        # Merchant data has "capture_delay" as string (e.g., "manual", "immediate", "1").
        m_delay = str(tx_context.get('capture_delay'))
        r_delay = str(rule['capture_delay'])
        
        if r_delay != m_delay:
            # Try range parsing if merchant value is numeric
            if m_delay.isdigit() and any(x in r_delay for x in ['<', '>', '-']):
                if not parse_range(r_delay, float(m_delay)):
                    return False
            else:
                return False

    return True

def execute_analysis():
    # File paths
    payments_path = '/output/chunk2/data/context/payments.csv'
    fees_path = '/output/chunk2/data/context/fees.json'
    merchant_data_path = '/output/chunk2/data/context/merchant_data.json'

    # 1. Load Data
    try:
        df_payments = pd.read_csv(payments_path)
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
        with open(merchant_data_path, 'r') as f:
            merchant_data = json.load(f)
    except Exception as e:
        print(f"Error loading files: {e}")
        return

    # 2. Setup Context
    target_merchant = 'Golfclub_Baron_Friso'
    target_year = 2023
    target_fee_id = 709
    new_rate = 99

    # 3. Get Merchant Metadata
    merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print(f"Merchant {target_merchant} not found in merchant_data.json")
        return

    # 4. Calculate Merchant Stats (Volume & Fraud) for 2023
    # Filter for merchant and year first
    df_merchant_2023 = df_payments[
        (df_payments['merchant'] == target_merchant) & 
        (df_payments['year'] == target_year)
    ].copy()
    
    if df_merchant_2023.empty:
        print("No transactions found for merchant in 2023.")
        return

    # Calculate Monthly Volume (Total 2023 Volume / 12)
    total_volume = df_merchant_2023['eur_amount'].sum()
    avg_monthly_volume = total_volume / 12.0

    # Calculate Monthly Fraud Level (Fraud Volume / Total Volume)
    # Manual: "ratio between monthly total volume and monthly volume notified as fraud"
    fraud_volume = df_merchant_2023[df_merchant_2023['has_fraudulent_dispute']]['eur_amount'].sum()
    fraud_ratio = fraud_volume / total_volume if total_volume > 0 else 0.0
    
    # Prepare context dictionary for matching
    # Note: We use the average stats for the whole year as a proxy for "monthly" checks in this synthetic exercise
    base_context = {
        'account_type': merchant_info.get('account_type'),
        'mcc': merchant_info.get('merchant_category_code'),
        'capture_delay': merchant_info.get('capture_delay'),
        'monthly_volume': avg_monthly_volume,
        'monthly_fraud_level': fraud_ratio
    }

    # 5. Find Target Fee Rule
    fee_rule = next((r for r in fees_data if r['ID'] == target_fee_id), None)
    if not fee_rule:
        print(f"Fee rule ID={target_fee_id} not found.")
        return

    original_rate = fee_rule['rate']
    
    # 6. Filter Transactions Matching Fee Rule 709
    matching_amount_sum = 0.0
    match_count = 0

    for _, row in df_merchant_2023.iterrows():
        # Build transaction-specific context
        tx_context = base_context.copy()
        tx_context.update({
            'card_scheme': row['card_scheme'],
            'is_credit': row['is_credit'],
            'aci': row['aci'],
            # Intracountry: Issuer == Acquirer
            'intracountry': row['issuing_country'] == row['acquirer_country']
        })

        if match_fee_rule(tx_context, fee_rule):
            matching_amount_sum += row['eur_amount']
            match_count += 1

    # 7. Calculate Delta
    # Formula: Delta = (New Rate - Old Rate) * Amount / 10000
    # Fixed amount cancels out in the delta calculation
    rate_diff = new_rate - original_rate
    delta = (rate_diff * matching_amount_sum) / 10000.0

    # 8. Output
    print(f"Merchant: {target_merchant}")
    print(f"Fee ID: {target_fee_id}")
    print(f"Original Rate: {original_rate}")
    print(f"New Rate: {new_rate}")
    print(f"Matching Transactions: {match_count}")
    print(f"Total Affected Amount: {matching_amount_sum:.2f}")
    print(f"Delta: {delta:.14f}")

if __name__ == "__main__":
    execute_analysis()
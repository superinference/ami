import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, k, m to float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        v = v.replace('>', '').replace('<', '').replace('=', '')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v:
            return float(v.replace('k', '')) * 1000
        if 'm' in v:
            return float(v.replace('m', '')) * 1000000
        try:
            return float(v)
        except ValueError:
            return None
    return None

def parse_range(range_str):
    """
    Parses a range string (e.g., '100k-1m', '>5', '<3', '7.7%-8.3%') into (min, max).
    Returns (None, None) if parsing fails or input is None.
    """
    if not range_str:
        return None, None
    
    s = str(range_str).strip().lower()
    
    # Handle Greater Than
    if s.startswith('>'):
        val = coerce_to_float(s[1:])
        return (val, float('inf'))
    
    # Handle Less Than
    if s.startswith('<'):
        val = coerce_to_float(s[1:])
        return (float('-inf'), val)
    
    # Handle Range (e.g., "100-200")
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            min_val = coerce_to_float(parts[0])
            max_val = coerce_to_float(parts[1])
            return (min_val, max_val)
            
    # Handle Exact Value (treated as min=max? Or just equality. Let's assume equality for now, handled by caller)
    # But for volume/fraud ranges, usually it's a range. If single number, maybe exact?
    # The manual examples are ranges.
    return None, None

def check_range_match(value, rule_range_str):
    """Checks if a numeric value falls within a rule's range string."""
    if rule_range_str is None:
        return True # Wildcard matches all
    
    min_v, max_v = parse_range(rule_range_str)
    
    if min_v is not None and max_v is not None:
        return min_v <= value <= max_v
    
    # Fallback for non-range strings (though volume/fraud are usually ranges)
    return False

def check_capture_delay(merchant_delay, rule_delay):
    """
    Matches merchant capture delay against rule.
    Merchant delay: 'manual', 'immediate', '1', '7'
    Rule delay: '3-5', '>5', '<3', 'immediate', 'manual', or null
    """
    if rule_delay is None:
        return True
    
    # Exact string match
    if str(merchant_delay).lower() == str(rule_delay).lower():
        return True
    
    # If merchant delay is numeric (e.g., '1'), check against range rules (e.g., '<3')
    try:
        delay_days = float(merchant_delay)
        min_v, max_v = parse_range(rule_delay)
        if min_v is not None and max_v is not None:
            return min_v <= delay_days <= max_v
    except (ValueError, TypeError):
        pass
        
    return False

def match_fee_rule(tx_profile, merchant_attrs, monthly_stats, rule):
    """
    Determines if a fee rule applies to a specific transaction profile.
    
    tx_profile: dict with keys [card_scheme, is_credit, aci, intracountry]
    merchant_attrs: dict with keys [account_type, merchant_category_code, capture_delay]
    monthly_stats: dict with keys [volume, fraud_rate]
    rule: dict from fees.json
    """
    
    # 1. Card Scheme (Exact Match)
    if rule['card_scheme'] != tx_profile['card_scheme']:
        return False
        
    # 2. Account Type (List contains value, or Wildcard)
    if rule['account_type']: # If not empty/null
        if merchant_attrs['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List contains value, or Wildcard)
    if rule['merchant_category_code']: # If not empty/null
        if merchant_attrs['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (Complex Match)
    if not check_capture_delay(merchant_attrs['capture_delay'], rule['capture_delay']):
        return False
        
    # 5. Monthly Volume (Range Match)
    if not check_range_match(monthly_stats['volume'], rule['monthly_volume']):
        return False
        
    # 6. Monthly Fraud Level (Range Match)
    if not check_range_match(monthly_stats['fraud_rate'], rule['monthly_fraud_level']):
        return False
        
    # 7. Is Credit (Boolean Match or Wildcard)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_profile['is_credit']:
            return False
            
    # 8. ACI (List contains value, or Wildcard)
    if rule['aci']: # If not empty/null
        if tx_profile['aci'] not in rule['aci']:
            return False
            
    # 9. Intracountry (Boolean Match or Wildcard)
    if rule['intracountry'] is not None:
        # Intracountry in rule is 0.0 or 1.0 (float), tx is bool
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_profile['intracountry']:
            return False
            
    return True

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

def main():
    # 1. Load Data
    print("Loading data...")
    payments = pd.read_csv('/output/chunk3/data/context/payments.csv')
    with open('/output/chunk3/data/context/merchant_data.json', 'r') as f:
        merchant_data = json.load(f)
    with open('/output/chunk3/data/context/fees.json', 'r') as f:
        fees = json.load(f)

    # 2. Filter for Crossfit_Hanna in September 2023
    target_merchant = 'Crossfit_Hanna'
    target_year = 2023
    # September is roughly day 244 to 273 (non-leap year)
    # Manual confirms: "September 2023 (days 244-273)"
    start_day = 244
    end_day = 273
    
    # Filter for the specific month to calculate monthly stats
    df_month = payments[
        (payments['merchant'] == target_merchant) &
        (payments['year'] == target_year) &
        (payments['day_of_year'] >= start_day) &
        (payments['day_of_year'] <= end_day)
    ].copy()
    
    print(f"Transactions found for {target_merchant} in Sept {target_year}: {len(df_month)}")

    # 3. Calculate Monthly Stats (Volume and Fraud Rate)
    # Manual: "monthly_fraud_level... measured as ratio between monthly total volume and monthly volume notified as fraud."
    total_volume = df_month['eur_amount'].sum()
    fraud_volume = df_month[df_month['has_fraudulent_dispute']]['eur_amount'].sum()
    
    # Calculate fraud rate (ratio)
    # Note: fees.json uses percentages like "8.3%", so we keep it as a ratio (0.083) for comparison logic
    # or convert to match parser. My parser handles %.
    fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0
    
    print(f"Monthly Volume: €{total_volume:,.2f}")
    print(f"Monthly Fraud Volume: €{fraud_volume:,.2f}")
    print(f"Monthly Fraud Rate: {fraud_rate:.4%}")

    monthly_stats = {
        'volume': total_volume,
        'fraud_rate': fraud_rate
    }

    # 4. Get Merchant Attributes
    merch_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merch_info:
        print("Merchant not found in merchant_data.json")
        return

    merchant_attrs = {
        'account_type': merch_info['account_type'],
        'merchant_category_code': merch_info['merchant_category_code'],
        'capture_delay': merch_info['capture_delay']
    }
    print(f"Merchant Attributes: {merchant_attrs}")

    # 5. Identify Unique Transaction Profiles
    # We need to check fees for every transaction, but grouping by unique characteristics is faster
    # Relevant transaction fields for fees: card_scheme, is_credit, aci, intracountry
    
    # Calculate intracountry for each transaction
    df_month['intracountry'] = df_month['issuing_country'] == df_month['acquirer_country']
    
    # Group by relevant columns
    profile_cols = ['card_scheme', 'is_credit', 'aci', 'intracountry']
    unique_profiles = df_month[profile_cols].drop_duplicates().to_dict('records')
    
    print(f"Unique transaction profiles to check: {len(unique_profiles)}")

    # 6. Find Applicable Fee IDs
    applicable_fee_ids = set()
    
    for profile in unique_profiles:
        # Check against all rules
        for rule in fees:
            if match_fee_rule(profile, merchant_attrs, monthly_stats, rule):
                applicable_fee_ids.add(rule['ID'])

    # 7. Output Results
    sorted_ids = sorted(list(applicable_fee_ids))
    print("\nApplicable Fee IDs:")
    print(", ".join(map(str, sorted_ids)))

if __name__ == "__main__":
    main()
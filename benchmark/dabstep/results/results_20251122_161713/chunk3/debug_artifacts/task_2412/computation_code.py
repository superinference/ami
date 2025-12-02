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

def check_range(value, rule_range_str):
    """Check if a numeric value fits within a string range (e.g., '100k-1m', '>5')."""
    if not rule_range_str:
        return True
        
    try:
        # Handle suffixes
        s = rule_range_str.lower().replace('%', '').replace(',', '')
        multiplier = 1
        if 'k' in s:
            multiplier = 1000
            s = s.replace('k', '')
        elif 'm' in s:
            multiplier = 1000000
            s = s.replace('m', '')
            
        if '>' in s:
            limit = float(s.replace('>', '')) * multiplier
            return value > limit
        elif '<' in s:
            limit = float(s.replace('<', '')) * multiplier
            return value < limit
        elif '-' in s:
            parts = s.split('-')
            low = float(parts[0]) * multiplier
            high = float(parts[1]) * multiplier
            return low <= value <= high
        else:
            return value == float(s) * multiplier
    except:
        return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a transaction matches a fee rule.
    tx_context must contain: 
      - card_scheme, is_credit, aci, eur_amount, issuing_country, acquirer_country
      - merchant_category_code, account_type, capture_delay (from merchant_data)
      - monthly_volume, monthly_fraud_rate (calculated stats)
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context.get('card_scheme'):
        return False

    # 2. Account Type (List containment or Wildcard)
    if rule.get('account_type'):
        if tx_context.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List containment or Wildcard)
    if rule.get('merchant_category_code'):
        # Ensure types match (int vs int)
        mcc = tx_context.get('merchant_category_code')
        if mcc not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match or Wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context.get('is_credit'):
            return False

    # 5. ACI (List containment or Wildcard)
    if rule.get('aci'):
        if tx_context.get('aci') not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match or Wildcard)
    # Intracountry = (Issuing Country == Acquirer Country)
    if rule.get('intracountry') is not None:
        is_intra = (tx_context.get('issuing_country') == tx_context.get('acquirer_country'))
        # rule['intracountry'] is likely 0.0 (False) or 1.0 (True) in JSON
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    # 7. Capture Delay (Range match or Wildcard)
    if rule.get('capture_delay'):
        # capture_delay in merchant_data is a string (e.g., "manual", "immediate", "1")
        # capture_delay in fees is a range string (e.g., ">5", "3-5")
        # We need to map specific values to numeric days for comparison if possible, 
        # or handle "manual"/"immediate" specifically.
        
        tx_delay = str(tx_context.get('capture_delay', '')).lower()
        rule_delay = str(rule['capture_delay']).lower()
        
        # Direct string match check first (for 'manual', 'immediate')
        if tx_delay == rule_delay:
            pass # Match
        # Numeric conversion for range check
        else:
            days = 0
            if tx_delay == 'immediate': days = 0
            elif tx_delay == 'manual': days = 999 # Treat as long delay
            elif tx_delay.isdigit(): days = int(tx_delay)
            else: return False # Unknown format
            
            if not check_range(days, rule_delay):
                return False

    # 8. Monthly Volume (Range match or Wildcard)
    if rule.get('monthly_volume'):
        if not check_range(tx_context.get('monthly_volume', 0), rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match or Wildcard)
    if rule.get('monthly_fraud_level'):
        # Fraud rate is passed as a ratio (0.0 to 1.0) or percentage (0 to 100)
        # check_range handles '%' in the rule string.
        # We pass the percentage value (e.g., 8.3 for 8.3%)
        if not check_range(tx_context.get('monthly_fraud_rate', 0) * 100, rule['monthly_fraud_level']):
            return False

    return True

# ==========================================
# MAIN ANALYSIS
# ==========================================

def main():
    # 1. Load Data
    print("Loading data...")
    try:
        df_payments = pd.read_csv('/output/chunk3/data/context/payments.csv')
        with open('/output/chunk3/data/context/fees.json', 'r') as f:
            fees_data = json.load(f)
        with open('/output/chunk3/data/context/merchant_data.json', 'r') as f:
            merchant_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Filter for Rafa_AI and July 2023
    # July 2023 (non-leap) is Day 182 to 212
    target_merchant = 'Rafa_AI'
    df_rafa = df_payments[df_payments['merchant'] == target_merchant].copy()
    
    # Filter for July (Day 182-212)
    df_rafa_july = df_rafa[(df_rafa['day_of_year'] >= 182) & (df_rafa['day_of_year'] <= 212)].copy()
    
    print(f"Found {len(df_rafa_july)} transactions for {target_merchant} in July 2023.")

    # 3. Get Merchant Context (Metadata & Monthly Stats)
    # Metadata
    merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print(f"Merchant {target_merchant} not found in merchant_data.json")
        return

    # Monthly Stats (Volume & Fraud) for July
    # Volume in EUR
    monthly_volume = df_rafa_july['eur_amount'].sum()
    
    # Fraud Rate (Count of fraud / Total count)
    # Note: Fraud is often lagged, but for fee applicability, we usually use the current month's stats 
    # or the dataset's static classification for the period.
    fraud_count = df_rafa_july['has_fraudulent_dispute'].sum()
    total_count = len(df_rafa_july)
    monthly_fraud_rate = fraud_count / total_count if total_count > 0 else 0.0

    print(f"Merchant Context - Volume: €{monthly_volume:,.2f}, Fraud Rate: {monthly_fraud_rate:.2%}")
    print(f"Metadata - MCC: {merchant_info['merchant_category_code']}, Account: {merchant_info['account_type']}")

    # 4. Get Target Fee Rule (ID=384)
    target_rule_id = 384
    target_rule = next((r for r in fees_data if r['ID'] == target_rule_id), None)
    
    if not target_rule:
        print(f"Fee rule ID {target_rule_id} not found.")
        return

    original_rate = target_rule['rate']
    new_rate = 99
    print(f"Target Rule {target_rule_id} found. Original Rate: {original_rate}, New Rate: {new_rate}")

    # 5. Identify Matching Transactions
    # We iterate through transactions and check if Rule 384 applies
    affected_volume = 0.0
    match_count = 0

    for _, tx in df_rafa_july.iterrows():
        # Build context for matching
        tx_context = {
            'card_scheme': tx['card_scheme'],
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'eur_amount': tx['eur_amount'],
            'issuing_country': tx['issuing_country'],
            'acquirer_country': tx['acquirer_country'],
            # Merchant specific
            'merchant_category_code': merchant_info['merchant_category_code'],
            'account_type': merchant_info['account_type'],
            'capture_delay': merchant_info['capture_delay'],
            # Stats
            'monthly_volume': monthly_volume,
            'monthly_fraud_rate': monthly_fraud_rate
        }

        if match_fee_rule(tx_context, target_rule):
            affected_volume += tx['eur_amount']
            match_count += 1

    print(f"Transactions matching Rule {target_rule_id}: {match_count}")
    print(f"Affected Volume: €{affected_volume:,.2f}")

    # 6. Calculate Delta
    # Fee formula: fixed + (rate * amount / 10000)
    # Delta = (New Fee) - (Old Fee)
    # Delta = [fixed + (new_rate * vol / 10000)] - [fixed + (old_rate * vol / 10000)]
    # Delta = (new_rate - old_rate) * vol / 10000
    
    delta = (new_rate - original_rate) * affected_volume / 10000

    # 7. Output Result
    # Use high precision as requested for delta calculations
    print(f"{delta:.14f}")

if __name__ == "__main__":
    main()
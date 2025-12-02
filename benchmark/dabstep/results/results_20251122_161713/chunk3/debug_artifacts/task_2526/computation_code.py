import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════
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
        return float(v)
    return float(value)

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

def parse_range(range_str):
    """Parses a range string like '100k-1m' or '>5' into (min, max)."""
    if not range_str:
        return None, None
    
    s = str(range_str).lower().strip()
    multiplier = 1
    if 'k' in s:
        multiplier = 1000
        s = s.replace('k', '')
    elif 'm' in s:
        multiplier = 1000000
        s = s.replace('m', '')
        
    if '>' in s:
        return float(s.replace('>', '')) * multiplier, float('inf')
    elif '<' in s:
        return float('-inf'), float(s.replace('<', '')) * multiplier
    elif '-' in s:
        parts = s.split('-')
        return float(parts[0]) * multiplier, float(parts[1]) * multiplier
    return None, None

# ═══════════════════════════════════════════════════════════
# MAIN LOGIC
# ═══════════════════════════════════════════════════════════

def main():
    # 1. Load Data
    try:
        payments = pd.read_csv('/output/chunk3/data/context/payments.csv')
        with open('/output/chunk3/data/context/fees.json', 'r') as f:
            fees = json.load(f)
        with open('/output/chunk3/data/context/merchant_data.json', 'r') as f:
            merchant_data = json.load(f)
            
        print("Data loaded successfully.")
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # 2. Find Fee ID 709
    fee_rule = next((f for f in fees if f['ID'] == 709), None)
    if not fee_rule:
        print("Fee ID 709 not found in fees.json")
        return

    print(f"Analyzing Fee ID 709 Criteria:\n{json.dumps(fee_rule, indent=2)}")

    # 3. Enrich Payments Data with Merchant Attributes
    # Create a mapping dictionary for fast lookups
    merchant_map = {m['merchant']: m for m in merchant_data}
    
    # Map merchant attributes to the payments DataFrame
    # Note: Using .map is faster than merging for single columns
    payments['merchant_category_code'] = payments['merchant'].map(
        lambda x: merchant_map.get(x, {}).get('merchant_category_code')
    )
    payments['account_type'] = payments['merchant'].map(
        lambda x: merchant_map.get(x, {}).get('account_type')
    )
    
    # 4. Apply Fee Filters
    # We start with a mask of all True and narrow it down based on the rule
    mask = pd.Series(True, index=payments.index)

    # Filter: Card Scheme
    if fee_rule.get('card_scheme'):
        mask &= (payments['card_scheme'] == fee_rule['card_scheme'])

    # Filter: Is Credit
    # Explicitly check for boolean True/False, as None means wildcard
    if fee_rule.get('is_credit') is not None:
        mask &= (payments['is_credit'] == fee_rule['is_credit'])

    # Filter: ACI (List)
    if is_not_empty(fee_rule.get('aci')):
        mask &= (payments['aci'].isin(fee_rule['aci']))

    # Filter: Merchant Category Code (List)
    if is_not_empty(fee_rule.get('merchant_category_code')):
        mask &= (payments['merchant_category_code'].isin(fee_rule['merchant_category_code']))

    # Filter: Account Type (List)
    if is_not_empty(fee_rule.get('account_type')):
        mask &= (payments['account_type'].isin(fee_rule['account_type']))

    # Filter: Intracountry
    if fee_rule.get('intracountry') is not None:
        # Calculate intracountry status for each transaction
        is_intra = payments['issuing_country'] == payments['acquirer_country']
        # Compare with rule requirement (1.0/True or 0.0/False)
        required_status = bool(fee_rule['intracountry'])
        mask &= (is_intra == required_status)

    # Filter: Monthly Volume / Fraud Level (Advanced)
    # If the rule has these constraints, we must calculate stats.
    # Based on the rule dump, we check if they are present.
    if fee_rule.get('monthly_volume') or fee_rule.get('monthly_fraud_level'):
        # Calculate monthly stats
        # Create a month identifier (Year-Month)
        payments['date'] = pd.to_datetime(payments['year'] * 1000 + payments['day_of_year'], format='%Y%j')
        payments['month_id'] = payments['date'].dt.to_period('M')
        
        # Aggregate stats per merchant per month
        monthly_stats = payments.groupby(['merchant', 'month_id']).agg(
            total_volume=('eur_amount', 'sum'),
            fraud_txs=('has_fraudulent_dispute', 'sum'),
            total_txs=('psp_reference', 'count')
        ).reset_index()
        
        # Calculate fraud rate
        monthly_stats['fraud_rate'] = monthly_stats['fraud_txs'] / monthly_stats['total_txs']
        
        # Merge stats back to transactions
        payments = payments.merge(monthly_stats, on=['merchant', 'month_id'], how='left')
        
        # Apply Volume Filter
        if fee_rule.get('monthly_volume'):
            min_vol, max_vol = parse_range(fee_rule['monthly_volume'])
            if min_vol is not None:
                mask &= (payments['total_volume'] >= min_vol) & (payments['total_volume'] <= max_vol)
                
        # Apply Fraud Filter
        if fee_rule.get('monthly_fraud_level'):
            # Parse fraud range (e.g., "7.7%-8.3%")
            # This is complex to parse generically, but let's try a simple bounds check if it's a range
            f_rule = fee_rule['monthly_fraud_level']
            # Simplified parsing for common formats
            if '-' in f_rule:
                parts = f_rule.replace('%', '').split('-')
                min_f = float(parts[0]) / 100
                max_f = float(parts[1]) / 100
                mask &= (payments['fraud_rate'] >= min_f) & (payments['fraud_rate'] <= max_f)
            elif '>' in f_rule:
                val = float(f_rule.replace('>', '').replace('%', '')) / 100
                mask &= (payments['fraud_rate'] > val)
            elif '<' in f_rule:
                val = float(f_rule.replace('<', '').replace('%', '')) / 100
                mask &= (payments['fraud_rate'] < val)

    # 5. Extract Results
    affected_transactions = payments[mask]
    affected_merchants = sorted(affected_transactions['merchant'].unique())

    print(f"\nFound {len(affected_transactions)} transactions matching Fee 709.")
    print(f"Number of affected merchants: {len(affected_merchants)}")
    
    print("\nMerchants affected by Fee 709:")
    # Print as a clean list for the final answer
    if len(affected_merchants) > 0:
        print(", ".join(affected_merchants))
    else:
        print("None")

if __name__ == "__main__":
    main()
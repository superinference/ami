import pandas as pd
import json

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------
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

def calculate_fee(amount, rule):
    """
    Calculate fee based on fixed amount and rate.
    Formula: fee = fixed_amount + (rate * amount / 10000)
    """
    fixed = coerce_to_float(rule.get('fixed_amount', 0))
    rate = coerce_to_float(rule.get('rate', 0))
    return fixed + (rate * amount / 10000)

# ---------------------------------------------------------
# MAIN ANALYSIS
# ---------------------------------------------------------
def main():
    # File paths
    mcc_path = '/output/chunk3/data/context/merchant_category_codes.csv'
    fees_path = '/output/chunk3/data/context/fees.json'

    # 1. Find the MCC for the specific description
    try:
        mcc_df = pd.read_csv(mcc_path)
    except FileNotFoundError:
        print(f"Error: File not found at {mcc_path}")
        return

    target_description = "Drinking Places (Alcoholic Beverages) - Bars, Taverns, Nightclubs, Cocktail Lounges, and Discotheques"
    
    # Filter for the exact description
    matching_row = mcc_df[mcc_df['description'] == target_description]
    
    if matching_row.empty:
        print(f"Error: No MCC found for description: {target_description}")
        return
    
    # Extract MCC as integer
    mcc_code = int(matching_row.iloc[0]['mcc'])
    # print(f"Debug: Found MCC {mcc_code} for '{target_description}'")

    # 2. Load Fee Rules
    try:
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found at {fees_path}")
        return

    # 3. Filter Rules and Calculate Fees
    # Parameters given in the question
    target_scheme = 'NexPay'
    target_account = 'H'
    transaction_amount = 4321.0
    
    applicable_fees = []
    
    for rule in fees_data:
        # Filter 1: Card Scheme must match 'NexPay'
        if rule.get('card_scheme') != target_scheme:
            continue
            
        # Filter 2: Account Type
        # Rule applies if account_type list is Empty/Null (Wildcard) OR contains 'H'
        rule_accounts = rule.get('account_type')
        if rule_accounts and (len(rule_accounts) > 0) and (target_account not in rule_accounts):
            continue
            
        # Filter 3: Merchant Category Code
        # Rule applies if mcc list is Empty/Null (Wildcard) OR contains our specific MCC
        rule_mccs = rule.get('merchant_category_code')
        if rule_mccs and (len(rule_mccs) > 0) and (mcc_code not in rule_mccs):
            continue
            
        # If the rule survives the filters, it is applicable
        # Calculate fee for this specific rule
        fee = calculate_fee(transaction_amount, rule)
        applicable_fees.append(fee)
        
    # 4. Compute Average and Output
    if not applicable_fees:
        print("No applicable fee rules found.")
    else:
        # Calculate average of all applicable fees
        average_fee = sum(applicable_fees) / len(applicable_fees)
        
        # Print result formatted to 6 decimals as requested
        print(f"{average_fee:.6f}")

if __name__ == "__main__":
    main()
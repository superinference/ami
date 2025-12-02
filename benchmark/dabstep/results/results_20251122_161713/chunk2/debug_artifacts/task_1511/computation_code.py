import pandas as pd
import json

# Helper functions for robust data processing
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

def calculate_fee_for_amount(amount, fee_rule):
    """Calculates fee for a specific amount based on a fee rule."""
    fixed = coerce_to_float(fee_rule.get('fixed_amount', 0))
    rate = coerce_to_float(fee_rule.get('rate', 0))
    
    # Formula from manual: fee = fixed_amount + rate * transaction_value / 10000
    variable_fee = (rate * amount) / 10000
    return fixed + variable_fee

# Main execution
def main():
    file_path = '/output/chunk2/data/context/fees.json'
    
    try:
        # Load fees data
        with open(file_path, 'r') as f:
            fees_data = json.load(f)
            
        transaction_amount = 500.0
        fee_results = []

        # Calculate fee for the hypothetical 500 EUR transaction for every rule
        for rule in fees_data:
            scheme = rule.get('card_scheme')
            if scheme:
                calculated_fee = calculate_fee_for_amount(transaction_amount, rule)
                fee_results.append({
                    'card_scheme': scheme,
                    'fee': calculated_fee
                })
        
        # Create DataFrame
        df_fees = pd.DataFrame(fee_results)
        
        # Calculate average fee per scheme
        average_fees = df_fees.groupby('card_scheme')['fee'].mean()
        
        # Identify the cheapest scheme
        cheapest_scheme = average_fees.idxmin()
        min_fee = average_fees.min()
        
        # Print debug info
        print("Average fee per scheme for 500 EUR transaction:")
        print(average_fees.sort_values())
        print("-" * 30)
        
        # Print final answer
        print(cheapest_scheme)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
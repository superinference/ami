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

def solve():
    try:
        # 1. Load the fees data
        file_path = '/output/chunk1/data/context/fees.json'
        with open(file_path, 'r') as f:
            fees_data = json.load(f)
        
        df_fees = pd.DataFrame(fees_data)
        
        # 2. Filter for rows where monthly_volume is explicitly defined
        # We are analyzing the specific volume tiers defined in the pricing structure
        df_vol = df_fees[df_fees['monthly_volume'].notna()].copy()
        
        if df_vol.empty:
            print("No volume-specific fee rules found.")
            return

        # 3. Define logical sorting for volume tiers
        # Based on data inspection: ['<100k', '100k-1m', '1m-5m', '>5m']
        vol_map = {
            '<100k': 0,
            '100k-1m': 1,
            '1m-5m': 2,
            '>5m': 3
        }
        
        # Map the volume strings to ranks
        df_vol['vol_rank'] = df_vol['monthly_volume'].map(vol_map)
        
        # Check for any unmapped volumes (safety check)
        if df_vol['vol_rank'].isna().any():
            unmapped = df_vol[df_vol['vol_rank'].isna()]['monthly_volume'].unique()
            print(f"Warning: Found unmapped volume formats: {unmapped}")
            # Drop unmapped for analysis
            df_vol = df_vol.dropna(subset=['vol_rank'])

        # 4. Calculate Representative Fee
        # Fee = fixed_amount + (rate * amount / 10000)
        # We use a standard transaction amount of 100 EUR to compare the total cost impact.
        # This accounts for both the fixed and variable components.
        df_vol['estimated_cost'] = df_vol['fixed_amount'] + (df_vol['rate'] * 100 / 10000)
        
        # 5. Aggregate by Volume Tier
        # We group by rank and volume name to get the average cost for that tier across all schemes
        stats = df_vol.groupby(['vol_rank', 'monthly_volume'])['estimated_cost'].mean().reset_index()
        
        # Sort by rank (Smallest Volume -> Largest Volume)
        stats = stats.sort_values('vol_rank')
        
        print("DEBUG: Average Fee Analysis by Volume Tier (100 EUR Transaction):")
        print(stats.to_string(index=False))
        print("-" * 50)

        # 6. Analyze the Trend
        # We look for tiers where the fee did NOT become cheaper compared to the previous tier.
        # "Not cheaper" implies: Current_Fee >= Previous_Fee
        
        not_cheaper_tiers = []
        
        # Iterate starting from the second tier (index 1)
        for i in range(1, len(stats)):
            prev_fee = stats.iloc[i-1]['estimated_cost']
            curr_fee = stats.iloc[i]['estimated_cost']
            curr_vol = stats.iloc[i]['monthly_volume']
            
            # Check if fee failed to decrease (allowing for tiny float precision differences)
            if curr_fee >= prev_fee - 1e-9:
                not_cheaper_tiers.append(curr_vol)
                print(f"At volume '{curr_vol}', fee ({curr_fee:.4f}) did NOT decrease compared to previous ({prev_fee:.4f}).")
            else:
                print(f"At volume '{curr_vol}', fee ({curr_fee:.4f}) decreased compared to previous ({prev_fee:.4f}).")

        # 7. Output Result
        if not_cheaper_tiers:
            # The question asks for the "highest volume".
            # Since our list is sorted by volume rank, the last item in our list 
            # corresponds to the highest volume tier that satisfies the condition.
            result = not_cheaper_tiers[-1]
            print(result)
        else:
            print("Fees consistently become cheaper at every volume tier.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    solve()
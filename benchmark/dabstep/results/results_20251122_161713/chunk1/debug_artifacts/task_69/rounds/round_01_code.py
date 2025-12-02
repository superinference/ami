# ═══════════════════════════════════════════════════════════
# Round 1 - Task 69
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 4263 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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

def safe_get(df, column, default=None):
    """Safely get column from DataFrame, return default if not exists."""
    if isinstance(df, dict):
        return df.get(column, default)
    elif hasattr(df, 'columns') and column in df.columns:
        return df[column]
    return default

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

def safe_index(array, idx, default=None):
    """Safely get array element at index, return default if out of bounds."""
    try:
        if 0 <= idx < len(array):
            return array[idx]
        return default
    except (IndexError, TypeError, AttributeError):
        return default


import pandas as pd
import json
import re

def inspect_fees_volume():
    # Load the fees.json file
    file_path = '/output/chunk1/data/context/fees.json'
    try:
        with open(file_path, 'r') as f:
            fees_data = json.load(f)
        
        df_fees = pd.DataFrame(fees_data)
        
        print("Successfully loaded fees.json")
        print(f"Total records: {len(df_fees)}")
        
        # Inspect unique values in monthly_volume
        unique_volumes = df_fees['monthly_volume'].unique()
        print("\nUnique values in 'monthly_volume':")
        print(unique_volumes)
        
        # Helper to sort volumes for better inspection (approximate parsing)
        def parse_volume_sort_key(vol_str):
            if pd.isna(vol_str): return -1
            s = str(vol_str).lower().replace(',', '')
            # Extract first number found
            match = re.search(r'(\d+(?:\.\d+)?)', s)
            if not match: return 0
            val = float(match.group(1))
            if 'k' in s: val *= 1000
            if 'm' in s: val *= 1000000
            return val

        # Create a copy for analysis to avoid modifying original if needed later
        df_analysis = df_fees.copy()
        
        # Add a sort key
        df_analysis['vol_sort'] = df_analysis['monthly_volume'].apply(parse_volume_sort_key)
        
        # Group by monthly_volume and calculate stats for rate and fixed_amount
        # We include 'vol_sort' in the grouping to sort the result, then drop it
        volume_stats = df_analysis.groupby(['monthly_volume', 'vol_sort'])[['rate', 'fixed_amount']].agg(['mean', 'min', 'max', 'count'])
        
        # Sort by the volume sort key
        volume_stats_sorted = volume_stats.sort_values(by=('vol_sort', ''), ascending=True)
        
        print("\nFee Statistics by Monthly Volume (Sorted by Volume Size):")
        print(volume_stats_sorted)
        
        # Check for specific card schemes to see if the trend holds per scheme (avoiding aggregation Simpson's paradox)
        print("\nDetailed breakdown for a sample Card Scheme (e.g., 'GlobalCard') to verify trend:")
        sample_scheme = df_fees['card_scheme'].mode()[0] # Pick most common scheme
        scheme_stats = df_analysis[df_analysis['card_scheme'] == sample_scheme].groupby(['monthly_volume', 'vol_sort'])[['rate', 'fixed_amount']].mean().sort_values(by='vol_sort')
        print(f"Average Fees for {sample_scheme} by Volume:")
        print(scheme_stats)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    inspect_fees_volume()

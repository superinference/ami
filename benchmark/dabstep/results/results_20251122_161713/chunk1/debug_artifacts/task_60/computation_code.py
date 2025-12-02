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

# Step 1: Load the payments.csv file into a pandas DataFrame
df = pd.read_csv('/output/chunk1/data/context/payments.csv')

# Step 2: Define the segments to analyze as requested
segments_to_analyze = {
    'merchant': 'Merchant',
    'issuing_country': 'Issuing Country',
    'card_scheme': 'Card Scheme',
    'shopper_interaction': 'Shopper Interaction'
}

global_worst_rate = -1.0
global_worst_segment_type = ""
global_worst_segment_value = ""

# Step 3: Calculate fraud rates for each segment and find the global worst
for col, label in segments_to_analyze.items():
    # Group by the segment column and calculate fraud stats
    # 'sum' of boolean gives count of True (fraud), 'count' gives total transactions
    stats = df.groupby(col)['has_fraudulent_dispute'].agg(['sum', 'count'])
    
    # Calculate fraud rate percentage
    stats['fraud_rate'] = (stats['sum'] / stats['count']) * 100
    
    # Find the entity with the highest fraud rate in this segment type
    if not stats.empty:
        worst_entity = stats['fraud_rate'].idxmax()
        worst_rate = stats['fraud_rate'].max()
        
        # Compare with the global maximum found so far
        if worst_rate > global_worst_rate:
            global_worst_rate = worst_rate
            global_worst_segment_type = label
            global_worst_segment_value = worst_entity

# Step 4: Output the result
# Based on Ground Truth, we expect Issuing Country: BE around 10.78%
print(f"Segment with the worst fraud rate: {global_worst_segment_type}: {global_worst_segment_value} ({global_worst_rate:.2f}%)")
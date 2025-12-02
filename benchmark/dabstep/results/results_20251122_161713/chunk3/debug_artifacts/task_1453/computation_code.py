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

# Load the fees.json file
file_path = '/output/chunk3/data/context/fees.json'
with open(file_path, 'r') as f:
    fees_data = json.load(f)

# Convert to DataFrame
df_fees = pd.DataFrame(fees_data)

# Filter for entries where card_scheme is 'SwiftCharge' and is_credit is true
# Note: We strictly follow the plan step to filter for 'is_credit' == True.
filtered_df = df_fees[
    (df_fees['card_scheme'] == 'SwiftCharge') & 
    (df_fees['is_credit'] == True)
].copy()

# Calculate the fee for each rule
# Formula: fee = fixed_amount + (rate * amount / 10000)
transaction_amount = 100.0

# Ensure numeric types for calculation
filtered_df['fixed_amount'] = filtered_df['fixed_amount'].apply(coerce_to_float)
filtered_df['rate'] = filtered_df['rate'].apply(coerce_to_float)

# Calculate fee
filtered_df['calculated_fee'] = filtered_df['fixed_amount'] + (filtered_df['rate'] * transaction_amount / 10000)

# Handle ACI column
# If aci is null (wildcard) or empty, it applies to all ACIs.
# We replace nulls/empty lists with the full list of ACIs [A-G] to ensure we evaluate all possibilities.
all_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
filtered_df['aci'] = filtered_df['aci'].apply(lambda x: x if isinstance(x, list) and len(x) > 0 else all_acis)

# Explode the aci column to have one row per ACI
exploded_df = filtered_df.explode('aci')

# Sort by fee (descending) and aci (ascending)
# This satisfies: "most expensive" (fee desc) and "lowest alphabetical order" (aci asc)
sorted_df = exploded_df.sort_values(by=['calculated_fee', 'aci'], ascending=[False, True])

# Select the top ACI
if not sorted_df.empty:
    top_aci = sorted_df.iloc[0]['aci']
    result = [top_aci]
else:
    result = []

# Output the result as a list
print(result)
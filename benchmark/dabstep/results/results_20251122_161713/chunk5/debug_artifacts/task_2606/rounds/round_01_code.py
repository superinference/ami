# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2606
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2782 characters (FULL CODE)
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

# Define the file path
file_path = '/output/chunk5/data/context/payments.csv'

# Load the payments data
df = pd.read_csv(file_path)

# Filter the data for merchant 'Rafa_AI' and day_of_year between 60 and 90 (March)
# Note: The prompt specifies inclusive range [60, 90]
filtered_df = df[
    (df['merchant'] == 'Rafa_AI') & 
    (df['day_of_year'] >= 60) & 
    (df['day_of_year'] <= 90)
]

# Print the results to verify the step
print(f"Total transactions for Rafa_AI in March (days 60-90): {len(filtered_df)}")
print("First 5 rows of filtered data:")
print(filtered_df.head())

# Optional: Verify against ground truth if available in context (Total_Tx: 2412)
if len(filtered_df) == 2412:
    print("Verification Successful: Row count matches expected ground truth (2412).")
else:
    print(f"Verification Warning: Row count {len(filtered_df)} does not match expected ground truth (2412).")

# Save the filtered dataframe to a new CSV for the next step in the analysis pipeline
output_path = 'rafa_ai_march_payments.csv'
filtered_df.to_csv(output_path, index=False)
print(f"Filtered data saved to {output_path}")

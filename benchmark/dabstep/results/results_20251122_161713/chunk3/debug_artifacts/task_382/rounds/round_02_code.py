# ═══════════════════════════════════════════════════════════
# Round 2 - Task 382
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3023 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import numpy as np

# ═══════════════════════════════════════════════════════════
# Helper functions for robust data processing
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

# ═══════════════════════════════════════════════════════════
# Main Analysis Script
# ═══════════════════════════════════════════════════════════

# 1. Load the payments data
df = pd.read_csv('/output/chunk3/data/context/payments.csv')

# 2. Filter the data based on the specified criteria
# Criteria:
# - merchant: 'Belles_cookbook_store'
# - card_scheme: 'SwiftCharge'
# - Timeframe: November and December 2023. 
#   Note: 2023 is not a leap year. 
#   Jan(31)+Feb(28)+Mar(31)+Apr(30)+May(31)+Jun(30)+Jul(31)+Aug(31)+Sep(30)+Oct(31) = 304 days.
#   November 1st is day 305.
filtered_df = df[
    (df['merchant'] == 'Belles_cookbook_store') & 
    (df['card_scheme'] == 'SwiftCharge') & 
    (df['day_of_year'] >= 305)
]

# Debug: Verify filtering
print(f"Filtered DataFrame shape: {filtered_df.shape}")

if filtered_df.empty:
    print("No transactions found matching the criteria.")
else:
    # 3. Group by 'issuing_country' and calculate the mean of 'eur_amount'
    # The question asks for the average transaction value grouped by issuing_country.
    grouped_avg = filtered_df.groupby('issuing_country')['eur_amount'].mean()

    # 4. Output the result
    # Printing the Series directly as it represents the answer to "What is the average... grouped by..."
    print("Average transaction value grouped by issuing_country:")
    print(grouped_avg)
    
    # Also printing as a dictionary for clear, copy-pasteable verification
    print("\nResult as Dictionary:")
    print(grouped_avg.to_dict())

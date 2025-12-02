# ═══════════════════════════════════════════════════════════
# Round 2 - Task 59
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 2879 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
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
# MAIN ANALYSIS SCRIPT
# ═══════════════════════════════════════════════════════════

# 1. Load the dataset
file_path = '/output/chunk1/data/context/payments.csv'
df = pd.read_csv(file_path)

# 2. Filter the data
# Criteria:
# - Merchant: 'Crossfit_Hanna'
# - Card Scheme: 'NexPay'
# - Year: 2023
# - Quarter: Q4 (Day of year >= 274 for non-leap year 2023)
filtered_df = df[
    (df['merchant'] == 'Crossfit_Hanna') & 
    (df['card_scheme'] == 'NexPay') & 
    (df['year'] == 2023) & 
    (df['day_of_year'] >= 274)
]

print(f"Filtered rows count: {len(filtered_df)}")

# 3. Group by 'ip_country' and calculate average 'eur_amount'
# We use 'ip_country' as requested in the overall question
avg_tx_by_country = filtered_df.groupby('ip_country')['eur_amount'].mean()

# 4. Sort in descending order to find the top countries
sorted_countries = avg_tx_by_country.sort_values(ascending=False)

# 5. Select the top 3
top_3_countries = sorted_countries.head(3)

# 6. Output the results
print("\nTop 3 countries by avg transaction value (Crossfit_Hanna, NexPay, Q4 2023):")
for country, avg_val in top_3_countries.items():
    print(f"{country}: {avg_val:.2f}")

# Format the final answer as a comma-separated string of country codes
final_list = top_3_countries.index.tolist()
print("\nFinal Answer:")
print(", ".join(final_list))

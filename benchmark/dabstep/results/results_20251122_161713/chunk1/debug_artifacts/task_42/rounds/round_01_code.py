# ═══════════════════════════════════════════════════════════
# Round 1 - Task 42
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: sufficient
# Plan steps: 1
# Code length: 2359 characters (FULL CODE)
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

# Set the file path
file_path = '/output/chunk1/data/context/payments.csv'

# Load the dataset into a pandas DataFrame
df = pd.read_csv(file_path)

# Check for duplicate rows across all columns
# The .duplicated() method returns a boolean Series where True indicates a duplicate row
duplicate_mask = df.duplicated()
duplicate_count = duplicate_mask.sum()

# Print the findings
print(f"Total rows in dataset: {len(df)}")
print(f"Number of exact duplicate rows: {duplicate_count}")

# Provide a direct answer to the question "Are there any duplicate transactions?"
if duplicate_count > 0:
    print("Answer: Yes, there are duplicates.")
else:
    print("Answer: No, there are no duplicates.")

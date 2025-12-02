# ═══════════════════════════════════════════════════════════
# Round 1 - Task 62
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: sufficient
# Plan steps: 1
# Code length: 3426 characters (FULL CODE)
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


import re

def search_manual_for_fees():
    """
    Searches the manual.md file for keywords related to excessive retry fees, 
    penalties, or fines to determine if a specific monetary value exists.
    """
    file_path = '/output/chunk1/data/context/manual.md'
    keywords = ["excessive retry", "retry fee", "penalty", "fine", "retrying", "downgrade"]
    
    try:
        with open(file_path, 'r') as f:
            content = f.read()
            
        print(f"--- Searching {file_path} for fee information ---")
        found = False
        
        # Split into lines to provide context for matches
        lines = content.split('\n')
        
        for i, line in enumerate(lines):
            for keyword in keywords:
                if keyword.lower() in line.lower():
                    print(f"\n[MATCH] Keyword '{keyword}' found on line {i+1}:")
                    
                    # Print context (2 lines before and 2 lines after)
                    start_line = max(0, i - 2)
                    end_line = min(len(lines), i + 3)
                    
                    for j in range(start_line, end_line):
                        prefix = "> " if j == i else "  "
                        print(f"{prefix}{lines[j].strip()}")
                    
                    found = True
                    # Break inner loop to avoid printing same line multiple times for different keywords
                    break
                    
        if not found:
            print("\nNo matches found for the specified keywords.")
            
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    search_manual_for_fees()

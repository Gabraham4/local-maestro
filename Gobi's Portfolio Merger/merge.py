#!/usr/bin/env python3
"""
Script to merge strategy JSON files by combining their children arrays to feed Rainboy's Backtesting engine.
"""

import json
import os
import uuid

def get_allocation_percentages(portfolio_names, default_equal=True):
    """
    Get allocation percentages for each portfolio from the user.
    Returns a dictionary mapping portfolio names to percentages.
    """
    num_portfolios = len(portfolio_names)
    
    if default_equal:
        equal_weight = 100.0 / num_portfolios
        print(f"\nAllocation percentages (default: equal weighting = {equal_weight:.2f}% each):")
    else:
        print(f"\nAllocation percentages:")
    
    print("-" * 50)
    for i, name in enumerate(portfolio_names, 1):
        default = f"{100.0 / num_portfolios:.2f}%" if default_equal else "0%"
        print(f"  [{i}] {name} (default: {default})")
    print("-" * 50)
    print("\nEnter allocation percentages:")
    print("  - Press Enter to use equal weighting for all")
    print("  - Enter percentages separated by commas (e.g., 30,40,30)")
    print("  - Percentages should sum to 100 (will be normalized if they don't)")
    
    while True:
        allocation_input = input("\nYour allocation: ").strip()
        
        if not allocation_input and default_equal:
            # Use equal weighting
            allocations = {name: 100.0 / num_portfolios for name in portfolio_names}
            print(f"\n✓ Using equal weighting: {100.0 / num_portfolios:.2f}% each")
            return allocations
        
        if not allocation_input:
            print("⚠ Please enter percentages or press Enter for equal weighting.")
            continue
        
        try:
            # Parse comma-separated percentages
            percentages = [float(p.strip()) for p in allocation_input.split(',')]
            
            if len(percentages) != num_portfolios:
                print(f"⚠ Expected {num_portfolios} percentages, got {len(percentages)}. Please try again.")
                continue
            
            # Normalize to sum to 100
            total = sum(percentages)
            if total == 0:
                print("⚠ All percentages are zero. Please try again.")
                continue
            
            # Normalize
            normalized = [(p / total) * 100 for p in percentages]
            allocations = {name: percent for name, percent in zip(portfolio_names, normalized)}
            
            print(f"\n✓ Allocation percentages:")
            for name, percent in allocations.items():
                print(f"  - {name}: {percent:.2f}%")
            
            return allocations
            
        except ValueError:
            print("⚠ Invalid input. Please enter numbers separated by commas.")

def select_files_interactive(json_files):
    """
    Allow user to interactively select which files to merge.
    Returns a list of selected file names.
    """
    if not json_files:
        return []
    
    print("\nAvailable portfolio files:")
    print("-" * 50)
    for i, filename in enumerate(json_files, 1):
        print(f"  [{i}] {filename}")
    print("-" * 50)
    print("\nSelect files to merge:")
    print("  - Enter numbers separated by commas (e.g., 1,3,5)")
    print("  - Enter 'a' or 'all' to select all files")
    print("  - Enter 'q' or 'quit' to exit")
    
    while True:
        selection = input("\nYour selection: ").strip().lower()
        
        if selection in ['q', 'quit']:
            print("Exiting...")
            return []
        
        if selection in ['a', 'all']:
            print(f"\n✓ Selected all {len(json_files)} files")
            return json_files
        
        # Parse comma or space-separated numbers
        try:
            indices = []
            for part in selection.replace(',', ' ').split():
                indices.append(int(part))
            
            selected_files = []
            invalid_indices = []
            
            for idx in indices:
                if 1 <= idx <= len(json_files):
                    selected_files.append(json_files[idx - 1])
                else:
                    invalid_indices.append(idx)
            
            if invalid_indices:
                print(f"⚠ Warning: Invalid indices {invalid_indices} (valid range: 1-{len(json_files)})")
            
            if selected_files:
                print(f"\n✓ Selected {len(selected_files)} file(s):")
                for f in selected_files:
                    print(f"  - {f}")
                return selected_files
            else:
                print("⚠ No valid files selected. Please try again.")
        except ValueError:
            print("⚠ Invalid input. Please enter numbers separated by commas, 'a' for all, or 'q' to quit.")

def merge_all_portfolios(directory_path, output_filename='master.json', interactive=True):
    """
    Merge portfolio JSON files by combining their children arrays.
    
    Args:
        directory_path: Directory containing JSON files
        output_filename: Name of output file
        interactive: If True, allow user to select files interactively
    """
    # Get all JSON files, excluding only output files and the script itself
    # Note: bil.json and spy.json are NOT excluded - they will appear in the selection menu
    exclude_files = ['master.json', 'merged_spy_bil.json', 'merge_portfolios.py']
    all_json_files = sorted([
        f for f in os.listdir(directory_path) 
        if f.endswith('.json') 
        and f not in exclude_files
    ])
    
    if not all_json_files:
        print("No JSON files found to merge.")
        return None
    
    # Select files interactively or use all
    if interactive:
        json_files = select_files_interactive(all_json_files)
        if not json_files:
            return None
    else:
        json_files = all_json_files
        print(f"Found {len(json_files)} portfolio files to merge:")
        for f in json_files:
            print(f"  - {f}")
    
    # Read selected portfolios
    portfolios = []
    portfolio_names = []
    for json_file in json_files:
        file_path = os.path.join(directory_path, json_file)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                portfolio = json.load(f)
                portfolios.append(portfolio)
                portfolio_names.append(json_file)
                print(f"✓ Loaded {json_file}")
        except Exception as e:
            print(f"✗ Error reading {json_file}: {e}")
            continue
    
    if not portfolios:
        print("No valid portfolios to merge.")
        return None
    
    # Get allocation percentages
    allocations = {}
    if interactive:
        allocations = get_allocation_percentages(portfolio_names, default_equal=True)
    else:
        # Equal weighting for non-interactive mode
        equal_weight = 100.0 / len(portfolio_names)
        allocations = {name: equal_weight for name in portfolio_names}
        print(f"\nUsing equal weighting: {equal_weight:.2f}% each")
    
    # Use first portfolio as base
    merged = portfolios[0].copy()
    
    # Apply allocation percentages correctly
    # If one portfolio has 100% and others have 0%, use that portfolio's structure directly
    non_zero_allocations = {name: pct for name, pct in allocations.items() if pct > 0.01}
    
    if len(non_zero_allocations) == 1:
        # Single portfolio with 100% allocation - use its structure exactly
        portfolio_name = list(non_zero_allocations.keys())[0]
        portfolio_idx = portfolio_names.index(portfolio_name)
        merged = portfolios[portfolio_idx].copy()
        # Keep the original structure intact - this should match the single file exactly
        merged['name'] = 'master_portfolio'
    else:
        # Multiple portfolios with allocations
        # Check if all allocations are equal (within 0.1% tolerance)
        allocation_values = list(allocations.values())
        all_equal = len(set(round(v, 1) for v in allocation_values)) == 1
        
        if all_equal:
            # Equal weighting - combine all children directly in a single container
            all_children = []
            for portfolio in portfolios:
                if 'children' in portfolio and portfolio['children']:
                    all_children.extend(portfolio['children'])
            
            # Use first portfolio as base structure
            merged = portfolios[0].copy()
            # Single wt-cash-equal container with all strategies
            merged['children'] = [{
                "id": str(uuid.uuid4()),
                "step": "wt-cash-equal",
                "children": all_children
            }]
        else:
            # Different allocations - structure: root -> wt-cash-specified -> wt-cash-equal (with weights)
            weighted_portfolios = []
            for portfolio, portfolio_name in zip(portfolios, portfolio_names):
                allocation_pct = allocations.get(portfolio_name, 0)
                if allocation_pct > 0.01 and 'children' in portfolio and portfolio['children']:
                    # Convert percentage to num/den format
                    weight_num = int(round(allocation_pct))
                    weight_den = 100
                    
                    # Each portfolio becomes a wt-cash-equal child with its allocation weight
                    wrapped_child = {
                        "id": str(uuid.uuid4()),
                        "step": "wt-cash-equal",
                        "weight": {
                            "num": weight_num,
                            "den": weight_den
                        },
                        "children": portfolio['children']
                    }
                    weighted_portfolios.append(wrapped_child)
            
            # Use first portfolio as base structure
            merged = portfolios[0].copy()
            # Wrap all weighted portfolios in a single wt-cash-specified container
            # Structure: root -> wt-cash-specified -> [wt-cash-equal (with weights), ...]
            merged['children'] = [{
                "id": str(uuid.uuid4()),
                "step": "wt-cash-specified",
                "children": weighted_portfolios
            }]
    
    # Combine asset_classes (unique values)
    all_asset_classes = set()
    for portfolio in portfolios:
        if 'asset_classes' in portfolio:
            all_asset_classes.update(portfolio['asset_classes'])
    
    if all_asset_classes:
        merged['asset_classes'] = list(all_asset_classes)
    
    # Update name to reflect it's a merged portfolio
    merged['name'] = 'master_portfolio'
    
    # Write merged portfolio
    output_path = os.path.join(directory_path, output_filename)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ Master portfolio created: {output_path}")
    print(f"  - Total children: {len(merged.get('children', []))}")
    print(f"  - Asset classes: {merged.get('asset_classes', [])}")
    
    return merged

if __name__ == "__main__":
    import sys
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Check for command-line argument to disable interactive mode
    interactive = True
    if len(sys.argv) > 1 and sys.argv[1] in ['--no-interactive', '-n', '--all']:
        interactive = False
    
    merge_all_portfolios(script_dir, interactive=interactive)

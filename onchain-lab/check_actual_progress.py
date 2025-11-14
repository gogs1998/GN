#!/usr/bin/env python3
"""Check actual ingestion progress by counting blocks in parquet files."""

from pathlib import Path
import pyarrow.parquet as pq
import json

def count_blocks_in_parquet(data_root: Path):
    """Count actual blocks by reading parquet files."""
    print(f"\nCounting blocks in parquet files: {data_root}")
    print("=" * 70)
    
    blocks_dir = data_root / "blocks"
    if not blocks_dir.exists():
        print(f"❌ Blocks directory does not exist")
        return None
    
    # Get all parquet files
    all_parquet_files = list(blocks_dir.glob("**/*.parquet"))
    print(f"Found {len(all_parquet_files)} parquet files")
    
    if not all_parquet_files:
        print(f"❌ No parquet files found")
        return None
    
    # Count blocks by reading files
    total_blocks = 0
    files_read = 0
    errors = 0
    
    print(f"\nReading parquet files to count blocks...")
    print(f"(This may take a while for large datasets)")
    
    # Sample first few files to understand structure
    sample_files = all_parquet_files[:10]
    for i, file_path in enumerate(sample_files):
        try:
            table = pq.read_table(file_path)
            rows = table.num_rows
            total_blocks += rows
            files_read += 1
            if i < 5:
                print(f"  {file_path.name}: {rows} blocks")
        except Exception as e:
            errors += 1
            print(f"  Error reading {file_path.name}: {e}")
    
    # If we have many files, estimate based on average
    if len(all_parquet_files) > 10:
        avg_blocks_per_file = total_blocks / files_read if files_read > 0 else 0
        estimated_total = avg_blocks_per_file * len(all_parquet_files)
        print(f"\n  Sampled {files_read} files, average {avg_blocks_per_file:.1f} blocks/file")
        print(f"  Estimated total blocks: {estimated_total:,.0f}")
        print(f"\n  Reading remaining files for accurate count...")
        
        # Read remaining files
        for file_path in all_parquet_files[10:]:
            try:
                table = pq.read_table(file_path)
                total_blocks += table.num_rows
                files_read += 1
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"  Error reading {file_path.name}: {e}")
    
    print(f"\n✅ Total blocks counted: {total_blocks:,}")
    print(f"   Files read: {files_read}/{len(all_parquet_files)}")
    print(f"   Errors: {errors}")
    
    return {
        "total_blocks": int(total_blocks),
        "files_read": files_read,
        "errors": errors,
    }

def check_markers(data_root: Path):
    """Check marker files."""
    marker_dir = data_root / "_markers"
    if not marker_dir.exists():
        return None
    
    markers = sorted([
        int(f.stem) for f in marker_dir.glob("*.done")
        if f.stem.isdigit()
    ])
    
    state_file = marker_dir / "state.json"
    state_max = -1
    if state_file.exists():
        state = json.loads(state_file.read_text())
        state_max = state.get("max_height", -1)
    
    return {
        "marker_count": len(markers),
        "min_height": min(markers) if markers else -1,
        "max_height": max(markers) if markers else -1,
        "state_max_height": state_max,
    }

if __name__ == "__main__":
    print("ACTUAL BLOCKCHAIN INGESTION PROGRESS")
    print("=" * 70)
    
    d_location = Path("d:/Blockchain/onchain-data/parquet")
    
    # Check markers first
    marker_info = check_markers(d_location)
    if marker_info:
        print(f"\nMARKER FILES:")
        print(f"  Marker files: {marker_info['marker_count']:,}")
        print(f"  Height range: {marker_info['min_height']} - {marker_info['max_height']}")
        print(f"  State max_height: {marker_info['state_max_height']}")
    
    # Count actual blocks
    block_info = count_blocks_in_parquet(d_location)
    
    if block_info and marker_info:
        print(f"\n" + "=" * 70)
        print(f"COMPARISON")
        print("=" * 70)
        print(f"Blocks in parquet files: {block_info['total_blocks']:,}")
        print(f"Marker files: {marker_info['marker_count']:,}")
        
        if block_info['total_blocks'] != marker_info['marker_count']:
            diff = block_info['total_blocks'] - marker_info['marker_count']
            print(f"\n⚠️  MISMATCH DETECTED!")
            print(f"   Difference: {diff:,} blocks")
            print(f"   This suggests marker files are incomplete or missing")
        
        # Calculate progress
        estimated_tip = 520000
        progress_pct = (block_info['total_blocks'] / estimated_tip * 100) if estimated_tip > 0 else 0
        print(f"\nESTIMATED PROGRESS:")
        print(f"  Based on parquet files: {progress_pct:.2f}%")
        print(f"  Blocks processed: {block_info['total_blocks']:,} / ~{estimated_tip:,}")


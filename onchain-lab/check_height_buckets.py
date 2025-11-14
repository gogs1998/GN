#!/usr/bin/env python3
"""Check height buckets to find actual ingestion progress."""

from pathlib import Path
import pyarrow.parquet as pq

def check_height_buckets(data_root: Path):
    """Check height buckets to determine actual progress."""
    print(f"\nChecking height buckets in: {data_root}")
    print("=" * 70)
    
    blocks_dir = data_root / "blocks"
    if not blocks_dir.exists():
        print(f"❌ Blocks directory does not exist")
        return None
    
    # Get all height bucket directories
    height_dirs = [
        d for d in blocks_dir.iterdir() 
        if d.is_dir() and d.name.startswith("height=")
    ]
    
    if not height_dirs:
        print(f"❌ No height bucket directories found")
        return None
    
    # Sort by height bucket number
    height_dirs = sorted(
        height_dirs,
        key=lambda x: int(x.name.split("=")[1])
    )
    
    print(f"✅ Found {len(height_dirs)} height buckets")
    print(f"   First bucket: {height_dirs[0].name}")
    print(f"   Last bucket: {height_dirs[-1].name}")
    
    # Get last bucket number
    last_bucket = int(height_dirs[-1].name.split("=")[1])
    bucket_size = 10000  # From config
    estimated_max_height = last_bucket + bucket_size - 1
    
    print(f"\n   Last bucket covers heights: {last_bucket} - {estimated_max_height}")
    
    # Count total parquet files
    total_block_files = 0
    total_rows = 0
    
    print(f"\n   Counting files in each bucket...")
    for i, h_dir in enumerate(height_dirs):
        files = list(h_dir.glob("*.parquet"))
        total_block_files += len(files)
        
        # Sample a few files to count rows
        if files and i < 5:  # Check first 5 buckets
            try:
                table = pq.read_table(files[0])
                total_rows += table.num_rows * len(files)
            except Exception as e:
                print(f"      Warning: Could not read {h_dir.name}: {e}")
    
    print(f"\n   Total block parquet files: {total_block_files:,}")
    
    # Estimate actual blocks processed
    # Each bucket should have files, and each file should have blocks
    # But we need to check if files span multiple blocks or if there's one file per block
    
    # Check a sample from the last bucket
    last_bucket_dir = height_dirs[-1]
    last_files = list(last_bucket_dir.glob("*.parquet"))
    if last_files:
        try:
            # Read the last file to see how many blocks it contains
            last_table = pq.read_table(last_files[-1])
            blocks_in_last_file = last_table.num_rows
            print(f"\n   Last file in last bucket has {blocks_in_last_file} blocks")
            
            # Estimate: if we have files, and each file has multiple blocks,
            # we might have more blocks than marker files suggest
            estimated_blocks = total_block_files * blocks_in_last_file if blocks_in_last_file > 0 else total_block_files
            print(f"   Estimated total blocks (if {blocks_in_last_file} blocks/file): {estimated_blocks:,}")
        except Exception as e:
            print(f"   Could not read last file: {e}")
    
    # Also check transaction files to get a better estimate
    tx_dir = data_root / "tx"
    if tx_dir.exists():
        tx_height_dirs = [
            d for d in tx_dir.iterdir() 
            if d.is_dir() and d.name.startswith("height=")
        ]
        tx_height_dirs = sorted(
            tx_height_dirs,
            key=lambda x: int(x.name.split("=")[1])
        )
        if tx_height_dirs:
            tx_last_bucket = int(tx_height_dirs[-1].name.split("=")[1])
            print(f"\n   Transaction files: {len(tx_height_dirs)} buckets, last bucket: height={tx_last_bucket}")
    
    return {
        "num_buckets": len(height_dirs),
        "last_bucket": last_bucket,
        "estimated_max_height": estimated_max_height,
        "total_block_files": total_block_files,
    }

if __name__ == "__main__":
    print("HEIGHT BUCKET ANALYSIS")
    print("=" * 70)
    
    d_location = Path("d:/Blockchain/onchain-data/parquet")
    result = check_height_buckets(d_location)
    
    if result:
        estimated_tip = 520000
        progress_pct = (result["estimated_max_height"] / estimated_tip * 100) if estimated_tip > 0 else 0
        print(f"\n" + "=" * 70)
        print(f"ESTIMATED PROGRESS")
        print("=" * 70)
        print(f"Estimated max height processed: {result['estimated_max_height']:,}")
        print(f"Progress: {progress_pct:.2f}% (assuming tip ~{estimated_tip})")
        print(f"\nNote: This is based on height buckets, not marker files.")
        print(f"      Marker files may be missing or in a different location.")


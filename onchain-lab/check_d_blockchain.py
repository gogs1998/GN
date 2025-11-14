#!/usr/bin/env python3
"""Check blockchain ingestion progress in d:\Blockchain location."""

from pathlib import Path
import json

def check_progress(data_root: Path):
    """Check ingestion progress for a given data root."""
    print(f"\nChecking: {data_root}")
    print("=" * 70)
    
    if not data_root.exists():
        print(f"❌ Data root does not exist: {data_root}")
        return None
    
    # Check markers
    marker_dir = data_root / "_markers"
    if not marker_dir.exists():
        print(f"❌ Markers directory does not exist")
        return None
    
    # Count marker files
    markers = sorted([
        int(f.stem) for f in marker_dir.glob("*.done")
        if f.stem.isdigit()
    ])
    
    processed_count = len(markers)
    
    if not markers:
        print(f"❌ No marker files found")
        return None
    
    min_height = min(markers)
    max_height = max(markers)
    
    # Read state.json
    state_file = marker_dir / "state.json"
    state_max_height = -1
    if state_file.exists():
        state = json.loads(state_file.read_text())
        state_max_height = state.get("max_height", -1)
    
    # Check for gaps
    gaps = []
    if markers:
        prev = markers[0] - 1
        for h in markers:
            if h - prev > 1:
                gaps.append((prev + 1, h - 1))
            prev = h
    
    # Count data files
    blocks_dir = data_root / "blocks"
    tx_dir = data_root / "tx"
    txin_dir = data_root / "txin"
    txout_dir = data_root / "txout"
    
    block_files = len(list(blocks_dir.glob("**/*.parquet"))) if blocks_dir.exists() else 0
    tx_files = len(list(tx_dir.glob("**/*.parquet"))) if tx_dir.exists() else 0
    txin_files = len(list(txin_dir.glob("**/*.parquet"))) if txin_dir.exists() else 0
    txout_files = len(list(txout_dir.glob("**/*.parquet"))) if txout_dir.exists() else 0
    
    # Estimate blockchain tip (rough: current tip is ~520K)
    estimated_tip = 520000
    total_blocks = estimated_tip + 1
    progress_pct = (processed_count / total_blocks * 100) if total_blocks > 0 else 0
    
    print(f"✅ Found {processed_count:,} processed blocks")
    print(f"   Height range: {min_height} - {max_height}")
    print(f"   State max_height: {state_max_height}")
    print(f"   Progress: {progress_pct:.2f}% (assuming tip ~{estimated_tip})")
    print(f"   Gaps: {len(gaps)}")
    if gaps and len(gaps) <= 10:
        print(f"   Gap ranges: {gaps}")
    print(f"\n   Data files:")
    print(f"     Blocks: {block_files:,}")
    print(f"     Transactions: {tx_files:,}")
    print(f"     TxIn: {txin_files:,}")
    print(f"     TxOut: {txout_files:,}")
    
    return {
        "processed_count": processed_count,
        "min_height": min_height,
        "max_height": max_height,
        "state_max_height": state_max_height,
        "progress_pct": progress_pct,
        "gaps": len(gaps),
        "block_files": block_files,
        "tx_files": tx_files,
    }

if __name__ == "__main__":
    print("BLOCKCHAIN INGESTION PROGRESS CHECK")
    print("=" * 70)
    
    # Check both locations
    d_location = Path("d:/Blockchain/onchain-data/parquet")
    s_location = Path("S:/S/Other/Blockchain/onchain-data/parquet")
    
    d_result = check_progress(d_location)
    s_result = check_progress(s_location)
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    if d_result:
        print(f"\nd:\\Blockchain: {d_result['processed_count']:,} blocks ({d_result['progress_pct']:.2f}%)")
    else:
        print(f"\nd:\\Blockchain: Not found or empty")
    
    if s_result:
        print(f"S:\\...: {s_result['processed_count']:,} blocks ({s_result['progress_pct']:.2f}%)")
    else:
        print(f"S:\\...: Not found or empty")
    
    if d_result and s_result:
        total_unique = max(d_result['max_height'], s_result['max_height']) + 1
        print(f"\n⚠️  Data exists in BOTH locations - potential duplicate!")
        print(f"   Consider consolidating to avoid confusion")


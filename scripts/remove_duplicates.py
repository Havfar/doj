#!/usr/bin/env python3
"""Remove duplicate files with -2, -3, etc. suffixes from downloads folder."""

import argparse
import re
from pathlib import Path


def find_duplicates(directory: Path, dry_run: bool = True) -> list[Path]:
    """Find files with -N suffix where the original exists."""
    
    # Pattern: filename-2.pdf, filename-3.pdf, etc.
    suffix_pattern = re.compile(r'^(.+)-(\d+)(\.[^.]+)$')
    
    duplicates = []
    
    for file in sorted(directory.iterdir()):
        if not file.is_file():
            continue
        
        match = suffix_pattern.match(file.name)
        if match:
            stem, num, ext = match.groups()
            original = directory / f"{stem}{ext}"
            
            # Only consider it a duplicate if original exists
            if original.exists():
                duplicates.append(file)
    
    return duplicates


def main():
    parser = argparse.ArgumentParser(
        description="Remove duplicate files with -2, -3, etc. suffixes"
    )
    parser.add_argument(
        "--dir", 
        default="downloads", 
        help="Directory to clean (default: downloads)"
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true", 
        help="Show what would be deleted without deleting"
    )
    parser.add_argument(
        "--force", 
        action="store_true", 
        help="Delete without confirmation"
    )
    args = parser.parse_args()
    
    directory = Path(args.dir)
    if not directory.exists():
        print(f"Directory not found: {directory}")
        return
    
    duplicates = find_duplicates(directory)
    
    if not duplicates:
        print("No duplicates found.")
        return
    
    # Calculate total size
    total_size = sum(f.stat().st_size for f in duplicates)
    size_mb = total_size / (1024 * 1024)
    
    print(f"Found {len(duplicates)} duplicate files ({size_mb:.1f} MB)")
    
    if args.dry_run:
        print("\nWould delete:")
        for f in duplicates[:20]:
            print(f"  {f.name}")
        if len(duplicates) > 20:
            print(f"  ... and {len(duplicates) - 20} more")
        print("\nRun without --dry-run to delete.")
        return
    
    if not args.force:
        response = input(f"\nDelete {len(duplicates)} files? [y/N] ")
        if response.lower() != 'y':
            print("Aborted.")
            return
    
    # Delete duplicates
    deleted = 0
    for f in duplicates:
        try:
            f.unlink()
            deleted += 1
        except Exception as e:
            print(f"Error deleting {f.name}: {e}")
    
    print(f"Deleted {deleted} files ({size_mb:.1f} MB freed)")


if __name__ == "__main__":
    main()

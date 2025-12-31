#!/usr/bin/env python3
"""
Western Wear Store Collection CLI

Usage:
    python run_collection.py --state SC           # Single state
    python run_collection.py --priority           # Priority western states
    python run_collection.py --full               # Full USA
    python run_collection.py --resume             # Resume interrupted run
    python run_collection.py --dry-run --state TX # Show coverage only
"""

import argparse
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.nationwide_collector import NationwideCollector
from src.usa_grid import generate_grid, generate_priority_grid, estimate_coverage, PRIORITY_STATES


def main():
    parser = argparse.ArgumentParser(
        description="Collect western wear stores across the USA",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_collection.py --state SC             # Collect SC only
  python run_collection.py --priority             # TX, OK, MT, WY, etc.
  python run_collection.py --full                 # Entire USA
  python run_collection.py --dry-run --state TX   # Show TX coverage
  python run_collection.py --resume               # Resume previous run
        """
    )
    
    # Scope options (mutually exclusive)
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument(
        "--state",
        type=str,
        metavar="XX",
        help="Collect single state (e.g., TX, SC, CA)"
    )
    scope.add_argument(
        "--priority",
        action="store_true",
        help=f"Collect priority western states: {', '.join(PRIORITY_STATES)}"
    )
    scope.add_argument(
        "--full",
        action="store_true",
        help="Collect entire continental USA"
    )
    scope.add_argument(
        "--resume",
        action="store_true",
        help="Resume previous interrupted collection"
    )
    
    # Options
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show coverage without actually collecting"
    )
    parser.add_argument(
        "--max-points",
        type=int,
        metavar="N",
        help="Limit to N grid points (for testing)"
    )
    parser.add_argument(
        "--spacing",
        type=float,
        default=70.0,
        metavar="KM",
        help="Grid spacing in km (default: 70)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/collected",
        help="Output directory (default: data/collected)"
    )
    parser.add_argument(
        "--no-google",
        action="store_true",
        help="Disable Google Places API"
    )
    parser.add_argument(
        "--use-yelp",
        action="store_true",
        help="Enable Yelp API (requires API key)"
    )
    parser.add_argument(
        "--no-osm",
        action="store_true",
        help="Disable OSM Overpass"
    )
    
    args = parser.parse_args()
    
    # Determine scope
    state = None
    priority_only = False
    resume = False
    
    if args.state:
        state = args.state.upper()
    elif args.priority:
        priority_only = True
    elif args.resume:
        resume = True
    # else: --full (no special flags needed)
    
    # Setup output
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("Western Wear Store Collector")
    print("=" * 60)
    
    if args.dry_run:
        # Just show coverage
        if state:
            points = generate_grid(spacing_km=args.spacing, state=state)
        elif priority_only:
            points = generate_priority_grid(spacing_km=args.spacing)
        else:
            points = generate_grid(spacing_km=args.spacing)
        
        if args.max_points:
            points = points[:args.max_points]
        
        stats = estimate_coverage(points, radius_km=50.0)
        
        print(f"\nDry Run - Coverage Analysis")
        print(f"  Grid points: {stats['total_points']}")
        print(f"  Estimated area: {stats['estimated_area_km2']:,} km²")
        print(f"\n  States covered:")
        for s, count in sorted(stats['by_state'].items(), key=lambda x: -x[1])[:15]:
            print(f"    {s}: {count} points")
        
        # Cost estimate
        queries = 7  # default number of queries
        api_calls = stats['total_points'] * queries
        cost = api_calls * 0.032  # Google Places pricing
        
        print(f"\n  Estimated API calls: {api_calls:,}")
        print(f"  Estimated Google cost: ${cost:.2f}")
        
        return
    
    # Run collection
    collector = NationwideCollector(
        output_dir=output_dir,
        spacing_km=args.spacing,
        use_google=not args.no_google,
        use_yelp=args.use_yelp,
        use_osm=not args.no_osm,
    )
    
    try:
        result = collector.run(
            state=state,
            priority_only=priority_only,
            resume=resume or args.resume,
            dry_run=args.dry_run,
            max_points=args.max_points,
        )
        
        print("\nCollection Summary:")
        print(f"  Stores found: {result.get('stores_found', 0)}")
        print(f"  Output: {result.get('output_path', 'N/A')}")
        
    except KeyboardInterrupt:
        print("\n\nInterrupted. Progress saved - run with --resume to continue.")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

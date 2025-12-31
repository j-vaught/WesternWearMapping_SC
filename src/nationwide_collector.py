"""
Nationwide Store Collector

Orchestrates collection across the entire USA using grid-based search.
Supports progress tracking, resume capability, and multiple data sources.
"""

import json
import time
import os
from datetime import datetime
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, asdict

from .usa_grid import generate_grid, generate_priority_grid, GridPoint, estimate_coverage
from .deduplicator import Deduplicator
from .collectors import google_places, yelp, osm_overpass


@dataclass
class CollectionProgress:
    """Tracks collection progress for resume capability."""
    started_at: str
    last_updated: str
    total_points: int
    completed_points: int
    current_index: int
    stores_found: int
    errors: int
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> "CollectionProgress":
        return cls(**data)
    
    def save(self, path: Path) -> None:
        self.last_updated = datetime.now().isoformat()
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)
    
    @classmethod
    def load(cls, path: Path) -> Optional["CollectionProgress"]:
        if path.exists():
            with open(path) as f:
                return cls.from_dict(json.load(f))
        return None


class NationwideCollector:
    """
    Collects western wear stores across the USA.
    
    Uses grid-based search with configurable spacing.
    Supports resume from interruption.
    """
    
    DEFAULT_QUERIES = [
        "western wear store",
        "cowboy boots",
        "cowboy hats",
        "tack shop",
        "western clothing",
        "Boot Barn",
        "Cavender's",
    ]
    
    def __init__(
        self,
        output_dir: Path,
        spacing_km: float = 70.0,
        search_radius_m: int = 50000,
        queries: Optional[list[str]] = None,
        use_google: bool = True,
        use_yelp: bool = False,
        use_osm: bool = True,
        delay_seconds: float = 0.5,
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.spacing_km = spacing_km
        self.search_radius_m = search_radius_m
        self.queries = queries or self.DEFAULT_QUERIES
        self.delay_seconds = delay_seconds
        
        # Collector flags
        self.use_google = use_google
        self.use_yelp = use_yelp
        self.use_osm = use_osm
        
        # Paths
        self.progress_path = self.output_dir / "progress.json"
        self.stores_path = self.output_dir / "stores.json"
        self.log_path = self.output_dir / "collection.log"
        
        # State
        self.dedup = Deduplicator()
        self.progress: Optional[CollectionProgress] = None
        self.grid_points: list[GridPoint] = []
    
    def _log(self, message: str) -> None:
        """Log message to file and console."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] {message}"
        print(line)
        with open(self.log_path, "a") as f:
            f.write(line + "\n")
    
    def _save_stores(self) -> None:
        """Save current stores to JSON."""
        stores = self.dedup.get_all()
        with open(self.stores_path, "w") as f:
            json.dump({
                "collection_date": datetime.now().isoformat(),
                "total_stores": len(stores),
                "stores": stores
            }, f, indent=2)
    
    def _collect_at_point(self, point: GridPoint) -> int:
        """
        Run all collectors at a single grid point.
        
        Returns number of new stores found.
        """
        new_count = 0
        location = (point.lat, point.lon)
        
        for query in self.queries:
            # Google Places
            if self.use_google:
                try:
                    places = google_places.search_places(
                        query=query,
                        location=location,
                        radius_meters=self.search_radius_m
                    )
                    for place in places:
                        store_data = google_places.parse_place(place)
                        _, is_new = self.dedup.add(store_data, source="google_places")
                        if is_new:
                            new_count += 1
                    time.sleep(self.delay_seconds)
                except Exception as e:
                    self._log(f"  Google error at {point.lat},{point.lon}: {e}")
            
            # OSM Overpass (free, no rate limit concerns)
            if self.use_osm:
                try:
                    # Create bounding box around point
                    delta = self.search_radius_m / 111000  # rough conversion to degrees
                    bbox = (
                        point.lat - delta,
                        point.lon - delta,
                        point.lat + delta,
                        point.lon + delta
                    )
                    pois = osm_overpass.search_area(bbox=bbox, query=query)
                    for poi in pois:
                        store_data = osm_overpass.parse_element(poi)
                        if store_data:
                            _, is_new = self.dedup.add(store_data, source="osm")
                            if is_new:
                                new_count += 1
                except Exception as e:
                    self._log(f"  OSM error at {point.lat},{point.lon}: {e}")
            
            # Yelp (if enabled and configured)
            if self.use_yelp:
                try:
                    businesses = yelp.search_businesses(
                        term=query,
                        latitude=point.lat,
                        longitude=point.lon,
                        radius=min(self.search_radius_m, 40000)  # Yelp max is 40km
                    )
                    for biz in businesses:
                        store_data = yelp.parse_business(biz)
                        _, is_new = self.dedup.add(store_data, source="yelp")
                        if is_new:
                            new_count += 1
                    time.sleep(self.delay_seconds)
                except Exception as e:
                    self._log(f"  Yelp error at {point.lat},{point.lon}: {e}")
        
        return new_count
    
    def run(
        self,
        state: Optional[str] = None,
        priority_only: bool = False,
        resume: bool = True,
        dry_run: bool = False,
        max_points: Optional[int] = None,
    ) -> dict:
        """
        Run the collection.
        
        Args:
            state: Collect only this state (e.g., "TX")
            priority_only: Only collect priority western states
            resume: Resume from previous progress if available
            dry_run: Just show what would be collected
            max_points: Limit number of points (for testing)
        
        Returns:
            Summary statistics
        """
        # Generate grid
        if state:
            self.grid_points = generate_grid(spacing_km=self.spacing_km, state=state)
            self._log(f"Generated {len(self.grid_points)} points for {state}")
        elif priority_only:
            self.grid_points = generate_priority_grid(spacing_km=self.spacing_km)
            self._log(f"Generated {len(self.grid_points)} points for priority states")
        else:
            self.grid_points = generate_grid(spacing_km=self.spacing_km)
            self._log(f"Generated {len(self.grid_points)} points for full USA")
        
        if max_points:
            self.grid_points = self.grid_points[:max_points]
            self._log(f"Limited to {max_points} points")
        
        # Show coverage estimate
        stats = estimate_coverage(self.grid_points, radius_km=self.search_radius_m/1000)
        self._log(f"Coverage: {stats['estimated_area_km2']:,} km² across {len(stats['by_state'])} states")
        
        if dry_run:
            self._log("Dry run - no collection performed")
            return {
                "dry_run": True,
                "grid_points": len(self.grid_points),
                "coverage": stats,
                "estimated_api_calls": len(self.grid_points) * len(self.queries),
            }
        
        # Check for resume
        start_index = 0
        if resume:
            self.progress = CollectionProgress.load(self.progress_path)
            if self.progress and self.progress.current_index < len(self.grid_points):
                start_index = self.progress.current_index
                self._log(f"Resuming from point {start_index}/{len(self.grid_points)}")
                # Reload existing stores
                if self.stores_path.exists():
                    with open(self.stores_path) as f:
                        data = json.load(f)
                        for store in data.get("stores", []):
                            self.dedup.add(store, source="resume")
        
        # Initialize progress
        if not self.progress:
            self.progress = CollectionProgress(
                started_at=datetime.now().isoformat(),
                last_updated=datetime.now().isoformat(),
                total_points=len(self.grid_points),
                completed_points=0,
                current_index=0,
                stores_found=0,
                errors=0,
            )
        
        self._log("=" * 60)
        self._log("Starting collection")
        self._log(f"  Points: {len(self.grid_points)}")
        self._log(f"  Queries: {len(self.queries)}")
        self._log(f"  Sources: Google={self.use_google}, Yelp={self.use_yelp}, OSM={self.use_osm}")
        self._log("=" * 60)
        
        # Collect
        try:
            for i, point in enumerate(self.grid_points[start_index:], start=start_index):
                self._log(f"Point {i+1}/{len(self.grid_points)}: ({point.lat}, {point.lon}) [{point.state}]")
                
                new_stores = self._collect_at_point(point)
                
                self.progress.current_index = i + 1
                self.progress.completed_points = i + 1
                self.progress.stores_found = len(self.dedup.stores)
                
                if new_stores > 0:
                    self._log(f"  Found {new_stores} new stores (total: {len(self.dedup.stores)})")
                
                # Save progress periodically
                if (i + 1) % 10 == 0:
                    self.progress.save(self.progress_path)
                    self._save_stores()
                    self._log(f"  Progress saved")
        
        except KeyboardInterrupt:
            self._log("Interrupted - saving progress")
            self.progress.save(self.progress_path)
            self._save_stores()
            raise
        
        # Final save
        self.progress.save(self.progress_path)
        self._save_stores()
        
        summary = {
            "completed": True,
            "total_points": len(self.grid_points),
            "stores_found": len(self.dedup.stores),
            "dedup_stats": self.dedup.stats(),
            "output_path": str(self.stores_path),
        }
        
        self._log("=" * 60)
        self._log("Collection complete")
        self._log(f"  Total stores: {summary['stores_found']}")
        self._log(f"  Output: {summary['output_path']}")
        self._log("=" * 60)
        
        return summary


def run_collection(
    state: Optional[str] = None,
    priority_only: bool = False,
    resume: bool = True,
    dry_run: bool = False,
    max_points: Optional[int] = None,
    output_dir: Optional[str] = None,
) -> dict:
    """
    Convenience function to run a collection.
    
    Args:
        state: Single state to collect (e.g., "TX", "SC")
        priority_only: Only collect priority western states
        resume: Resume from previous run
        dry_run: Show coverage without collecting
        max_points: Limit points for testing
        output_dir: Output directory (defaults to data/collected)
    
    Returns:
        Collection summary
    """
    if output_dir is None:
        output_dir = Path(__file__).parent.parent / "data" / "collected"
    
    collector = NationwideCollector(
        output_dir=output_dir,
        use_google=True,
        use_yelp=False,  # Requires API key
        use_osm=True,
    )
    
    return collector.run(
        state=state,
        priority_only=priority_only,
        resume=resume,
        dry_run=dry_run,
        max_points=max_points,
    )

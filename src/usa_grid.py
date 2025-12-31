"""
USA Grid Generator

Generates a grid of lat/lon points covering the continental USA
for systematic search coverage.
"""

import math
from dataclasses import dataclass
from typing import Iterator, Optional


@dataclass
class GridPoint:
    """A search point on the grid."""
    lat: float
    lon: float
    state: Optional[str] = None
    
    def __hash__(self):
        return hash((round(self.lat, 4), round(self.lon, 4)))


# Continental USA bounding box
USA_BOUNDS = {
    "min_lat": 24.5,   # Southern tip of Florida
    "max_lat": 49.0,   # Northern border
    "min_lon": -125.0, # West coast
    "max_lon": -66.5   # East coast
}

# State centers for priority ordering and state-based filtering
# (approximate centroids)
STATE_CENTERS = {
    "TX": (31.0, -100.0),
    "OK": (35.5, -97.5),
    "MT": (47.0, -110.0),
    "WY": (43.0, -107.5),
    "AZ": (34.0, -111.5),
    "NM": (34.5, -106.0),
    "CO": (39.0, -105.5),
    "NV": (39.0, -117.0),
    "UT": (39.0, -111.5),
    "ID": (44.0, -114.5),
    "KS": (38.5, -98.5),
    "NE": (41.5, -100.0),
    "SD": (44.5, -100.0),
    "ND": (47.5, -100.5),
    "CA": (37.0, -120.0),
    "OR": (44.0, -120.5),
    "WA": (47.5, -120.5),
    "SC": (34.0, -81.0),
    "NC": (35.5, -80.0),
    "GA": (32.5, -83.5),
    "FL": (28.0, -82.5),
    "AL": (32.8, -86.8),
    "MS": (32.7, -89.7),
    "LA": (31.0, -92.0),
    "AR": (34.8, -92.2),
    "TN": (35.8, -86.3),
    "KY": (37.8, -85.7),
    "VA": (37.5, -78.8),
    "WV": (38.9, -80.5),
    "PA": (41.0, -77.5),
    "NY": (43.0, -75.5),
    "OH": (40.2, -82.8),
    "IN": (40.0, -86.2),
    "IL": (40.0, -89.2),
    "MI": (44.3, -85.5),
    "WI": (44.5, -89.8),
    "MN": (46.0, -94.5),
    "IA": (42.0, -93.5),
    "MO": (38.5, -92.5),
}

# State bounding boxes for filtering
STATE_BOUNDS = {
    "TX": {"min_lat": 25.8, "max_lat": 36.5, "min_lon": -106.7, "max_lon": -93.5},
    "OK": {"min_lat": 33.6, "max_lat": 37.0, "min_lon": -103.0, "max_lon": -94.4},
    "MT": {"min_lat": 44.4, "max_lat": 49.0, "min_lon": -116.1, "max_lon": -104.0},
    "WY": {"min_lat": 41.0, "max_lat": 45.0, "min_lon": -111.1, "max_lon": -104.1},
    "AZ": {"min_lat": 31.3, "max_lat": 37.0, "min_lon": -114.8, "max_lon": -109.0},
    "NM": {"min_lat": 31.3, "max_lat": 37.0, "min_lon": -109.1, "max_lon": -103.0},
    "CO": {"min_lat": 37.0, "max_lat": 41.0, "min_lon": -109.1, "max_lon": -102.0},
    "NV": {"min_lat": 35.0, "max_lat": 42.0, "min_lon": -120.0, "max_lon": -114.0},
    "SC": {"min_lat": 32.0, "max_lat": 35.2, "min_lon": -83.4, "max_lon": -78.5},
    "CA": {"min_lat": 32.5, "max_lat": 42.0, "min_lon": -124.5, "max_lon": -114.1},
}

# Priority states for western wear (start collection here)
PRIORITY_STATES = ["TX", "OK", "MT", "WY", "AZ", "NM", "CO", "NV", "CA"]


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in kilometers."""
    R = 6371  # Earth's radius in km
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = (math.sin(delta_lat / 2) ** 2 +
         math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c


def generate_grid(
    spacing_km: float = 70.0,
    bounds: Optional[dict] = None,
    state: Optional[str] = None
) -> list[GridPoint]:
    """
    Generate grid points covering the specified area.
    
    Args:
        spacing_km: Distance between grid points in km (default 70km for 50km radius overlap)
        bounds: Custom bounding box (uses USA if not provided)
        state: Filter to single state (uses STATE_BOUNDS)
    
    Returns:
        List of GridPoint objects
    """
    if state and state.upper() in STATE_BOUNDS:
        bounds = STATE_BOUNDS[state.upper()]
    elif bounds is None:
        bounds = USA_BOUNDS
    
    points = []
    
    # Convert km to approximate degrees
    # At 40°N latitude, 1 degree lat ≈ 111km, 1 degree lon ≈ 85km
    lat_step = spacing_km / 111.0
    
    lat = bounds["min_lat"]
    while lat <= bounds["max_lat"]:
        # Longitude step varies with latitude
        lon_step = spacing_km / (111.0 * math.cos(math.radians(lat)))
        
        lon = bounds["min_lon"]
        while lon <= bounds["max_lon"]:
            point = GridPoint(lat=round(lat, 4), lon=round(lon, 4))
            
            # Assign state if we can determine it
            if state:
                point.state = state.upper()
            else:
                point.state = _guess_state(lat, lon)
            
            points.append(point)
            lon += lon_step
        
        lat += lat_step
    
    return points


def _guess_state(lat: float, lon: float) -> Optional[str]:
    """Guess which state a point is in based on proximity to state center."""
    min_dist = float('inf')
    closest_state = None
    
    for state, (clat, clon) in STATE_CENTERS.items():
        dist = haversine_distance(lat, lon, clat, clon)
        if dist < min_dist:
            min_dist = dist
            closest_state = state
    
    return closest_state


def generate_priority_grid(spacing_km: float = 70.0) -> list[GridPoint]:
    """
    Generate grid covering priority western wear states first.
    
    Returns points ordered by priority (TX first, then OK, etc.)
    """
    all_points = []
    seen = set()
    
    # First, add priority states in order
    for state in PRIORITY_STATES:
        if state in STATE_BOUNDS:
            state_points = generate_grid(spacing_km=spacing_km, state=state)
            for p in state_points:
                if p not in seen:
                    seen.add(p)
                    all_points.append(p)
    
    # Then add remaining USA
    usa_points = generate_grid(spacing_km=spacing_km)
    for p in usa_points:
        if p not in seen:
            seen.add(p)
            all_points.append(p)
    
    return all_points


def estimate_coverage(points: list[GridPoint], radius_km: float = 50.0) -> dict:
    """
    Estimate coverage statistics for a set of grid points.
    
    Returns:
        Dict with coverage stats
    """
    # Count by state
    state_counts = {}
    for p in points:
        state = p.state or "Unknown"
        state_counts[state] = state_counts.get(state, 0) + 1
    
    # Estimate total area covered (rough approximation)
    # Each point covers π * r² km²
    area_per_point = math.pi * radius_km ** 2
    total_area = len(points) * area_per_point
    
    return {
        "total_points": len(points),
        "by_state": state_counts,
        "estimated_area_km2": round(total_area),
        "search_radius_km": radius_km
    }


if __name__ == "__main__":
    # Demo: generate grid for SC
    print("=" * 60)
    print("USA Grid Generator Demo")
    print("=" * 60)
    
    # SC only
    sc_points = generate_grid(state="SC")
    print(f"\nSC grid points: {len(sc_points)}")
    for p in sc_points[:5]:
        print(f"  {p.lat}, {p.lon}")
    
    # Priority states
    priority_points = generate_priority_grid()
    stats = estimate_coverage(priority_points)
    print(f"\nPriority grid coverage:")
    print(f"  Total points: {stats['total_points']}")
    print(f"  Estimated area: {stats['estimated_area_km2']:,} km²")
    print(f"\n  By state:")
    for state, count in sorted(stats['by_state'].items(), key=lambda x: -x[1])[:10]:
        print(f"    {state}: {count} points")

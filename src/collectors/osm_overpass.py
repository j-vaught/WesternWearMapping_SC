"""
OpenStreetMap Overpass API Collector

Queries OSM for western wear stores using the Overpass API.
Completely free with no API key required.
"""

import requests
import json
import time
from typing import Optional
from pathlib import Path

OVERPASS_URL = "https://overpass-api.de/api/interpreter"


def build_query(bbox: tuple[float, float, float, float], name_patterns: list[str]) -> str:
    """
    Build Overpass QL query for western wear stores.
    
    Args:
        bbox: (south, west, north, east) bounding box
        name_patterns: List of name patterns to search for (case-insensitive regex)
    
    Returns:
        Overpass QL query string
    """
    south, west, north, east = bbox
    pattern = "|".join(name_patterns)
    
    query = f"""
[out:json][timeout:120];
(
  // Clothing stores with western-related names
  node["shop"="clothes"]["name"~"{pattern}",i]({south},{west},{north},{east});
  way["shop"="clothes"]["name"~"{pattern}",i]({south},{west},{north},{east});
  
  // Shoe stores with boot/cowboy in name
  node["shop"="shoes"]["name"~"{pattern}",i]({south},{west},{north},{east});
  way["shop"="shoes"]["name"~"{pattern}",i]({south},{west},{north},{east});
  
  // Outdoor/farm stores
  node["shop"="outdoor"]["name"~"{pattern}",i]({south},{west},{north},{east});
  way["shop"="outdoor"]["name"~"{pattern}",i]({south},{west},{north},{east});
  node["shop"="farm"]["name"~"{pattern}",i]({south},{west},{north},{east});
  way["shop"="farm"]["name"~"{pattern}",i]({south},{west},{north},{east});
  
  // General retail with western names
  node["shop"]["name"~"western wear|cowboy|boot barn|cavender|tack|saddlery",i]({south},{west},{north},{east});
  way["shop"]["name"~"western wear|cowboy|boot barn|cavender|tack|saddlery",i]({south},{west},{north},{east});
);
out center;
"""
    return query


def search_area(
    bbox: tuple[float, float, float, float],
    query: str = None
) -> list[dict]:
    """
    Search an area for western wear stores.
    
    Args:
        bbox: (south, west, north, east) bounding box
        query: Ignored (uses built-in western wear patterns)
    
    Returns:
        List of parsed store dicts
    """
    name_patterns = [
        "western", "cowboy", "boot", "ranch", "tack", 
        "rodeo", "saddlery", "wrangler", "ariat", "cavender",
        "boot barn", "sheplers"
    ]
    
    overpass_query = build_query(bbox, name_patterns)
    result = query_overpass(overpass_query)
    
    if not result:
        return []
    
    elements = result.get("elements", [])
    return [parse_osm_element(e) for e in elements]


def parse_element(element: dict) -> dict:
    """Alias for parse_osm_element for compatibility."""
    return parse_osm_element(element)


def query_overpass(query: str, max_retries: int = 3) -> Optional[dict]:
    """
    Execute Overpass API query with retry logic.
    
    Args:
        query: Overpass QL query string
        max_retries: Number of retries on failure
    
    Returns:
        JSON response dict or None on failure
    """
    for attempt in range(max_retries):
        try:
            print(f"  Querying Overpass API (attempt {attempt + 1}/{max_retries})...")
            response = requests.post(
                OVERPASS_URL,
                data={"data": query},
                timeout=180
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"  Error: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt * 10  # Exponential backoff
                print(f"  Retrying in {wait_time}s...")
                time.sleep(wait_time)
    return None


def parse_osm_element(element: dict) -> dict:
    """
    Parse an OSM element into our standardized store format.
    
    Args:
        element: OSM element from Overpass response
    
    Returns:
        Standardized store dict
    """
    tags = element.get("tags", {})
    
    # Get coordinates (center for ways, direct for nodes)
    if element["type"] == "node":
        lat = element.get("lat")
        lon = element.get("lon")
    else:
        center = element.get("center", {})
        lat = center.get("lat")
        lon = center.get("lon")
    
    return {
        "osm_id": f"{element['type']}/{element['id']}",
        "name": tags.get("name", "Unknown"),
        "address_street": tags.get("addr:street", ""),
        "address_housenumber": tags.get("addr:housenumber", ""),
        "address_city": tags.get("addr:city", ""),
        "address_state": tags.get("addr:state", ""),
        "address_postcode": tags.get("addr:postcode", ""),
        "latitude": lat,
        "longitude": lon,
        "phone": tags.get("phone", ""),
        "website": tags.get("website", ""),
        "opening_hours": tags.get("opening_hours", ""),
        "shop_type": tags.get("shop", ""),
        "brand": tags.get("brand", ""),
        "operator": tags.get("operator", ""),
        "data_source": "osm",
        "raw_tags": tags
    }


def collect_sc_pilot() -> list[dict]:
    """
    Run pilot collection for South Carolina (Richland/Lexington counties).
    
    Returns:
        List of store dicts
    """
    # Bounding box for Richland/Lexington area
    bbox = (33.7, -81.4, 34.3, -80.7)
    
    name_patterns = [
        "western", "cowboy", "boot", "ranch", "tack", 
        "rodeo", "saddlery", "wrangler", "ariat"
    ]
    
    print("=" * 60)
    print("OSM Overpass Collector - SC Pilot")
    print("=" * 60)
    print(f"Bounding box: {bbox}")
    print(f"Name patterns: {name_patterns}")
    print()
    
    query = build_query(bbox, name_patterns)
    result = query_overpass(query)
    
    if not result:
        print("Failed to query Overpass API")
        return []
    
    elements = result.get("elements", [])
    print(f"Found {len(elements)} OSM elements")
    
    stores = [parse_osm_element(e) for e in elements]
    
    # Print summary
    print()
    print("Stores found:")
    for store in stores:
        print(f"  - {store['name']} ({store['address_city'] or 'No city'})")
    
    return stores


def collect_usa_full() -> list[dict]:
    """
    Collect western wear stores across the entire USA.
    Uses state-by-state queries to avoid timeout.
    
    Returns:
        List of store dicts
    """
    # State bounding boxes (approximate)
    # We'll implement this for full collection later
    raise NotImplementedError("Full USA collection not yet implemented")


def save_results(stores: list[dict], output_path: Path) -> None:
    """Save collected stores to JSON file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(stores, f, indent=2)
    print(f"Saved {len(stores)} stores to {output_path}")


if __name__ == "__main__":
    # Run SC pilot
    stores = collect_sc_pilot()
    
    if stores:
        output_path = Path(__file__).parent.parent.parent / "data" / "raw" / "osm_sc_pilot.json"
        save_results(stores, output_path)

"""
Google Places API Collector

Uses Google Places API (Text Search) to find western wear stores.
Free tier: $200/month credit (~6,000 requests).
"""

import requests
import json
import time
import os
from typing import Optional
from pathlib import Path

# Google Places API endpoint
PLACES_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"


def get_api_key() -> str:
    """Get Google API key from environment or config file."""
    # Try environment variable first
    key = os.environ.get("GOOGLE_PLACES_API_KEY")
    if key:
        return key
    
    # Try config file
    config_path = Path(__file__).parent.parent.parent / "config" / "api_keys.env"
    if config_path.exists():
        with open(config_path) as f:
            for line in f:
                if line.startswith("GOOGLE_PLACES_API_KEY="):
                    return line.split("=", 1)[1].strip()
    
    raise ValueError(
        "GOOGLE_PLACES_API_KEY not found. "
        "Set it in environment or config/api_keys.env"
    )


def search_places(
    query: str,
    location: tuple[float, float],
    radius_meters: int = 50000,
    api_key: Optional[str] = None
) -> list[dict]:
    """
    Search for places using Google Places API (New).
    
    Args:
        query: Search query (e.g., "western wear store")
        location: (lat, lon) center point
        radius_meters: Search radius in meters
        api_key: Google API key (uses env if not provided)
    
    Returns:
        List of place results
    """
    if api_key is None:
        api_key = get_api_key()
    
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": (
            "places.id,places.displayName,places.formattedAddress,"
            "places.location,places.types,places.nationalPhoneNumber,"
            "places.websiteUri,places.regularOpeningHours,places.rating,"
            "places.userRatingCount,places.priceLevel"
        )
    }
    
    lat, lon = location
    
    payload = {
        "textQuery": query,
        "locationBias": {
            "circle": {
                "center": {"latitude": lat, "longitude": lon},
                "radius": radius_meters
            }
        },
        "maxResultCount": 20
    }
    
    try:
        response = requests.post(
            PLACES_SEARCH_URL,
            headers=headers,
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        return data.get("places", [])
    except requests.exceptions.RequestException as e:
        print(f"  Error: {e}")
        return []


def parse_place(place: dict) -> dict:
    """
    Parse Google Place into our standardized store format.
    
    Args:
        place: Google Place object
    
    Returns:
        Standardized store dict
    """
    location = place.get("location", {})
    display_name = place.get("displayName", {})
    
    # Parse opening hours if available
    hours = place.get("regularOpeningHours", {})
    hours_text = hours.get("weekdayDescriptions", [])
    
    return {
        "google_place_id": place.get("id", ""),
        "name": display_name.get("text", "Unknown"),
        "formatted_address": place.get("formattedAddress", ""),
        "latitude": location.get("latitude"),
        "longitude": location.get("longitude"),
        "phone": place.get("nationalPhoneNumber", ""),
        "website": place.get("websiteUri", ""),
        "opening_hours": hours_text,
        "types": place.get("types", []),
        "google_rating": place.get("rating"),
        "google_review_count": place.get("userRatingCount"),
        "price_level": place.get("priceLevel"),
        "data_source": "google_places"
    }


def collect_sc_pilot(api_key: Optional[str] = None) -> list[dict]:
    """
    Run pilot collection for South Carolina (Richland/Lexington counties).
    
    Args:
        api_key: Google API key (optional, uses env if not provided)
    
    Returns:
        List of store dicts
    """
    # Center of Columbia, SC (Richland/Lexington area)
    center = (34.0007, -81.0348)
    radius = 50000  # 50km to cover both counties
    
    search_queries = [
        "western wear store",
        "cowboy boots",
        "cowboy hats",
        "tack shop",
        "western clothing",
        "Boot Barn",
        "Cavender's"
    ]
    
    print("=" * 60)
    print("Google Places Collector - SC Pilot")
    print("=" * 60)
    print(f"Center: {center}")
    print(f"Radius: {radius}m")
    print(f"Queries: {search_queries}")
    print()
    
    all_stores = []
    seen_ids = set()
    
    for query in search_queries:
        print(f"Searching: '{query}'...")
        try:
            places = search_places(query, center, radius, api_key)
            
            for place in places:
                place_id = place.get("id")
                if place_id and place_id not in seen_ids:
                    seen_ids.add(place_id)
                    all_stores.append(parse_place(place))
                    
            print(f"  Found {len(places)} results ({len(seen_ids)} unique total)")
            
            # Be nice to the API
            time.sleep(0.5)
            
        except ValueError as e:
            print(f"  Skipping: {e}")
            break
    
    print()
    print(f"Total unique stores: {len(all_stores)}")
    print()
    print("Stores found:")
    for store in all_stores:
        print(f"  - {store['name']}")
        print(f"    {store['formatted_address']}")
        if store['google_rating']:
            print(f"    Rating: {store['google_rating']} ({store['google_review_count']} reviews)")
        print()
    
    return all_stores


def save_results(stores: list[dict], output_path: Path) -> None:
    """Save collected stores to JSON file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(stores, f, indent=2)
    print(f"Saved {len(stores)} stores to {output_path}")


if __name__ == "__main__":
    # Check for API key
    try:
        key = get_api_key()
        print(f"Using API key: {key[:8]}...")
        
        stores = collect_sc_pilot()
        
        if stores:
            output_path = Path(__file__).parent.parent.parent / "data" / "raw" / "google_sc_pilot.json"
            save_results(stores, output_path)
    except ValueError as e:
        print(f"ERROR: {e}")
        print()
        print("To use this collector:")
        print("1. Get a Google Cloud API key with Places API enabled")
        print("2. Set GOOGLE_PLACES_API_KEY environment variable, or")
        print("3. Add it to config/api_keys.env")

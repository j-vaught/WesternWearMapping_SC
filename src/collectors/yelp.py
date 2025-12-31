"""
Yelp Fusion API Collector

Uses Yelp Fusion API to find western wear stores with reviews.
Free tier: 5,000 API calls/day.
"""

import requests
import json
import time
import os
from typing import Optional
from pathlib import Path

YELP_SEARCH_URL = "https://api.yelp.com/v3/businesses/search"


def get_api_key() -> str:
    """Get Yelp API key from environment or config file."""
    key = os.environ.get("YELP_API_KEY")
    if key:
        return key
    
    config_path = Path(__file__).parent.parent.parent / "config" / "api_keys.env"
    if config_path.exists():
        with open(config_path) as f:
            for line in f:
                if line.startswith("YELP_API_KEY="):
                    return line.split("=", 1)[1].strip()
    
    raise ValueError(
        "YELP_API_KEY not found. "
        "Set it in environment or config/api_keys.env"
    )


def search_yelp(
    term: str,
    location: str,
    categories: Optional[str] = None,
    limit: int = 50,
    api_key: Optional[str] = None
) -> list[dict]:
    """
    Search Yelp for businesses.
    
    Args:
        term: Search term
        location: Location string (e.g., "Columbia, SC")
        categories: Yelp category filter
        limit: Max results (up to 50)
        api_key: Yelp API key
    
    Returns:
        List of business results
    """
    if api_key is None:
        api_key = get_api_key()
    
    headers = {
        "Authorization": f"Bearer {api_key}"
    }
    
    params = {
        "term": term,
        "location": location,
        "limit": limit,
        "sort_by": "best_match"
    }
    
    if categories:
        params["categories"] = categories
    
    try:
        response = requests.get(
            YELP_SEARCH_URL,
            headers=headers,
            params=params,
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        return data.get("businesses", [])
    except requests.exceptions.RequestException as e:
        print(f"  Error: {e}")
        return []


def parse_business(biz: dict) -> dict:
    """
    Parse Yelp business into our standardized store format.
    
    Args:
        biz: Yelp business object
    
    Returns:
        Standardized store dict
    """
    location = biz.get("location", {})
    coords = biz.get("coordinates", {})
    
    # Build address
    address_parts = location.get("display_address", [])
    
    return {
        "yelp_id": biz.get("id", ""),
        "name": biz.get("name", "Unknown"),
        "address_street": location.get("address1", ""),
        "address_city": location.get("city", ""),
        "address_state": location.get("state", ""),
        "address_zip": location.get("zip_code", ""),
        "formatted_address": ", ".join(address_parts),
        "latitude": coords.get("latitude"),
        "longitude": coords.get("longitude"),
        "phone": biz.get("display_phone", ""),
        "yelp_url": biz.get("url", ""),
        "yelp_rating": biz.get("rating"),
        "yelp_review_count": biz.get("review_count"),
        "price": biz.get("price", ""),
        "categories": [c.get("title") for c in biz.get("categories", [])],
        "is_closed": biz.get("is_closed", False),
        "data_source": "yelp"
    }


def collect_sc_pilot(api_key: Optional[str] = None) -> list[dict]:
    """
    Run pilot collection for South Carolina (Richland/Lexington counties).
    
    Args:
        api_key: Yelp API key (optional)
    
    Returns:
        List of store dicts
    """
    locations = ["Columbia, SC", "Lexington, SC", "Irmo, SC", "West Columbia, SC"]
    
    search_terms = [
        "western wear",
        "cowboy boots",
        "western clothing",
        "tack shop"
    ]
    
    print("=" * 60)
    print("Yelp Collector - SC Pilot")
    print("=" * 60)
    print(f"Locations: {locations}")
    print(f"Search terms: {search_terms}")
    print()
    
    all_stores = []
    seen_ids = set()
    
    for location in locations:
        for term in search_terms:
            print(f"Searching: '{term}' in {location}...")
            try:
                businesses = search_yelp(term, location, api_key=api_key)
                
                for biz in businesses:
                    biz_id = biz.get("id")
                    if biz_id and biz_id not in seen_ids:
                        seen_ids.add(biz_id)
                        all_stores.append(parse_business(biz))
                
                print(f"  Found {len(businesses)} results ({len(seen_ids)} unique total)")
                time.sleep(0.3)  # Rate limiting
                
            except ValueError as e:
                print(f"  Skipping: {e}")
                return all_stores
    
    print()
    print(f"Total unique stores: {len(all_stores)}")
    print()
    print("Stores found:")
    for store in all_stores:
        print(f"  - {store['name']}")
        print(f"    {store['formatted_address']}")
        print(f"    Categories: {', '.join(store['categories'])}")
        if store['yelp_rating']:
            print(f"    Rating: {store['yelp_rating']} ({store['yelp_review_count']} reviews)")
        print()
    
    return all_stores


def save_results(stores: list[dict], output_path: Path) -> None:
    """Save collected stores to JSON file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(stores, f, indent=2)
    print(f"Saved {len(stores)} stores to {output_path}")


if __name__ == "__main__":
    try:
        key = get_api_key()
        print(f"Using API key: {key[:8]}...")
        
        stores = collect_sc_pilot()
        
        if stores:
            output_path = Path(__file__).parent.parent.parent / "data" / "raw" / "yelp_sc_pilot.json"
            save_results(stores, output_path)
    except ValueError as e:
        print(f"ERROR: {e}")
        print()
        print("To use this collector:")
        print("1. Create a Yelp Fusion API app at https://www.yelp.com/developers")
        print("2. Set YELP_API_KEY environment variable, or")
        print("3. Add it to config/api_keys.env")

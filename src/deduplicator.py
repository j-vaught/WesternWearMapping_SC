"""
Deduplicator Module

Handles deduplication of stores collected from multiple sources.
Uses Google Place ID as primary key, fuzzy name matching as fallback.
"""

import re
from dataclasses import dataclass, field
from typing import Optional
from difflib import SequenceMatcher


@dataclass
class Store:
    """Canonical store record."""
    # Identifiers
    google_place_id: Optional[str] = None
    yelp_id: Optional[str] = None
    osm_id: Optional[str] = None
    
    # Core info
    name: str = ""
    formatted_address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    
    # Location
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    
    # Contact
    phone: str = ""
    website: str = ""
    
    # Ratings
    google_rating: Optional[float] = None
    google_review_count: Optional[int] = None
    yelp_rating: Optional[float] = None
    yelp_review_count: Optional[int] = None
    
    # Metadata
    store_type: str = ""
    categories: list = field(default_factory=list)
    sources: list = field(default_factory=list)
    notes: str = ""
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "google_place_id": self.google_place_id,
            "yelp_id": self.yelp_id,
            "osm_id": self.osm_id,
            "name": self.name,
            "formatted_address": self.formatted_address,
            "city": self.city,
            "state": self.state,
            "zip_code": self.zip_code,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "phone": self.phone,
            "website": self.website,
            "google_rating": self.google_rating,
            "google_review_count": self.google_review_count,
            "yelp_rating": self.yelp_rating,
            "yelp_review_count": self.yelp_review_count,
            "store_type": self.store_type,
            "categories": self.categories,
            "sources": self.sources,
            "notes": self.notes,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Store":
        """Create Store from dictionary."""
        return cls(
            google_place_id=data.get("google_place_id"),
            yelp_id=data.get("yelp_id"),
            osm_id=data.get("osm_id"),
            name=data.get("name", ""),
            formatted_address=data.get("formatted_address", ""),
            city=data.get("city", ""),
            state=data.get("state", ""),
            zip_code=data.get("zip_code", ""),
            latitude=data.get("latitude"),
            longitude=data.get("longitude"),
            phone=data.get("phone", ""),
            website=data.get("website", ""),
            google_rating=data.get("google_rating"),
            google_review_count=data.get("google_review_count"),
            yelp_rating=data.get("yelp_rating"),
            yelp_review_count=data.get("yelp_review_count"),
            store_type=data.get("store_type", ""),
            categories=data.get("categories", []),
            sources=data.get("sources", []),
            notes=data.get("notes", ""),
        )


def normalize_name(name: str) -> str:
    """Normalize store name for comparison."""
    # Lowercase
    name = name.lower()
    # Remove common suffixes
    suffixes = [
        "inc", "llc", "ltd", "corp", "co", 
        "western wear", "boot store", "boots", "store", "shop"
    ]
    for suffix in suffixes:
        name = re.sub(rf"\s+{suffix}\.?$", "", name)
    # Remove punctuation
    name = re.sub(r"[^\w\s]", "", name)
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name


def normalize_address(address: str) -> str:
    """Normalize address for comparison."""
    address = address.lower()
    # Standardize common abbreviations
    replacements = {
        r"\bstreet\b": "st",
        r"\bst\.": "st",
        r"\bavenue\b": "ave",
        r"\bave\.": "ave",
        r"\broad\b": "rd",
        r"\brd\.": "rd",
        r"\bdrive\b": "dr",
        r"\bdr\.": "dr",
        r"\bboulevard\b": "blvd",
        r"\bblvd\.": "blvd",
        r"\bsuite\b": "ste",
        r"\bste\.": "ste",
        r"\bnorth\b": "n",
        r"\bsouth\b": "s",
        r"\beast\b": "e",
        r"\bwest\b": "w",
    }
    for pattern, replacement in replacements.items():
        address = re.sub(pattern, replacement, address)
    # Remove punctuation except numbers
    address = re.sub(r"[^\w\s]", "", address)
    address = re.sub(r"\s+", " ", address).strip()
    return address


def extract_city_state(address: str) -> tuple[str, str]:
    """Extract city and state from a formatted address."""
    # Try to match "City, ST ZIPCODE" pattern
    match = re.search(r"([A-Za-z\s]+),\s*([A-Z]{2})\s*\d{5}?", address)
    if match:
        return match.group(1).strip(), match.group(2)
    
    # Try just "City, ST"
    match = re.search(r"([A-Za-z\s]+),\s*([A-Z]{2})", address)
    if match:
        return match.group(1).strip(), match.group(2)
    
    return "", ""


def similarity_score(s1: str, s2: str) -> float:
    """Calculate similarity between two strings (0-1)."""
    return SequenceMatcher(None, s1, s2).ratio()


class Deduplicator:
    """
    Deduplicates stores from multiple sources.
    
    Primary key: Google Place ID
    Secondary: Fuzzy match on normalized name + city
    """
    
    def __init__(self, similarity_threshold: float = 0.85):
        self.similarity_threshold = similarity_threshold
        self.stores: dict[str, Store] = {}  # keyed by canonical ID
        self._name_index: dict[str, list[str]] = {}  # normalized_name -> [store_ids]
    
    def _generate_id(self, store: Store) -> str:
        """Generate a canonical ID for a store."""
        if store.google_place_id:
            return f"gp_{store.google_place_id}"
        elif store.yelp_id:
            return f"yp_{store.yelp_id}"
        elif store.osm_id:
            return f"osm_{store.osm_id}"
        else:
            # Fallback: hash of name + address
            key = f"{normalize_name(store.name)}_{normalize_address(store.formatted_address)}"
            return f"hash_{hash(key) & 0xFFFFFFFF:08x}"
    
    def add(self, store_data: dict, source: str = "unknown") -> tuple[str, bool]:
        """
        Add a store, deduplicating if necessary.
        
        Args:
            store_data: Store data dictionary
            source: Source identifier (e.g., "google_places", "yelp")
        
        Returns:
            Tuple of (store_id, is_new)
        """
        store = Store.from_dict(store_data)
        if source not in store.sources:
            store.sources.append(source)
        
        # Check for exact ID match
        if store.google_place_id:
            for sid, existing in self.stores.items():
                if existing.google_place_id == store.google_place_id:
                    self._merge(existing, store)
                    return sid, False
        
        # Check for fuzzy name match
        normalized = normalize_name(store.name)
        city, state = extract_city_state(store.formatted_address)
        if not city:
            city = store.city
        
        for candidate_id in self._name_index.get(normalized, []):
            candidate = self.stores[candidate_id]
            cand_city, _ = extract_city_state(candidate.formatted_address)
            if not cand_city:
                cand_city = candidate.city
            
            # Same normalized name and similar city
            name_sim = similarity_score(normalized, normalize_name(candidate.name))
            city_sim = similarity_score(city.lower(), cand_city.lower()) if city and cand_city else 0
            
            if name_sim >= self.similarity_threshold and city_sim >= 0.8:
                self._merge(candidate, store)
                return candidate_id, False
        
        # New store
        store_id = self._generate_id(store)
        self.stores[store_id] = store
        
        # Index by normalized name
        if normalized not in self._name_index:
            self._name_index[normalized] = []
        self._name_index[normalized].append(store_id)
        
        return store_id, True
    
    def _merge(self, existing: Store, new: Store) -> None:
        """Merge new store data into existing record."""
        # Merge sources
        for src in new.sources:
            if src not in existing.sources:
                existing.sources.append(src)
        
        # Fill in missing fields
        if not existing.google_place_id and new.google_place_id:
            existing.google_place_id = new.google_place_id
        if not existing.yelp_id and new.yelp_id:
            existing.yelp_id = new.yelp_id
        if not existing.osm_id and new.osm_id:
            existing.osm_id = new.osm_id
        if not existing.phone and new.phone:
            existing.phone = new.phone
        if not existing.website and new.website:
            existing.website = new.website
        if not existing.latitude and new.latitude:
            existing.latitude = new.latitude
            existing.longitude = new.longitude
        
        # Prefer higher review count for ratings
        if new.google_review_count and (
            not existing.google_review_count or 
            new.google_review_count > existing.google_review_count
        ):
            existing.google_rating = new.google_rating
            existing.google_review_count = new.google_review_count
        
        if new.yelp_review_count and (
            not existing.yelp_review_count or 
            new.yelp_review_count > existing.yelp_review_count
        ):
            existing.yelp_rating = new.yelp_rating
            existing.yelp_review_count = new.yelp_review_count
        
        # Merge categories
        for cat in new.categories:
            if cat not in existing.categories:
                existing.categories.append(cat)
    
    def get_all(self) -> list[dict]:
        """Get all deduplicated stores as list of dicts."""
        return [store.to_dict() for store in self.stores.values()]
    
    def stats(self) -> dict:
        """Get deduplication statistics."""
        source_counts = {}
        for store in self.stores.values():
            for src in store.sources:
                source_counts[src] = source_counts.get(src, 0) + 1
        
        multi_source = sum(1 for s in self.stores.values() if len(s.sources) > 1)
        
        return {
            "total_unique": len(self.stores),
            "by_source": source_counts,
            "multi_source_stores": multi_source,
        }


if __name__ == "__main__":
    # Demo
    dedup = Deduplicator()
    
    # Add same store from different sources
    dedup.add({
        "name": "Pistol Creek West Boot Store",
        "formatted_address": "4350 St Andrews Rd, Columbia, SC 29210",
        "google_place_id": "abc123",
        "google_rating": 4.4,
    }, source="google_places")
    
    dedup.add({
        "name": "Pistol Creek West",
        "formatted_address": "4350 Saint Andrews Road, Columbia, SC",
        "yelp_rating": 4.5,
    }, source="yelp")
    
    dedup.add({
        "name": "La Herradura Western Wear",
        "formatted_address": "7380 Two Notch Rd, Columbia, SC",
        "google_place_id": "def456",
    }, source="google_places")
    
    print("Deduplication Demo")
    print("=" * 40)
    print(f"Stats: {dedup.stats()}")
    print()
    for store in dedup.get_all():
        print(f"  {store['name']}")
        print(f"    Sources: {store['sources']}")

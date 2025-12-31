# WesternWearMapping

Automated collection of western wear, cowboy boot, and cowboy hat store data across the USA.

## Overview

This project scrapes and aggregates data from multiple free sources (OpenStreetMap, Yelp, Google Places free tier, directories) to build a comprehensive database of western wear retailers.

## Data Sources (All Free)

- **OpenStreetMap** - Overpass API for POIs
- **Yelp Fusion API** - 5,000 requests/day free
- **Google Places API** - $200/month free credit
- **Nominatim** - Free geocoding
- **Web Scrapers** - Boot Barn, Cavender's, Yellow Pages

## Project Structure

```
/Mapping
├── config/              # API keys, search terms
├── src/
│   ├── collectors/      # API and scraper modules
│   ├── processing/      # Geocoding, deduplication
│   └── storage/         # Database, export
├── data/                # Raw and processed data
└── scripts/             # Main orchestration scripts
```

## Getting Started

1. Clone this repo
2. Set up API keys in `config/api_keys.env`
3. Install dependencies: `pip install -r requirements.txt`
4. Run pilot: `python scripts/1_collect_data.py --pilot-sc`

## License

MIT

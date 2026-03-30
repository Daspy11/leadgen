"""
Systematic collection of all medspas in London using Brightdata Google Maps API.

Strategy:
- Search across all 32 London boroughs + City of London + key neighborhoods
- Use multiple search terms to catch different naming conventions
- Deduplicate results by place name + address
- Validate and output clean CSV
"""

import requests
import json
import time
import csv
import os
import re
from datetime import datetime
from pathlib import Path

API_KEY = "341ea684-19fe-4879-a4aa-ffb8b85e1cb4"
DATASET_ID = "gd_m6gjtfmeh43we6cqc"
BASE_URL = "https://api.brightdata.com/datasets/v3"
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

OUTPUT_DIR = Path(r"c:/Users/stuar/code/leadgen/medspa")
SNAPSHOTS_FILE = OUTPUT_DIR / "snapshots.json"
RAW_RESULTS_FILE = OUTPUT_DIR / "raw_results.json"
FINAL_CSV = OUTPUT_DIR / "london_medspas.csv"

# Search terms that capture different naming conventions for medspas
SEARCH_TERMS = [
    "medspa",
    "med spa",
    "medical spa",
    "medical aesthetics clinic",
    "aesthetic clinic",
    "aesthetics clinic",
    "cosmetic clinic",
    "skin clinic botox",
    "dermal fillers clinic",
    "anti wrinkle injections clinic",
    "laser skin clinic",
    "beauty clinic botox fillers",
]

# All London boroughs + key areas for comprehensive coverage
LONDON_AREAS = [
    # 32 London Boroughs + City of London
    "City of London",
    "Barking and Dagenham",
    "Barnet",
    "Bexley",
    "Brent",
    "Bromley",
    "Camden",
    "Croydon",
    "Ealing",
    "Enfield",
    "Greenwich",
    "Hackney",
    "Hammersmith and Fulham",
    "Haringey",
    "Harrow",
    "Havering",
    "Hillingdon",
    "Hounslow",
    "Islington",
    "Kensington and Chelsea",
    "Kingston upon Thames",
    "Lambeth",
    "Lewisham",
    "Merton",
    "Newham",
    "Redbridge",
    "Richmond upon Thames",
    "Southwark",
    "Sutton",
    "Tower Hamlets",
    "Waltham Forest",
    "Wandsworth",
    "Westminster",
    # Key neighborhoods/high streets known for clinics
    "Harley Street London",
    "Mayfair London",
    "Chelsea London",
    "Knightsbridge London",
    "Notting Hill London",
    "Marylebone London",
    "Soho London",
    "Covent Garden London",
    "Fitzrovia London",
    "Belgravia London",
    "South Kensington London",
    "Wimbledon London",
    "Canary Wharf London",
    "Shoreditch London",
    "Angel Islington London",
    "Battersea London",
    "Fulham London",
    "Putney London",
    "Clapham London",
    "Brixton London",
    "Dulwich London",
    "Hampstead London",
    "Muswell Hill London",
    "Chiswick London",
    "Richmond London",
    "Stratford London",
    "Wembley London",
    "Finchley London",
    "Highgate London",
    "St Johns Wood London",
    "Primrose Hill London",
    "Kings Cross London",
]

# Only use the most distinctive search terms per area to avoid excessive API calls
# but ensure comprehensive coverage
CORE_SEARCH_TERMS = [
    "medspa",
    "med spa",
    "medical spa",
    "aesthetic clinic",
    "cosmetic clinic",
    "medical aesthetics",
]

EXTENDED_SEARCH_TERMS = [
    "skin clinic botox",
    "dermal fillers clinic",
    "anti wrinkle injections",
    "laser skin clinic",
]


def build_search_url(term, area):
    """Build a Google Maps search URL for a term in an area."""
    query = f"{term} in {area}".replace(" ", "+")
    return f"https://www.google.com/maps/search/{query}?brd_json=1"


def trigger_batch(urls, batch_name=""):
    """Trigger a batch of URLs and return the snapshot_id."""
    payload = [{"url": url} for url in urls]
    resp = requests.post(
        f"{BASE_URL}/trigger",
        params={"dataset_id": DATASET_ID, "include_errors": "true"},
        headers=HEADERS,
        json=payload,
    )
    if resp.status_code == 200:
        snap_id = resp.json().get("snapshot_id")
        print(f"  Triggered batch '{batch_name}': {snap_id} ({len(urls)} URLs)")
        return snap_id
    else:
        print(f"  ERROR triggering batch '{batch_name}': {resp.status_code} {resp.text[:200]}")
        return None


def check_snapshot(snapshot_id):
    """Check if a snapshot is ready. Returns (status, data)."""
    resp = requests.get(
        f"{BASE_URL}/snapshot/{snapshot_id}",
        params={"format": "json"},
        headers={"Authorization": f"Bearer {API_KEY}"},
    )
    if resp.status_code == 200:
        return "ready", resp.json()
    elif resp.status_code == 202:
        return "running", None
    elif resp.status_code == 400:
        # Could be empty or error
        text = resp.text.strip()
        if "empty" in text.lower():
            return "empty", None
        return "error", text
    else:
        return "error", resp.text[:200]


def parse_maps_result(result):
    """Parse a single result from the Brightdata Google Maps response."""
    businesses = []

    # The result has a 'markdown' field containing escaped JSON
    markdown = result.get("markdown", "")
    if not markdown:
        return businesses

    # Try to parse the JSON from markdown field
    try:
        # Remove markdown escaping
        cleaned = markdown.replace("\\_", "_").replace("\\[", "[").replace("\\]", "]")
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        # Try to extract JSON another way
        try:
            data = json.loads(markdown)
        except json.JSONDecodeError:
            return businesses

    # Extract organic results (business listings)
    organic = data.get("organic", [])
    for biz in organic:
        business = {
            "name": biz.get("title", "").strip(),
            "website": biz.get("link", "").strip(),
            "address": biz.get("address", "").strip(),
            "phone": biz.get("phone", "").strip(),
            "rating": biz.get("rating", ""),
            "reviews_count": biz.get("reviews", ""),
            "category": "",
            "display_link": biz.get("display_link", "").strip(),
        }

        # Extract category
        categories = biz.get("category", [])
        if categories and isinstance(categories, list):
            business["category"] = ", ".join(
                c.get("title", "") for c in categories if isinstance(c, dict)
            )
        elif isinstance(categories, str):
            business["category"] = categories

        if business["name"]:
            businesses.append(business)

    return businesses


def save_snapshots(snapshots):
    """Save snapshot tracking data."""
    with open(SNAPSHOTS_FILE, "w") as f:
        json.dump(snapshots, f, indent=2)


def load_snapshots():
    """Load snapshot tracking data."""
    if SNAPSHOTS_FILE.exists():
        with open(SNAPSHOTS_FILE) as f:
            return json.load(f)
    return {}


def phase1_trigger_searches():
    """Phase 1: Trigger all Google Maps searches."""
    print("=" * 60)
    print("PHASE 1: Triggering Google Maps searches")
    print("=" * 60)

    snapshots = load_snapshots()
    batch_size = 20  # URLs per batch to avoid overwhelming the API

    # Build all search URLs
    all_urls = []

    # Core terms across all boroughs
    for area in LONDON_AREAS:
        for term in CORE_SEARCH_TERMS:
            url = build_search_url(term, area)
            all_urls.append((url, f"{term} | {area}"))

    # Extended terms only for key medspa-dense areas
    key_areas = [
        "Harley Street London", "Mayfair London", "Chelsea London",
        "Kensington and Chelsea", "Westminster", "Camden",
        "Marylebone London", "Knightsbridge London", "Fitzrovia London",
        "South Kensington London", "Notting Hill London", "Belgravia London",
        "Islington", "Hackney", "Shoreditch London", "Canary Wharf London",
    ]
    for area in key_areas:
        for term in EXTENDED_SEARCH_TERMS:
            url = build_search_url(term, area)
            all_urls.append((url, f"{term} | {area}"))

    print(f"Total search URLs to trigger: {len(all_urls)}")

    # Filter out already-triggered searches
    triggered_urls = set()
    for snap_info in snapshots.values():
        for u in snap_info.get("urls", []):
            triggered_urls.add(u)

    new_urls = [(u, desc) for u, desc in all_urls if u not in triggered_urls]
    print(f"Already triggered: {len(all_urls) - len(new_urls)}")
    print(f"New to trigger: {len(new_urls)}")

    if not new_urls:
        print("All searches already triggered!")
        return snapshots

    # Batch and trigger
    batch_num = len(snapshots)
    for i in range(0, len(new_urls), batch_size):
        batch = new_urls[i:i + batch_size]
        batch_urls = [u for u, _ in batch]
        batch_name = f"batch_{batch_num}"

        snap_id = trigger_batch(batch_urls, batch_name)
        if snap_id:
            snapshots[snap_id] = {
                "batch_name": batch_name,
                "urls": batch_urls,
                "descriptions": [d for _, d in batch],
                "status": "running",
                "triggered_at": datetime.now().isoformat(),
            }
            save_snapshots(snapshots)
            batch_num += 1

        # Small delay between batches
        time.sleep(1)

    print(f"\nTriggered {batch_num - len(snapshots) + len(new_urls) // batch_size + 1} batches")
    save_snapshots(snapshots)
    return snapshots


def phase2_collect_results():
    """Phase 2: Poll and collect all results."""
    print("\n" + "=" * 60)
    print("PHASE 2: Collecting results from snapshots")
    print("=" * 60)

    snapshots = load_snapshots()
    all_raw_results = []

    if RAW_RESULTS_FILE.exists():
        try:
            with open(RAW_RESULTS_FILE, encoding="utf-8") as f:
                all_raw_results = json.load(f)
            print(f"Loaded {len(all_raw_results)} previously collected raw results")
        except (json.JSONDecodeError, Exception) as e:
            print(f"Warning: Could not load raw_results.json ({e}), starting fresh")
            all_raw_results = []

    collected_snaps = {r["snapshot_id"] for r in all_raw_results if "snapshot_id" in r}

    pending = {
        sid: info for sid, info in snapshots.items()
        if info.get("status") != "collected" and sid not in collected_snaps
    }

    print(f"Pending snapshots to check: {len(pending)}")

    max_retries = 40  # ~20 minutes of polling
    retry = 0

    while pending and retry < max_retries:
        still_pending = {}

        for snap_id, info in pending.items():
            status, data = check_snapshot(snap_id)

            if status == "ready":
                results = data if isinstance(data, list) else [data]
                for r in results:
                    r["snapshot_id"] = snap_id
                    r["batch_name"] = info.get("batch_name", "")
                all_raw_results.extend(results)
                snapshots[snap_id]["status"] = "collected"
                print(f"  Collected {info['batch_name']}: {len(results)} results")

            elif status == "empty":
                snapshots[snap_id]["status"] = "empty"
                print(f"  Empty: {info['batch_name']}")

            elif status == "error":
                snapshots[snap_id]["status"] = f"error: {data}"
                print(f"  Error: {info['batch_name']}: {data[:100]}")

            elif status == "running":
                still_pending[snap_id] = info

        pending = still_pending
        save_snapshots(snapshots)

        # Save intermediate results
        with open(RAW_RESULTS_FILE, "w", encoding="utf-8") as f:
            json.dump(all_raw_results, f, indent=2, ensure_ascii=False)

        if pending:
            retry += 1
            wait = 30
            print(f"\n  {len(pending)} snapshots still running. Waiting {wait}s... (retry {retry}/{max_retries})")
            time.sleep(wait)

    print(f"\nTotal raw results collected: {len(all_raw_results)}")
    return all_raw_results


def phase3_parse_and_deduplicate(raw_results):
    """Phase 3: Parse all results and deduplicate."""
    print("\n" + "=" * 60)
    print("PHASE 3: Parsing and deduplicating")
    print("=" * 60)

    all_businesses = []
    parse_errors = 0

    for result in raw_results:
        if "error" in result and "markdown" not in result:
            parse_errors += 1
            continue
        businesses = parse_maps_result(result)
        all_businesses.extend(businesses)

    print(f"Total parsed businesses (before dedup): {len(all_businesses)}")
    print(f"Parse errors/empty results: {parse_errors}")

    # Deduplicate by normalized name + address
    seen = {}
    for biz in all_businesses:
        # Normalize for dedup
        name_norm = re.sub(r'[^a-z0-9]', '', biz["name"].lower())
        addr_norm = re.sub(r'[^a-z0-9]', '', biz["address"].lower())
        key = f"{name_norm}|{addr_norm}"

        if key not in seen:
            seen[key] = biz
        else:
            # Merge: keep the one with more data
            existing = seen[key]
            if not existing["website"] and biz["website"]:
                existing["website"] = biz["website"]
            if not existing["phone"] and biz["phone"]:
                existing["phone"] = biz["phone"]
            if not existing["rating"] and biz["rating"]:
                existing["rating"] = biz["rating"]

    unique = list(seen.values())
    print(f"Unique businesses after dedup: {len(unique)}")

    return unique


def is_medspa_like(biz):
    """Filter to keep only businesses that are likely medspas/aesthetic clinics."""
    name = biz["name"].lower()
    category = biz["category"].lower()
    address = biz["address"].lower()

    # Must be in London area
    london_indicators = [
        "london", "sw1", "sw2", "sw3", "sw4", "sw5", "sw6", "sw7", "sw8", "sw9",
        "sw10", "sw11", "sw12", "sw13", "sw14", "sw15", "sw16", "sw17", "sw18", "sw19", "sw20",
        "se1", "se2", "se3", "se4", "se5", "se6", "se7", "se8", "se9",
        "se10", "se11", "se12", "se13", "se14", "se15", "se16", "se17", "se18",
        "se19", "se20", "se21", "se22", "se23", "se24", "se25", "se26", "se27", "se28",
        "w1", "w2", "w3", "w4", "w5", "w6", "w7", "w8", "w9", "w10", "w11", "w12", "w13", "w14",
        "wc1", "wc2",
        "e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8", "e9",
        "e10", "e11", "e12", "e13", "e14", "e15", "e16", "e17", "e18", "e20",
        "ec1", "ec2", "ec3", "ec4",
        "n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9",
        "n10", "n11", "n12", "n13", "n14", "n15", "n16", "n17", "n18", "n19", "n20", "n21", "n22",
        "nw1", "nw2", "nw3", "nw4", "nw5", "nw6", "nw7", "nw8", "nw9", "nw10", "nw11",
        "ha", "ub", "tw", "kt", "sm", "cr", "br", "da", "rm", "ig", "en",
    ]
    if address and not any(ind in address.lower() for ind in london_indicators):
        # If address exists but has no London indicator, skip
        # (unless address is just very short/incomplete)
        if len(address) > 10:
            return False

    # Positive signals - the business is a medspa or aesthetic clinic
    medspa_keywords = [
        "medspa", "med spa", "medical spa", "medispa", "medi spa",
        "aesthetic", "aesthetics", "cosmetic", "skin clinic",
        "beauty clinic", "laser clinic", "dermal", "botox",
        "anti-ageing", "anti-aging", "rejuvenation",
        "skin care clinic", "skin treatment", "facial aesthetic",
        "cosmetic surgery", "plastic surgery", "non-surgical",
        "body sculpt", "body contouring", "skin rejuvenation",
        "hair removal clinic", "wellness clinic",
    ]

    medspa_categories = [
        "skin care clinic", "beauty salon", "medical spa",
        "cosmetic", "aesthetic", "dermatolog", "plastic surg",
        "laser hair removal", "day spa", "wellness",
    ]

    # Check name and category
    name_match = any(kw in name for kw in medspa_keywords)
    category_match = any(kw in category for kw in medspa_categories)

    return name_match or category_match


def phase4_filter_and_output(businesses):
    """Phase 4: Filter for actual medspas and output clean CSV."""
    print("\n" + "=" * 60)
    print("PHASE 4: Filtering and producing final output")
    print("=" * 60)

    # Filter for medspa-like businesses
    medspas = [biz for biz in businesses if is_medspa_like(biz)]
    print(f"Businesses matching medspa criteria: {len(medspas)}")

    # Sort by name
    medspas.sort(key=lambda x: x["name"].lower())

    # Write CSV
    with open(FINAL_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "name", "website", "address", "phone", "category", "rating", "reviews_count"
        ])
        writer.writeheader()
        for biz in medspas:
            writer.writerow({
                "name": biz["name"],
                "website": biz["website"],
                "address": biz["address"],
                "phone": biz["phone"],
                "category": biz["category"],
                "rating": biz.get("rating", ""),
                "reviews_count": biz.get("reviews_count", ""),
            })

    print(f"\nFinal CSV written to: {FINAL_CSV}")
    print(f"Total medspas found: {len(medspas)}")

    # Also save the full unfiltered data for reference
    unfiltered_csv = OUTPUT_DIR / "london_medspas_unfiltered.csv"
    with open(unfiltered_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "name", "website", "address", "phone", "category", "rating", "reviews_count"
        ])
        writer.writeheader()
        for biz in businesses:
            writer.writerow({
                "name": biz["name"],
                "website": biz["website"],
                "address": biz["address"],
                "phone": biz["phone"],
                "category": biz["category"],
                "rating": biz.get("rating", ""),
                "reviews_count": biz.get("reviews_count", ""),
            })
    print(f"Unfiltered CSV written to: {unfiltered_csv}")
    print(f"Total businesses (unfiltered): {len(businesses)}")

    return medspas


def main():
    print(f"Starting medspa collection at {datetime.now().isoformat()}")
    print(f"Output directory: {OUTPUT_DIR}")

    # Phase 1: Trigger all searches
    snapshots = phase1_trigger_searches()

    # Phase 2: Collect all results
    raw_results = phase2_collect_results()

    # Phase 3: Parse and deduplicate
    businesses = phase3_parse_and_deduplicate(raw_results)

    # Phase 4: Filter and output
    medspas = phase4_filter_and_output(businesses)

    print("\n" + "=" * 60)
    print("COLLECTION COMPLETE")
    print("=" * 60)
    print(f"Finished at {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()

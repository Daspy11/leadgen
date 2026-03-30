"""
Systematic collection of medspa-adjacent specialty clinics in London
using Brightdata Google Maps API.

Covers: fertility/IVF, dermatology, hair transplant, cosmetic dentistry,
physiotherapy/sports medicine, IV drip/wellness, weight loss, hormone/TRT,
private psychiatry/mental health, laser eye surgery.
"""

import requests
import json
import time
from datetime import datetime
from pathlib import Path

API_KEY = "341ea684-19fe-4879-a4aa-ffb8b85e1cb4"
DATASET_ID = "gd_m6gjtfmeh43we6cqc"
BASE_URL = "https://api.brightdata.com/datasets/v3"
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

OUTPUT_DIR = Path(r"c:/Users/stuar/code/leadgen/specialty_clinics")
SNAPSHOTS_FILE = OUTPUT_DIR / "snapshots.json"
RAW_RESULTS_FILE = OUTPUT_DIR / "raw_results.json"

# All London boroughs + key neighborhoods
LONDON_AREAS = [
    "City of London",
    "Barking and Dagenham", "Barnet", "Bexley", "Brent", "Bromley",
    "Camden", "Croydon", "Ealing", "Enfield", "Greenwich", "Hackney",
    "Hammersmith and Fulham", "Haringey", "Harrow", "Havering",
    "Hillingdon", "Hounslow", "Islington", "Kensington and Chelsea",
    "Kingston upon Thames", "Lambeth", "Lewisham", "Merton", "Newham",
    "Redbridge", "Richmond upon Thames", "Southwark", "Sutton",
    "Tower Hamlets", "Waltham Forest", "Wandsworth", "Westminster",
    "Harley Street London", "Mayfair London", "Chelsea London",
    "Knightsbridge London", "Marylebone London", "Fitzrovia London",
    "South Kensington London", "Canary Wharf London",
    "Wimbledon London", "Hampstead London", "Chiswick London",
    "Richmond London", "Clapham London", "Fulham London",
    "Notting Hill London", "Battersea London", "Putney London",
    "Shoreditch London", "Angel Islington London",
]

# Core search terms — applied to ALL areas
CORE_SEARCH_TERMS = [
    "fertility clinic",
    "IVF clinic",
    "dermatology clinic",
    "private dermatologist",
    "hair transplant clinic",
    "cosmetic dentist",
    "dental veneers clinic",
    "invisalign dentist",
    "physiotherapy clinic",
    "sports medicine clinic",
    "IV drip clinic",
    "wellness clinic",
    "weight loss clinic",
    "hormone clinic",
    "TRT clinic",
    "private psychiatrist",
    "private mental health clinic",
    "laser eye surgery",
    "hair restoration clinic",
    "private GP clinic",
]

# Extended terms — only for key areas where these cluster
EXTENDED_SEARCH_TERMS = [
    "private ENT clinic",
    "allergy clinic",
    "sexual health clinic private",
    "testosterone clinic",
    "vitamin infusion clinic",
    "osteopath clinic",
    "chiropractor clinic",
    "acupuncture clinic",
    "hyperbaric oxygen therapy",
    "cryotherapy clinic",
    "stem cell therapy clinic",
    "PRP treatment clinic",
    "private blood test clinic",
    "health screening clinic",
    "nutrition clinic",
    "sleep clinic private",
]

KEY_AREAS = [
    "Harley Street London", "Mayfair London", "Chelsea London",
    "Kensington and Chelsea", "Westminster", "Camden",
    "Marylebone London", "Knightsbridge London", "Fitzrovia London",
    "South Kensington London", "Canary Wharf London",
    "Islington", "City of London", "Wimbledon London",
    "Richmond London", "Hampstead London",
]


def build_search_url(term, area):
    query = f"{term} in {area}".replace(" ", "+")
    return f"https://www.google.com/maps/search/{query}?brd_json=1"


def trigger_batch(urls, batch_name=""):
    payload = [{"url": url} for url in urls]
    resp = requests.post(
        f"{BASE_URL}/trigger",
        params={"dataset_id": DATASET_ID, "include_errors": "true"},
        headers=HEADERS,
        json=payload,
    )
    if resp.status_code == 200:
        snap_id = resp.json().get("snapshot_id")
        print(f"  Triggered {batch_name}: {snap_id} ({len(urls)} URLs)")
        return snap_id
    else:
        print(f"  ERROR {batch_name}: {resp.status_code} {resp.text[:200]}")
        return None


def check_snapshot(snapshot_id):
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
        if "empty" in resp.text.lower():
            return "empty", None
        return "error", resp.text[:200]
    else:
        return "error", resp.text[:200]


def save_snapshots(snapshots):
    with open(SNAPSHOTS_FILE, "w", encoding="utf-8") as f:
        json.dump(snapshots, f, indent=2)


def load_snapshots():
    if SNAPSHOTS_FILE.exists():
        with open(SNAPSHOTS_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def phase1_trigger():
    print("=" * 60)
    print("PHASE 1: Triggering Google Maps searches")
    print("=" * 60)

    snapshots = load_snapshots()
    batch_size = 20

    all_urls = []
    for area in LONDON_AREAS:
        for term in CORE_SEARCH_TERMS:
            all_urls.append((build_search_url(term, area), f"{term} | {area}"))

    for area in KEY_AREAS:
        for term in EXTENDED_SEARCH_TERMS:
            all_urls.append((build_search_url(term, area), f"{term} | {area}"))

    print(f"Total search URLs: {len(all_urls)}")

    triggered_urls = set()
    for info in snapshots.values():
        for u in info.get("urls", []):
            triggered_urls.add(u)

    new_urls = [(u, d) for u, d in all_urls if u not in triggered_urls]
    print(f"Already triggered: {len(all_urls) - len(new_urls)}")
    print(f"New to trigger: {len(new_urls)}")

    if not new_urls:
        print("All searches already triggered!")
        return snapshots

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
                "status": "running",
                "triggered_at": datetime.now().isoformat(),
            }
            save_snapshots(snapshots)
            batch_num += 1
        time.sleep(1)

    save_snapshots(snapshots)
    return snapshots


def phase2_collect():
    print("\n" + "=" * 60)
    print("PHASE 2: Collecting results")
    print("=" * 60)

    snapshots = load_snapshots()
    all_raw = []

    if RAW_RESULTS_FILE.exists():
        try:
            with open(RAW_RESULTS_FILE, encoding="utf-8") as f:
                all_raw = json.load(f)
            print(f"Loaded {len(all_raw)} previous results")
        except Exception:
            all_raw = []

    collected_snaps = {r["snapshot_id"] for r in all_raw if "snapshot_id" in r}
    pending = {
        sid: info for sid, info in snapshots.items()
        if info.get("status") != "collected" and sid not in collected_snaps
    }
    print(f"Pending snapshots: {len(pending)}")

    max_retries = 40
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
                all_raw.extend(results)
                snapshots[snap_id]["status"] = "collected"
                print(f"  Collected {info['batch_name']}: {len(results)} results")
            elif status == "empty":
                snapshots[snap_id]["status"] = "empty"
                print(f"  Empty: {info['batch_name']}")
            elif status == "error":
                snapshots[snap_id]["status"] = f"error: {data}"
                print(f"  Error: {info['batch_name']}: {str(data)[:100]}")
            elif status == "running":
                still_pending[snap_id] = info

        pending = still_pending
        save_snapshots(snapshots)
        with open(RAW_RESULTS_FILE, "w", encoding="utf-8") as f:
            json.dump(all_raw, f, indent=2, ensure_ascii=False)

        if pending:
            retry += 1
            print(f"\n  {len(pending)} still running. Waiting 30s... ({retry}/{max_retries})")
            time.sleep(30)

    # Retry any errored snapshots
    errored = {
        sid: info for sid, info in snapshots.items()
        if isinstance(info.get("status", ""), str) and info["status"].startswith("error")
    }
    if errored:
        print(f"\nRetrying {len(errored)} errored snapshots...")
        for sid, info in errored.items():
            urls = info.get("urls", [])
            if not urls:
                continue
            new_snap = trigger_batch(urls, f"{info['batch_name']}_retry")
            if new_snap:
                snapshots[new_snap] = {
                    "batch_name": f"{info['batch_name']}_retry",
                    "urls": urls,
                    "status": "running",
                }
                save_snapshots(snapshots)
                time.sleep(1)

        # Wait and collect retries
        time.sleep(90)
        for snap_id, info in snapshots.items():
            if info.get("status") != "running":
                continue
            status, data = check_snapshot(snap_id)
            if status == "ready":
                results = data if isinstance(data, list) else [data]
                for r in results:
                    r["snapshot_id"] = snap_id
                all_raw.extend(results)
                snapshots[snap_id]["status"] = "collected"
                print(f"  Collected retry {info['batch_name']}: {len(results)}")
            else:
                print(f"  Retry still {status}: {info['batch_name']}")

        save_snapshots(snapshots)
        with open(RAW_RESULTS_FILE, "w", encoding="utf-8") as f:
            json.dump(all_raw, f, indent=2, ensure_ascii=False)

    print(f"\nTotal raw results: {len(all_raw)}")
    return all_raw


def main():
    print(f"Starting specialty clinic collection at {datetime.now().isoformat()}")
    phase1_trigger()
    phase2_collect()
    print(f"\nFinished at {datetime.now().isoformat()}")
    print("Next: run build_final_list.py to parse, filter, and deduplicate.")


if __name__ == "__main__":
    main()

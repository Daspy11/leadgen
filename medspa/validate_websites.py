"""
Validate websites in the medspa list and remove entries without working websites.
Uses concurrent requests for speed.
"""

import csv
import sys
import io
import re
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

INPUT_CSV = Path(r'c:/Users/stuar/code/leadgen/medspa/london_medspas.csv')
OUTPUT_CSV = Path(r'c:/Users/stuar/code/leadgen/medspa/london_medspas.csv')
REJECTED_CSV = Path(r'c:/Users/stuar/code/leadgen/medspa/london_medspas_rejected.csv')

TIMEOUT = 15
MAX_WORKERS = 30


def clean_url(url):
    """Clean and normalize URL for validation."""
    url = url.strip()
    if not url:
        return ''
    # Remove tracking params
    url = re.sub(r'\?utm_.*$', '', url)
    url = re.sub(r'\?ref=.*$', '', url)
    url = re.sub(r'\?share&.*$', '', url)
    # Ensure protocol
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    return url


def check_website(row):
    """Check if a website is reachable. Returns (row, is_valid, reason)."""
    url = clean_url(row['website'])
    if not url:
        return row, False, 'no_website'

    # Skip social media profile links - these aren't real business websites
    social_domains = ['instagram.com', 'facebook.com', 'tiktok.com',
                      'twitter.com', 'x.com', 'linkedin.com',
                      'youtube.com', 'wa.me', 'api.whatsapp.com']
    url_lower = url.lower()
    for domain in social_domains:
        if domain in url_lower:
            return row, False, f'social_media:{domain}'

    # Booking platform links are acceptable websites

    try:
        resp = requests.head(url, timeout=TIMEOUT, allow_redirects=True,
                             headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
        if resp.status_code < 400:
            return row, True, f'ok:{resp.status_code}'
        # Try GET if HEAD fails (some servers don't support HEAD)
        resp = requests.get(url, timeout=TIMEOUT, allow_redirects=True,
                            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
                            stream=True)
        resp.close()
        if resp.status_code < 400:
            return row, True, f'ok_get:{resp.status_code}'
        return row, False, f'http_error:{resp.status_code}'
    except requests.exceptions.SSLError:
        # Try http:// fallback
        try:
            http_url = url.replace('https://', 'http://')
            resp = requests.head(http_url, timeout=TIMEOUT, allow_redirects=True,
                                 headers={'User-Agent': 'Mozilla/5.0'})
            if resp.status_code < 400:
                return row, True, f'ok_http:{resp.status_code}'
            return row, False, 'ssl_error'
        except Exception:
            return row, False, 'ssl_error'
    except requests.exceptions.ConnectionError:
        return row, False, 'connection_error'
    except requests.exceptions.Timeout:
        return row, False, 'timeout'
    except Exception as e:
        return row, False, f'error:{type(e).__name__}'


def main():
    # Read input
    with open(INPUT_CSV, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f'Total entries: {len(rows)}')
    no_website = [r for r in rows if not r['website'].strip()]
    has_website = [r for r in rows if r['website'].strip()]
    print(f'No website: {no_website.__len__()}')
    print(f'With website: {len(has_website)}')

    # Validate websites concurrently
    valid = []
    invalid = []
    reasons = {}

    print(f'\nValidating {len(has_website)} websites with {MAX_WORKERS} workers...')

    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(check_website, row): row for row in has_website}
        for future in as_completed(futures):
            row, is_valid, reason = future.result()
            done += 1
            if done % 100 == 0:
                print(f'  Checked {done}/{len(has_website)}...')

            reason_key = reason.split(':')[0]
            reasons[reason_key] = reasons.get(reason_key, 0) + 1

            if is_valid:
                # Clean the URL before saving
                row['website'] = clean_url(row['website'])
                valid.append(row)
            else:
                row['reject_reason'] = reason
                invalid.append(row)

    # Add no-website entries to invalid
    for r in no_website:
        r['reject_reason'] = 'no_website'
        invalid.append(r)
    reasons['no_website'] = len(no_website)

    print(f'\nResults:')
    print(f'  Valid (working website): {len(valid)}')
    print(f'  Invalid/removed: {len(invalid)}')
    print(f'\nReasons for rejection:')
    for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
        print(f'  {reason}: {count}')

    # Write valid entries
    valid.sort(key=lambda x: x['name'].lower())
    fieldnames = ['name', 'website', 'address', 'phone', 'category', 'rating', 'reviews_count']
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in valid:
            writer.writerow({k: r[k] for k in fieldnames})
    print(f'\nValid entries written to: {OUTPUT_CSV}')

    # Write rejected entries for reference
    invalid.sort(key=lambda x: x['name'].lower())
    rejected_fields = fieldnames + ['reject_reason']
    with open(REJECTED_CSV, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=rejected_fields)
        writer.writeheader()
        for r in invalid:
            writer.writerow({k: r.get(k, '') for k in rejected_fields})
    print(f'Rejected entries written to: {REJECTED_CSV}')


if __name__ == '__main__':
    main()

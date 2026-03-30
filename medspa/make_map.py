"""
Geocode medspa addresses via postcodes.io (free, no auth, bulk)
and render on an interactive Folium map.
"""

import csv
import re
import sys
import io
import json
import requests
import folium
from folium.plugins import MarkerCluster
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

INPUT_CSV = Path(r'c:/Users/stuar/code/leadgen/medspa/london_medspas.csv')
OUTPUT_HTML = Path(r'c:/Users/stuar/code/leadgen/medspa/london_medspas_map.html')


def extract_postcode(address):
    """Extract full UK postcode from address."""
    match = re.search(r'([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})', address.upper())
    if match:
        return match.group(1)
    return None


def bulk_geocode(postcodes):
    """Geocode up to 100 postcodes at once via postcodes.io."""
    resp = requests.post(
        'https://api.postcodes.io/postcodes',
        json={'postcodes': postcodes},
        timeout=15,
    )
    resp.raise_for_status()
    results = {}
    for item in resp.json()['result']:
        r = item['result']
        if r:
            results[item['query'].upper().replace(' ', '')] = (r['latitude'], r['longitude'])
    return results


def main():
    with open(INPUT_CSV, encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))

    print(f'Total medspas: {len(rows)}')

    # Extract postcodes
    print('Extracting postcodes...')
    for row in rows:
        row['_postcode'] = extract_postcode(row['address'])

    has_pc = [r for r in rows if r['_postcode']]
    no_pc = [r for r in rows if not r['_postcode']]
    print(f'  With postcode: {len(has_pc)}')
    print(f'  Without postcode: {len(no_pc)}')
    if no_pc:
        for r in no_pc[:5]:
            print(f'    No postcode: {r["name"][:40]} | {r["address"][:50]}')

    # Bulk geocode in batches of 100
    print('Geocoding via postcodes.io...')
    unique_postcodes = list({r['_postcode'].upper().replace(' ', '') for r in has_pc})
    print(f'  Unique postcodes: {len(unique_postcodes)}')

    all_coords = {}
    batch_size = 100
    for i in range(0, len(unique_postcodes), batch_size):
        batch = unique_postcodes[i:i + batch_size]
        results = bulk_geocode(batch)
        all_coords.update(results)
        done = min(i + batch_size, len(unique_postcodes))
        ok = len(results)
        print(f'  [{done}/{len(unique_postcodes)}] batch got {ok}/{len(batch)} coords')

    print(f'  Total geocoded postcodes: {len(all_coords)}/{len(unique_postcodes)}')

    # Attach coords to rows
    geocoded = []
    failed = 0
    for row in has_pc:
        pc_key = row['_postcode'].upper().replace(' ', '')
        if pc_key in all_coords:
            row['lat'], row['lng'] = all_coords[pc_key]
            geocoded.append(row)
        else:
            failed += 1

    print(f'\nGeocoded medspas: {len(geocoded)}')
    print(f'Failed: {failed + len(no_pc)}')

    # Build map
    print('Building map...')
    m = folium.Map(location=[51.5074, -0.1278], zoom_start=11, tiles='CartoDB positron')
    cluster = MarkerCluster(name='Medspas').add_to(m)

    for row in geocoded:
        name = row['name']
        website = row['website']
        address = row['address']
        phone = row.get('phone', '')
        rating = row.get('rating', '')
        category = row.get('category', '')

        popup_html = f"""
        <div style="width:280px;font-family:Arial,sans-serif;">
            <h4 style="margin:0 0 6px 0;color:#333;">{name}</h4>
            <p style="margin:2px 0;font-size:12px;color:#666;">{category}</p>
            <p style="margin:2px 0;font-size:12px;">{address}</p>
            {'<p style="margin:2px 0;font-size:12px;">' + phone + '</p>' if phone else ''}
            {'<p style="margin:2px 0;font-size:12px;">Rating: ' + str(rating) + '</p>' if rating else ''}
            <p style="margin:6px 0 0 0;">
                <a href="{website}" target="_blank" style="color:#0066cc;font-size:12px;">
                    {website[:60]}{'...' if len(website) > 60 else ''}
                </a>
            </p>
        </div>
        """

        folium.Marker(
            location=[row['lat'], row['lng']],
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=name,
            icon=folium.Icon(color='purple', icon='heart', prefix='fa'),
        ).add_to(cluster)

    folium.LayerControl().add_to(m)
    m.save(str(OUTPUT_HTML))
    print(f'Done! Map saved to: {OUTPUT_HTML}')


if __name__ == '__main__':
    main()

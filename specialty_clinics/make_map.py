"""
Geocode specialty clinics via postcodes.io and render interactive Folium map.
"""

import csv
import re
import sys
import io
import requests
import folium
from folium.plugins import MarkerCluster
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

INPUT_CSV = Path(r'c:/Users/stuar/code/leadgen/specialty_clinics/london_specialty_clinics.csv')
OUTPUT_HTML = Path(r'c:/Users/stuar/code/leadgen/specialty_clinics/london_specialty_clinics_map.html')


def extract_postcode(address):
    match = re.search(r'([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})', address.upper())
    return match.group(1) if match else None


def bulk_geocode(postcodes):
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

    print(f'Total clinics: {len(rows)}')

    for row in rows:
        row['_postcode'] = extract_postcode(row['address'])

    has_pc = [r for r in rows if r['_postcode']]
    no_pc = [r for r in rows if not r['_postcode']]
    print(f'  With postcode: {len(has_pc)}')
    print(f'  Without postcode: {len(no_pc)}')

    print('Geocoding via postcodes.io...')
    unique_postcodes = list({r['_postcode'].upper().replace(' ', '') for r in has_pc})
    print(f'  Unique postcodes: {len(unique_postcodes)}')

    all_coords = {}
    for i in range(0, len(unique_postcodes), 100):
        batch = unique_postcodes[i:i + 100]
        results = bulk_geocode(batch)
        all_coords.update(results)
        done = min(i + 100, len(unique_postcodes))
        print(f'  [{done}/{len(unique_postcodes)}] batch got {len(results)}/{len(batch)} coords')

    print(f'  Total geocoded: {len(all_coords)}/{len(unique_postcodes)}')

    geocoded = []
    for row in has_pc:
        pc_key = row['_postcode'].upper().replace(' ', '')
        if pc_key in all_coords:
            row['lat'], row['lng'] = all_coords[pc_key]
            geocoded.append(row)

    print(f'\nGeocoded clinics: {len(geocoded)}')
    print('Building map...')

    m = folium.Map(location=[51.5074, -0.1278], zoom_start=11, tiles='CartoDB positron')
    cluster = MarkerCluster(name='Specialty Clinics').add_to(m)

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
            icon=folium.Icon(color='blue', icon='plus-sign'),
        ).add_to(cluster)

    folium.LayerControl().add_to(m)
    m.save(str(OUTPUT_HTML))
    print(f'Done! Map saved to: {OUTPUT_HTML}')


if __name__ == '__main__':
    main()

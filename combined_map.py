"""
Combined interactive map: medspas + specialty clinics on separate layers.
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

MEDSPA_CSV = Path(r'c:/Users/stuar/code/leadgen/medspa/london_medspas.csv')
CLINIC_CSV = Path(r'c:/Users/stuar/code/leadgen/specialty_clinics/london_specialty_clinics.csv')
OUTPUT_HTML = Path(r'c:/Users/stuar/code/leadgen/london_leads_map.html')


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


def load_csv(path, label):
    with open(path, encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))
    print(f'{label}: {len(rows)} entries')
    return rows


def geocode_rows(rows, label):
    for row in rows:
        row['_postcode'] = extract_postcode(row['address'])

    has_pc = [r for r in rows if r['_postcode']]
    print(f'  {label} with postcode: {len(has_pc)}')

    unique_pcs = list({r['_postcode'].upper().replace(' ', '') for r in has_pc})
    return has_pc, unique_pcs


def make_popup(row):
    name = row['name']
    website = row['website']
    address = row['address']
    phone = row.get('phone', '')
    rating = row.get('rating', '')
    category = row.get('category', '')

    return f"""
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


def main():
    medspas = load_csv(MEDSPA_CSV, 'Medspas')
    clinics = load_csv(CLINIC_CSV, 'Specialty clinics')

    # Collect all unique postcodes across both datasets
    ms_rows, ms_pcs = geocode_rows(medspas, 'Medspas')
    cl_rows, cl_pcs = geocode_rows(clinics, 'Clinics')

    all_pcs = list(set(ms_pcs + cl_pcs))
    print(f'\nTotal unique postcodes to geocode: {len(all_pcs)}')

    all_coords = {}
    for i in range(0, len(all_pcs), 100):
        batch = all_pcs[i:i + 100]
        results = bulk_geocode(batch)
        all_coords.update(results)
        done = min(i + 100, len(all_pcs))
        print(f'  [{done}/{len(all_pcs)}] got {len(results)}/{len(batch)}')

    print(f'Geocoded: {len(all_coords)}/{len(all_pcs)}')

    # Build map
    print('Building map...')
    m = folium.Map(location=[51.5074, -0.1278], zoom_start=11, tiles='CartoDB positron')

    # Medspa layer — purple hearts
    ms_cluster = MarkerCluster(name='Medspas (purple)', show=True).add_to(m)
    ms_mapped = 0
    for row in ms_rows:
        pc_key = row['_postcode'].upper().replace(' ', '')
        if pc_key not in all_coords:
            continue
        lat, lng = all_coords[pc_key]
        folium.Marker(
            location=[lat, lng],
            popup=folium.Popup(make_popup(row), max_width=300),
            tooltip=row['name'],
            icon=folium.Icon(color='purple', icon='heart', prefix='fa'),
        ).add_to(ms_cluster)
        ms_mapped += 1

    # Specialty clinic layer — blue plus signs
    cl_cluster = MarkerCluster(name='Specialty Clinics (blue)', show=True).add_to(m)
    cl_mapped = 0
    for row in cl_rows:
        pc_key = row['_postcode'].upper().replace(' ', '')
        if pc_key not in all_coords:
            continue
        lat, lng = all_coords[pc_key]
        folium.Marker(
            location=[lat, lng],
            popup=folium.Popup(make_popup(row), max_width=300),
            tooltip=row['name'],
            icon=folium.Icon(color='blue', icon='plus-sign'),
        ).add_to(cl_cluster)
        cl_mapped += 1

    folium.LayerControl(collapsed=False).add_to(m)

    m.save(str(OUTPUT_HTML))
    print(f'\nMedspas on map: {ms_mapped}')
    print(f'Clinics on map: {cl_mapped}')
    print(f'Map saved to: {OUTPUT_HTML}')


if __name__ == '__main__':
    main()

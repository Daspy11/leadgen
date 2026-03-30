"""
Phase 3+4: Parse raw Brightdata results, deduplicate, filter for London medspas,
and output clean CSV.
"""

import json
import csv
import re
import sys
import io
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

OUTPUT_DIR = Path(r'c:/Users/stuar/code/leadgen/medspa')
RAW_RESULTS_FILE = OUTPUT_DIR / 'raw_results.json'
FINAL_CSV = OUTPUT_DIR / 'london_medspas.csv'
ALL_CSV = OUTPUT_DIR / 'london_medspas_all_businesses.csv'

# Translations of "United Kingdom" to normalize
UK_VARIANTS = [
    'United Kingdom', 'Vương quốc Anh', 'Reino Unido', 'Royaume-Uni',
    'Vereinigtes Königreich', 'Birleşik Krallık', 'المملكة المتحدة',
    'Wielka Brytania', 'Regatul Unit', 'Великобритания',
    'Ηνωμένο Βασίλειο', 'Spojené království', 'Storbritannia',
    'Storbritannien', 'Iso-Britannia', 'Egyesült Királyság',
    'Apvienotā Karaliste', 'Обединено кралство',
    'Jungtinė Karalystė', 'İngiltere',
    'Spojené kráľovstvo', 'Regno Unito', 'Verenigd Koninkrijk',
    'Inggris Raya', 'Yhdistynyt kuningaskunta', 'Zjednoczone Królestwo',
    'Birleşik Krallık', 'Ühendkuningriik', 'Spojené kráľovstvo',
    'Apvienotā Karaliste', 'États-Unis',
    'UK', 'U.K.', 'England',
]

# Non-UK country indicators that prove a result is NOT in London
NON_UK_COUNTRIES = [
    'United States', 'Estados Unidos', 'Hoa Kỳ', 'Amerika Birleşik Devletleri',
    'États-Unis', 'الولايات المتحدة', 'Vereinigte Staaten',
    'Canada', 'Kanada', 'كندا',
    'Australia', 'Australie', 'Australien',
    'New Zealand', 'France', 'Germany', 'Italy', 'España', 'Spain',
    'Ireland', 'Irland', 'Irlande',
    ', NSW ', ', VIC ', ', QLD ', ', SA ', ', WA ', ', TAS ',
    ', CA ', ', CO ', ', CT ', ', MD ', ', NY ', ', NJ ', ', TX ',
    ', FL ', ', PA ', ', MA ', ', OH ', ', VA ', ', WA ',
    ', BC ', ', ON ', ', AB ', ', QC ',
]


def normalize_address(address):
    """Normalize address by replacing translated country names with English."""
    if not address:
        return address
    result = address
    for variant in UK_VARIANTS:
        if variant in result:
            result = result.replace(variant, 'United Kingdom')
            break
    return result.strip()


def parse_all_results():
    """Parse all raw results into business dicts."""
    with open(RAW_RESULTS_FILE, encoding='utf-8') as f:
        raw_results = json.load(f)

    print(f'Total raw results: {len(raw_results)}')
    all_businesses = []
    errors = 0

    for result in raw_results:
        markdown = result.get('markdown', '')
        if not markdown:
            if 'error' in result:
                errors += 1
            continue

        try:
            cleaned = markdown.replace(r'\_', '_').replace(r'\[', '[').replace(r'\]', ']')
            data = json.loads(cleaned)
        except json.JSONDecodeError:
            try:
                data = json.loads(markdown)
            except json.JSONDecodeError:
                errors += 1
                continue

        organic = data.get('organic', [])
        for biz in organic:
            address_raw = biz.get('address', '').strip()
            address = normalize_address(address_raw)

            business = {
                'name': biz.get('title', '').strip(),
                'website': biz.get('link', '').strip(),
                'address': address,
                'phone': biz.get('phone', '').strip(),
                'rating': str(biz.get('rating', '')).strip(),
                'reviews_count': str(biz.get('reviews', '')).strip(),
                'category': '',
            }

            categories = biz.get('category', [])
            if categories and isinstance(categories, list):
                # Only take English category names (filter non-Latin script)
                cat_names = []
                for c in categories:
                    if isinstance(c, dict):
                        title = c.get('title', '')
                        # Keep category if it's primarily Latin characters
                        if title and re.search(r'[a-zA-Z]', title):
                            cat_names.append(title)
                            break  # Just take the first English one
                business['category'] = ', '.join(cat_names)
            elif isinstance(categories, str):
                business['category'] = categories

            if business['name']:
                all_businesses.append(business)

    print(f'Parsed businesses: {len(all_businesses)}')
    print(f'Errors: {errors}')
    return all_businesses


def normalize_website(url):
    """Normalize website URL for dedup comparison."""
    if not url:
        return ''
    # Remove protocol, www, trailing slash, tracking params
    u = re.sub(r'https?://(www\.)?', '', url)
    u = re.sub(r'\?utm_.*$', '', u)
    u = re.sub(r'\?ref=.*$', '', u)
    u = u.rstrip('/')
    return u.lower()


def deduplicate(businesses):
    """Deduplicate by normalized name + address, then merge by website."""
    # Pass 1: Dedup by name + address (ignoring country suffix)
    seen = {}
    for biz in businesses:
        name_norm = re.sub(r'[^a-z0-9]', '', biz['name'].lower())

        addr = biz['address']
        # Remove country suffix for comparison
        addr_clean = re.sub(
            r',?\s*(United Kingdom|UK|England)\.?\s*$', '',
            addr, flags=re.IGNORECASE
        )
        # Also remove Arabic comma variant
        addr_clean = addr_clean.rstrip('، ').rstrip(', ')
        addr_norm = re.sub(r'[^a-z0-9]', '', addr_clean.lower())

        key = f'{name_norm}|{addr_norm}'

        if key not in seen:
            seen[key] = biz
        else:
            existing = seen[key]
            for field in ['website', 'phone', 'rating', 'reviews_count', 'category']:
                if not existing[field] and biz[field]:
                    existing[field] = biz[field]
            # Prefer address with "United Kingdom"
            if 'United Kingdom' in biz['address'] and 'United Kingdom' not in existing['address']:
                existing['address'] = biz['address']

    unique = list(seen.values())
    print(f'After name+address dedup: {len(unique)}')
    return unique


def is_in_london(address):
    """Check if address is in Greater London, UK (not US/AU cities with same names)."""
    if not address:
        return False

    # Reject if address contains non-UK country indicators
    for indicator in NON_UK_COUNTRIES:
        if indicator in address:
            return False

    addr_upper = address.upper()

    if 'LONDON' in addr_upper:
        return True

    # London postcode areas (inner)
    london_inner = [
        'SW', 'SE', 'WC', 'EC', 'NW',
    ]
    # Need number after prefix
    for prefix in london_inner:
        if re.search(r'\b' + prefix + r'\d', addr_upper):
            return True

    # W, E, N postcodes (need careful matching to avoid false positives)
    for prefix in ['W', 'E', 'N']:
        if re.search(r'\b' + prefix + r'\d{1,2}\b', addr_upper):
            return True

    # Explicitly exclude non-London UK postcodes that might appear
    non_london_postcodes = [
        'CM', 'SS', 'CO', 'CB', 'SG', 'AL', 'HP', 'SL', 'RG',
        'GU', 'RH', 'TN', 'ME', 'CT', 'BN', 'PO', 'SO', 'SP',
        'OX', 'MK', 'LU', 'BD', 'LS', 'HD', 'WF', 'DN', 'S1',
    ]
    for prefix in non_london_postcodes:
        if re.search(r'\b' + prefix + r'\d', addr_upper):
            return False

    # London outer postcode areas
    london_outer = ['HA', 'UB', 'TW', 'KT', 'SM', 'CR', 'BR', 'DA', 'RM', 'IG', 'EN']
    for prefix in london_outer:
        if re.search(r'\b' + prefix + r'\d', addr_upper):
            return True

    # London borough names
    london_boroughs = [
        'WESTMINSTER', 'CAMDEN', 'ISLINGTON', 'HACKNEY', 'TOWER HAMLETS',
        'GREENWICH', 'LEWISHAM', 'SOUTHWARK', 'LAMBETH', 'WANDSWORTH',
        'HAMMERSMITH', 'FULHAM', 'KENSINGTON', 'CHELSEA', 'BRENT',
        'EALING', 'HOUNSLOW', 'RICHMOND', 'KINGSTON', 'MERTON',
        'SUTTON', 'CROYDON', 'BROMLEY', 'BEXLEY', 'HAVERING',
        'BARKING', 'DAGENHAM', 'REDBRIDGE', 'NEWHAM', 'WALTHAM FOREST',
        'HARINGEY', 'ENFIELD', 'BARNET', 'HARROW', 'HILLINGDON',
        'MAYFAIR', 'SOHO', 'MARYLEBONE', 'FITZROVIA', 'NOTTING HILL',
        'KNIGHTSBRIDGE', 'BELGRAVIA', 'SHOREDITCH', 'CANARY WHARF',
        'BATTERSEA', 'CLAPHAM', 'BRIXTON', 'DULWICH', 'HAMPSTEAD',
        'MUSWELL HILL', 'CHISWICK', 'PUTNEY', 'WIMBLEDON', 'STRATFORD',
        'WEMBLEY', 'FINCHLEY', 'HIGHGATE', 'PRIMROSE HILL',
        'SURBITON', 'TWICKENHAM', 'HORNCHURCH', 'ROMFORD',
        'ILFORD', 'EDGWARE', 'STANMORE', 'PINNER',
    ]
    if any(borough in addr_upper for borough in london_boroughs):
        return True

    return False


def is_medspa(biz):
    """
    Determine if a business is a medspa / aesthetic clinic.
    Include: medical spas, aesthetic clinics, cosmetic clinics, skin clinics,
    laser clinics, dermatologists, cosmetic surgeons, facial spas, beauty clinics
    offering aesthetic treatments.
    Exclude: pure hair salons, nail bars, regular massage only, tattoo shops, etc.
    """
    name = biz['name'].lower()
    category = biz['category'].lower()

    # Exclusions first
    exclude_keywords = [
        'tattoo', 'barber', 'nail bar', 'nail salon', 'nail art',
        'pet ', 'veterinary', 'dental', 'dentist', 'optician',
        'pharmacy', 'physiotherapy', 'chiropract', 'osteopath',
        'florist', 'restaurant', 'cafe', 'hotel ', 'gym ',
        'fitness', 'pilates', 'yoga studio', 'car wash',
        'laundry', 'dry clean', 'solicitor', 'accountant',
        'estate agent', 'property', 'insurance', 'bank',
    ]
    if any(kw in name for kw in exclude_keywords):
        return False

    # Strong positive name keywords
    strong_name = [
        'medspa', 'med spa', 'medispa', 'medi spa', 'medi-spa',
        'medical spa', 'medical aesthetic', 'aesthetic clinic',
        'aesthetics clinic', 'cosmetic clinic', 'skin clinic',
        'laser clinic', 'dermal filler', 'botox', 'anti-ageing clinic',
        'anti-aging clinic', 'rejuvenation clinic', 'cosmetic surgery',
        'plastic surgery', 'non-surgical', 'body sculpt',
        'body contouring', 'skin rejuvenation', 'facial aesthetic',
        'aesthetic centre', 'aesthetic center', 'aesthetics centre',
        'aesthetic studio', 'aesthetics studio', 'aesthetics london',
        'cosmetic dermatology', 'aesthetic dermatology',
        'aesthetic medicine', 'beauty clinic',
        'wellness clinic', 'skin care clinic', 'skincare clinic',
        'skin treatment', 'laser hair removal', 'laser skin',
        'hydrafacial', 'microneedling', 'cosmetic treatment',
        'aesthetic surgery',
    ]
    if any(kw in name for kw in strong_name):
        return True

    # Strong positive categories
    strong_categories = [
        'skin care clinic', 'medical spa', 'facial spa',
        'laser hair removal', 'dermatologist',
        'cosmetic surg', 'plastic surg', 'medical clinic',
        'day spa',
    ]
    if any(kw in category for kw in strong_categories):
        return True

    # Translated strong categories
    translated_positive = [
        'clínica dermatológica', 'cơ sở chăm sóc da', 'cilt bakım kliniği',
        'spa terapéutico', 'thẩm mỹ viện', 'spa y tế', 'clinique dermatologique',
        'centro de estética', 'spa mặt', 'esteticista facial', 'spa médical',
        'institut de beauté',
    ]
    if any(kw in category for kw in translated_positive):
        return True

    # Moderate signals in name + any beauty/spa category
    moderate_name = ['aesthetic', 'skin', 'cosmetic', 'derma', 'laser',
                     'botox', 'filler', 'rejuven', 'clinic', 'spa']
    beauty_cats = ['beauty salon', 'beautician', 'massage spa', 'spa',
                   'waxing', 'hair removal']

    if any(kw in name for kw in moderate_name) and any(kw in category for kw in beauty_cats):
        return True

    # "Spa" in category alone (these are often aesthetic spas)
    if 'spa' == category.strip() and any(kw in name for kw in ['skin', 'beauty', 'glow', 'radiance', 'rejuven', 'clinic', 'aesthetic']):
        return True

    return False


def clean_website(url):
    """Clean up website URL."""
    if not url:
        return ''
    # Remove tracking parameters
    url = re.sub(r'\?utm_source=.*$', '', url)
    url = re.sub(r'\?ref=.*$', '', url)
    return url


def write_csv(businesses, filepath, label):
    """Write businesses to CSV."""
    businesses.sort(key=lambda x: x['name'].lower())
    with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'name', 'website', 'address', 'phone', 'category', 'rating', 'reviews_count'
        ])
        writer.writeheader()
        for biz in businesses:
            writer.writerow({
                'name': biz['name'],
                'website': clean_website(biz['website']),
                'address': biz['address'],
                'phone': biz['phone'],
                'category': biz['category'],
                'rating': biz.get('rating', ''),
                'reviews_count': biz.get('reviews_count', ''),
            })
    print(f'{label}: {len(businesses)} rows -> {filepath}')


def main():
    print('=' * 60)
    print('BUILDING FINAL MEDSPA LIST')
    print('=' * 60)

    # Parse
    all_businesses = parse_all_results()

    # Deduplicate
    unique = deduplicate(all_businesses)

    # Filter to London only
    london_businesses = [b for b in unique if is_in_london(b['address'])]
    print(f'In London: {len(london_businesses)}')

    non_london = [b for b in unique if not is_in_london(b['address'])]
    if non_london:
        print(f'Excluded (not London): {len(non_london)}')
        print('  Sample excluded addresses:')
        for b in non_london[:5]:
            print(f'    {b["name"]}: {b["address"]}')

    # Final dedup: same website + same address = same business
    final_dedup = {}
    for b in london_businesses:
        w = normalize_website(b['website'])
        a = re.sub(r'[^a-z0-9]', '', b['address'].lower())
        key = f'{w}|{a}' if w else f'no_website|{b["name"].lower()}|{a}'
        if key not in final_dedup:
            final_dedup[key] = b
        else:
            # Keep the one with shorter name (less noise)
            if len(b['name']) < len(final_dedup[key]['name']):
                final_dedup[key] = b
    london_businesses = list(final_dedup.values())
    print(f'After final website+address dedup: {len(london_businesses)}')

    # Filter for medspas
    medspas = [b for b in london_businesses if is_medspa(b)]
    print(f'Medspas/aesthetic clinics: {len(medspas)}')

    # Stats
    has_website = sum(1 for b in medspas if b['website'])
    has_address = sum(1 for b in medspas if b['address'])
    has_phone = sum(1 for b in medspas if b['phone'])
    print(f'\nWith website: {has_website}/{len(medspas)}')
    print(f'With address: {has_address}/{len(medspas)}')
    print(f'With phone: {has_phone}/{len(medspas)}')

    # Write CSVs
    write_csv(medspas, FINAL_CSV, 'Medspas (filtered)')
    write_csv(london_businesses, ALL_CSV, 'All London businesses (unfiltered)')

    # Show sample
    print('\nSample medspas (first 20):')
    for b in medspas[:20]:
        w = clean_website(b['website'])
        print(f'  {b["name"][:40]:<42} {w[:45]:<47} {b["address"][:55]}')

    # Category breakdown
    cats = {}
    for b in medspas:
        for c in b['category'].split(', '):
            c = c.strip()
            if c:
                cats[c] = cats.get(c, 0) + 1
    print(f'\nCategory breakdown (top 15):')
    for cat, count in sorted(cats.items(), key=lambda x: -x[1])[:15]:
        print(f'  {count:4d} {cat}')


if __name__ == '__main__':
    main()

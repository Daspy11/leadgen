"""
Parse, deduplicate, and filter specialty clinic results from Brightdata.
"""

import json
import csv
import re
import sys
import io
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

OUTPUT_DIR = Path(r'c:/Users/stuar/code/leadgen/specialty_clinics')
RAW_RESULTS_FILE = OUTPUT_DIR / 'raw_results.json'
FINAL_CSV = OUTPUT_DIR / 'london_specialty_clinics.csv'
ALL_CSV = OUTPUT_DIR / 'london_specialty_clinics_all.csv'

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
    'Ühendkuningriik',
    'UK', 'U.K.', 'England',
]

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
    if not address:
        return address
    result = address
    for variant in UK_VARIANTS:
        if variant in result:
            result = result.replace(variant, 'United Kingdom')
            break
    return result.strip()


def parse_all_results():
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

        for biz in data.get('organic', []):
            address = normalize_address(biz.get('address', '').strip())
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
                for c in categories:
                    if isinstance(c, dict):
                        title = c.get('title', '')
                        if title and re.search(r'[a-zA-Z]', title):
                            business['category'] = title
                            break
            elif isinstance(categories, str):
                business['category'] = categories

            if business['name']:
                all_businesses.append(business)

    print(f'Parsed businesses: {len(all_businesses)}')
    print(f'Errors: {errors}')
    return all_businesses


def normalize_website(url):
    if not url:
        return ''
    u = re.sub(r'https?://(www\.)?', '', url)
    u = re.sub(r'\?utm_.*$', '', u)
    u = re.sub(r'\?ref=.*$', '', u)
    u = u.rstrip('/')
    return u.lower()


def deduplicate(businesses):
    seen = {}
    for biz in businesses:
        name_norm = re.sub(r'[^a-z0-9]', '', biz['name'].lower())
        addr = biz['address']
        addr_clean = re.sub(r',?\s*(United Kingdom|UK|England)\.?\s*$', '', addr, flags=re.IGNORECASE)
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
            if 'United Kingdom' in biz['address'] and 'United Kingdom' not in existing['address']:
                existing['address'] = biz['address']

    unique = list(seen.values())
    print(f'After name+address dedup: {len(unique)}')
    return unique


def is_in_london(address):
    if not address:
        return False
    for indicator in NON_UK_COUNTRIES:
        if indicator in address:
            return False

    addr_upper = address.upper()
    if 'LONDON' in addr_upper:
        return True

    for prefix in ['SW', 'SE', 'WC', 'EC', 'NW']:
        if re.search(r'\b' + prefix + r'\d', addr_upper):
            return True
    for prefix in ['W', 'E', 'N']:
        if re.search(r'\b' + prefix + r'\d{1,2}\b', addr_upper):
            return True

    non_london_postcodes = [
        'CM', 'SS', 'CO', 'CB', 'SG', 'AL', 'HP', 'SL', 'RG',
        'GU', 'RH', 'TN', 'ME', 'CT', 'BN', 'PO', 'SO', 'SP',
        'OX', 'MK', 'LU', 'BD', 'LS', 'HD', 'WF', 'DN', 'S1',
    ]
    for prefix in non_london_postcodes:
        if re.search(r'\b' + prefix + r'\d', addr_upper):
            return False

    london_outer = ['HA', 'UB', 'TW', 'KT', 'SM', 'CR', 'BR', 'DA', 'RM', 'IG', 'EN']
    for prefix in london_outer:
        if re.search(r'\b' + prefix + r'\d', addr_upper):
            return True

    london_places = [
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
    return any(p in addr_upper for p in london_places)


def is_specialty_clinic(biz):
    """
    Match medspa-adjacent specialty clinics:
    fertility/IVF, dermatology, hair transplant, cosmetic dentistry,
    physiotherapy/sports med, IV drip/wellness, weight loss, hormone/TRT,
    private psychiatry, laser eye, etc.

    Exclude: NHS GPs, hospitals, pharmacies, vets, pure medspas (separate dataset).
    """
    name = biz['name'].lower()
    category = biz['category'].lower()

    # Exclude
    excludes = [
        'nhs', 'hospital', 'pharmacy', 'veterinary', 'pet ',
        'car wash', 'restaurant', 'cafe', 'hotel ', 'estate agent',
        'solicitor', 'accountant', 'bank', 'insurance',
        'tattoo', 'barber', 'nail bar', 'nail salon',
        'florist', 'laundry', 'dry clean', 'gym ', 'fitness',
        'yoga studio', 'pilates studio',
    ]
    if any(kw in name for kw in excludes):
        return False

    # Exclude dentists / dental clinics
    dental_keywords = [
        'dental', 'dentist', 'dentistry', 'orthodont', 'teeth',
        'tooth', 'oral surgery', 'oral health', 'endodont',
        'periodon', 'prosthodon',
    ]
    if any(kw in name for kw in dental_keywords):
        return False
    if any(kw in category for kw in ['dentist', 'dental', 'orthodont']):
        return False

    # Exclude generic doctors / GP surgeries
    gp_keywords = [
        'general practice', 'family doctor', 'family medicine',
        'gp surgery', 'gp practice', 'medical practice',
    ]
    if any(kw in name for kw in gp_keywords):
        return False
    if category in ['doctor', 'physician', 'general practitioner',
                     'family practice physician']:
        return False

    # Exclude psychologists, therapists, psychiatry, and conservative clinical types
    psych_keywords = [
        'psychiatr', 'psycholog', 'psychotherapy', 'psychotherapist',
        'mental health', 'counselling', 'counseling', 'counsellor',
        'therapist', 'therapy clinic', 'cbt ', 'cbt clinic',
        'talking therapy', 'cognitive behav',
        'physiotherapy', 'physio clinic', 'physical therap',
        'osteopath', 'chiropract', 'podiatr', 'chiropod',
        'optometrist', 'optician', 'hearing aid', 'audiol',
        'speech therap', 'occupational therap',
        'acupuncture', 'chinese medicine', 'homeopath', 'naturopath',
        'reflexolog', 'hypnotherap',
    ]
    if any(kw in name for kw in psych_keywords):
        return False
    psych_cats = [
        'psychiatr', 'psycholog', 'psychotherap', 'counsell',
        'mental health', 'therapist',
        'physiotherap', 'physical therap', 'osteopath', 'chiropract',
        'podiatr', 'optometrist', 'optician', 'audiol',
        'acupunctur', 'speech', 'occupational',
        'homeopath', 'naturopath', 'reflexolog', 'hypnotherap',
    ]
    if any(kw in category for kw in psych_cats):
        return False

    # Exclude pure medspas (already in the other dataset)
    medspa_exact = ['medspa', 'med spa', 'medispa', 'medi spa', 'medi-spa', 'medical spa']
    if any(kw in name for kw in medspa_exact):
        return False

    # Strong name matches
    strong_name = [
        # Fertility / IVF
        'fertility', 'ivf', 'reproductive', 'embryo',
        # Dermatology
        'dermatolog', 'skin doctor', 'skin specialist',
        # Hair transplant
        'hair transplant', 'hair restoration', 'hair loss clinic',
        'hair clinic', 'tricholog',
        # (Dentists excluded per user request)
        # Sports medicine (physio/osteo/chiro excluded)
        'sports medicine', 'sports injury', 'sports clinic',
        # IV drip / wellness
        'iv drip', 'iv therapy', 'vitamin infusion', 'wellness clinic',
        'health clinic', 'vitality clinic', 'biohack',
        # Weight loss
        'weight loss', 'weight management', 'slimming clinic',
        'body transformation', 'bariatric',
        # Hormone / TRT
        'hormone', 'trt clinic', 'testosterone', 'endocrin',
        'bioidentical', 'hrt clinic', 'menopause clinic',
        # (Mental health / psych excluded)
        # Laser eye
        'laser eye', 'lasik', 'eye surgery', 'ophthalmolog',
        'vision clinic', 'eye clinic',
        # Other adjacent
        'allergy clinic', 'ent clinic', 'ear nose throat',
        'sexual health', 'std clinic', 'sti clinic',
        'blood test', 'health screening', 'health check',
        'nutrition clinic', 'dietitian', 'nutritionist',
        'sleep clinic', 'hyperbaric', 'cryotherapy',
        'stem cell', 'prp treatment', 'regenerative',
        'concierge doctor',
        'private clinic', 'medical centre', 'medical center',
    ]
    if any(kw in name for kw in strong_name):
        return True

    # Strong category matches
    strong_cats = [
        'fertility', 'ivf', 'dermatolog', 'hair replacement',
        'ophthalmolog', 'eye care',
        'medical clinic', 'medical center', 'medical centre',
        'weight loss', 'nutritionist', 'dietitian',
        'sports medicine',
        'allergy', 'ent ', 'endocrin',
    ]
    if any(kw in category for kw in strong_cats):
        return True

    # Moderate: "clinic" in name + relevant category
    if 'clinic' in name:
        clinic_cats = ['health', 'medical', 'wellness', 'therapy',
                       'treatment', 'doctor', 'specialist']
        if any(kw in category for kw in clinic_cats):
            return True

    return False


def clean_website(url):
    if not url:
        return ''
    url = re.sub(r'\?utm_source=.*$', '', url)
    url = re.sub(r'\?ref=.*$', '', url)
    return url


def write_csv(businesses, filepath, label):
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
    print('BUILDING FINAL SPECIALTY CLINICS LIST')
    print('=' * 60)

    all_businesses = parse_all_results()
    unique = deduplicate(all_businesses)

    london = [b for b in unique if is_in_london(b['address'])]
    print(f'In London: {len(london)}')

    non_london = [b for b in unique if not is_in_london(b['address'])]
    print(f'Excluded (not London): {len(non_london)}')

    # Final website+address dedup
    final_dedup = {}
    for b in london:
        w = normalize_website(b['website'])
        a = re.sub(r'[^a-z0-9]', '', b['address'].lower())
        key = f'{w}|{a}' if w else f'no_website|{b["name"].lower()}|{a}'
        if key not in final_dedup:
            final_dedup[key] = b
        else:
            if len(b['name']) < len(final_dedup[key]['name']):
                final_dedup[key] = b
    london = list(final_dedup.values())
    print(f'After final dedup: {len(london)}')

    clinics = [b for b in london if is_specialty_clinic(b)]
    print(f'Specialty clinics: {len(clinics)}')

    has_website = sum(1 for b in clinics if b['website'])
    has_phone = sum(1 for b in clinics if b['phone'])
    print(f'\nWith website: {has_website}/{len(clinics)}')
    print(f'With phone: {has_phone}/{len(clinics)}')

    write_csv(clinics, FINAL_CSV, 'Specialty clinics (filtered)')
    write_csv(london, ALL_CSV, 'All London businesses (unfiltered)')

    # Category breakdown
    cats = {}
    for b in clinics:
        c = b['category'].strip()
        if c:
            cats[c] = cats.get(c, 0) + 1
    print(f'\nCategory breakdown (top 20):')
    for cat, count in sorted(cats.items(), key=lambda x: -x[1])[:20]:
        print(f'  {count:4d} {cat}')


if __name__ == '__main__':
    main()

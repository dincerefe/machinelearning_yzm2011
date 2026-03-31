import undetected_chromedriver as uc
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import re
import sys
import logging
import random
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout.reconfigure(encoding='utf-8')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
    encoding='utf-8'
)
log = logging.getLogger(__name__)
logging.getLogger('undetected_chromedriver').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('selenium').setLevel(logging.WARNING)

# Settings
CSV_DOSYASI = "turkiye_ucuslari.csv"
AIRCRAFT_TYPES_FILE = "aircraft_types.txt"
FLIGHTAWARE_BASE = "https://www.flightaware.com/live/flight"
NUM_WORKERS = 10 

file_lock = threading.Lock()
stats_lock = threading.Lock()

AIRLINE_IATA_TO_ICAO = {
    "TK": "THY", "PC": "PGT", "XQ": "SXS", "XC": "CAI", "VF": "TKJ",
    "AJ": "TKJ", "SK": "SAS", "AY": "FIN", "LH": "DLH", "BA": "BAW",
    "AF": "AFR", "KL": "KLM", "OS": "AUA", "SU": "AFL", "EK": "UAE",
    "QR": "QTR", "MS": "MSR", "RJ": "RJA", "PS": "AUI", "W6": "WZZ",
    "FR": "RYR", "6H": "ISR", "J2": "AHY", "BJ": "LBT", "FZ": "FDB",
    "GF": "GFA", "SV": "SVA", "UX": "AEA", "IB": "IBE", "VY": "VLG",
    "U2": "EZY", "KK": "KKK", "8Q": "OHY", "HY": "UZB", "KC": "KZR",
    "OZ": "AAR", "KE": "KAL", "CZ": "CSN", "CA": "CCA", "ET": "ETH",
    "TG": "THA", "MH": "MAS", "SQ": "SIA", "NH": "ANA", "JL": "JAL",
    "UA": "UAL", "AA": "AAL", "DL": "DAL", "LX": "SWR", "AZ": "ITY",
    "TP": "TAP", "RO": "ROT", "BT": "BTI", "A3": "AEE", "OA": "OAL",
    "ME": "MEA", "WY": "OMA", "G9": "ABY", "5F": "FIA", "LS": "EXS",
    "BY": "TOM", "DE": "CFG", "X3": "TUI", "EW": "EWG", "LO": "LOT",
    "OK": "CSA", "FB": "LZB", "JU": "ASL", "OU": "CTN", "B2": "BRU",
    "S7": "SBI", "DP": "FPO", "U6": "SVR", "UT": "UTA", "N4": "NWS",
    "WZ": "RWZ", "FV": "SDM", "ZF": "AZS", "HJ": "TMC", "T5": "TUA",
    "R3": "SYL", "GH": "GHA", "QS": "TVS", "TO": "TVF", "OR": "TRA",
    "MB": "MNB", "FH": "FHY", "XY": "KNE", "IA": "IAW", "3Z": "TVP",
    "DK": "VBA", "R5": "JAV", "5Q": "MKQ", "6D": "IOS", "2S": "SDY",
    "I8": "IZG", "NN": "MOV", "D2": "SSF", "GP": "GPA", "5N": "AUL",
    "7R": "BRB", "YK": "AVJ", "QN": "QNA",
}

PRIORITY_PREFIXES = ["TK", "PC", "VF", "XQ", "XC"]

KNOWN_AIRCRAFT = {
    'A318', 'A319', 'A320', 'A321', 'A332', 'A333', 'A338', 'A339',
    'A342', 'A343', 'A345', 'A346', 'A359', 'A35K', 'A380',
    'A20N', 'A21N',
    'B712', 'B732', 'B733', 'B734', 'B735', 'B736', 'B737', 'B738',
    'B739', 'B38M', 'B39M', 'B741', 'B742', 'B743', 'B744', 'B748',
    'B752', 'B753', 'B762', 'B763', 'B764', 'B772', 'B773',
    'B77L', 'B77W', 'B778', 'B779', 'B788', 'B789', 'B78X',
}

ICAO_TO_READABLE = {
    'A318': 'A318', 'A319': 'A319', 'A320': 'A320', 'A321': 'A321',
    'A20N': 'A320neo', 'A21N': 'A321neo',
    'A332': 'A330-200', 'A333': 'A330-300', 'A339': 'A330-900neo',
    'A359': 'A350-900', 'A35K': 'A350-1000', 'A380': 'A380',
    'B737': 'B737-700', 'B738': 'B737-800', 'B739': 'B737-900',
    'B38M': 'B737 MAX 8', 'B39M': 'B737 MAX 9',
    'B744': 'B747-400', 'B748': 'B747-8',
    'B752': 'B757-200', 'B763': 'B767-300',
    'B772': 'B777-200', 'B773': 'B777-300',
    'B77L': 'B777-200LR', 'B77W': 'B777-300ER',
    'B788': 'B787-8', 'B789': 'B787-9', 'B78X': 'B787-10',
}


def extract_iata_prefix(flight_code):
    m = re.match(r'^([A-Z]{2})\d', str(flight_code))
    if m:
        return m.group(1)
    m = re.match(r'^(\d[A-Z]|[A-Z]\d)\d', str(flight_code))
    if m:
        return m.group(1)
    return None


def flight_code_to_fa_id(flight_code):
    prefix = extract_iata_prefix(flight_code)
    if not prefix:
        return flight_code
    icao = AIRLINE_IATA_TO_ICAO.get(prefix)
    if icao:
        return f"{icao}{flight_code[len(prefix):]}"
    return flight_code


def load_existing_types():
    types = {}
    if os.path.exists(AIRCRAFT_TYPES_FILE):
        with open(AIRCRAFT_TYPES_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if '=' in line:
                    code, atype = line.split('=', 1)
                    types[code.strip()] = atype.strip()
    return types


def save_type(flight_code, aircraft_type):
    with file_lock:
        with open(AIRCRAFT_TYPES_FILE, 'a', encoding='utf-8') as f:
            f.write(f"{flight_code}={aircraft_type}\n")


def parse_aircraft_from_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    aircraft_types = []

    
    for link in soup.find_all('a', href=True):
        href = link.get('href', '')
        if '/live/aircrafttype/' in href:
            type_code = href.split('/live/aircrafttype/')[-1].strip('/')
            if type_code and len(type_code) >= 3:
                aircraft_types.append(type_code.upper())

    
    for td in soup.find_all('td'):
        text = td.get_text(strip=True).upper()
        if re.match(r'^[AB]\d{2}[0-9A-Z]$', text):
            aircraft_types.append(text)

   
    page_text = soup.get_text()
    all_matches = re.findall(r'\b([AB]\d{2}[0-9A-Z])\b', page_text)
    aircraft_types.extend([m.upper() for m in all_matches])

   
    valid = [t for t in aircraft_types if t in KNOWN_AIRCRAFT]
    if valid:
        return Counter(valid).most_common(1)[0][0]

    if aircraft_types:
        return Counter(aircraft_types).most_common(1)[0][0]

    return None


def create_driver(worker_id):
    options = uc.ChromeOptions()
    options.page_load_strategy = 'eager'
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_argument(f"--remote-debugging-port={9322 + worker_id}")

    for attempt in range(3):
        try:
            driver = uc.Chrome(options=options, version_main=146)
            driver.set_page_load_timeout(15)
            return driver
        except Exception as e:
            if attempt < 2:
                time.sleep(5 + worker_id * 2)
            else:
                raise


def worker_fn(worker_id, tasks, stats):
    log.info(f"[W{worker_id}] Başladı, {len(tasks)} görev")
    driver = None

    try:
        driver = create_driver(worker_id)
    except Exception as e:
        log.error(f"[W{worker_id}] Chrome başlatılamadı: {e}")
        return

    for i, code in enumerate(tasks):
        fa_id = flight_code_to_fa_id(code)
        url = f"{FLIGHTAWARE_BASE}/{fa_id}"

        try:
            driver.get(url)
            time.sleep(random.uniform(2.0, 3.0))

            title = (driver.title or "").lower()
            if "just a moment" in title or "security" in title:
                time.sleep(8)
                title = (driver.title or "").lower()
                if "just a moment" in title:
                    log.warning(f"[W{worker_id}] CloudFlare engeli, tarayıcı yeniden başlatılıyor...")
                    try: driver.quit()
                    except: pass
                    time.sleep(5)
                    driver = create_driver(worker_id)
                    driver.get(url)
                    time.sleep(4)

            html = driver.page_source
            aircraft = parse_aircraft_from_html(html)

            if aircraft:
                readable = ICAO_TO_READABLE.get(aircraft, aircraft)
                save_type(code, readable)
                with stats_lock:
                    stats['found'] += 1
                    total_done = stats['found'] + stats['not_found']
                if total_done % 25 == 0 or i < 3:
                    log.info(f"[W{worker_id}] ✓ {code} -> {readable}  | Toplam: {stats['found']} bulundu, {stats['not_found']} yok [{total_done}/{stats['total']}]")
            else:
                save_type(code, "UNKNOWN")
                with stats_lock:
                    stats['not_found'] += 1

            time.sleep(random.uniform(0.8, 1.5))

        except Exception as e:
            err = str(e)[:80]
            log.error(f"[W{worker_id}] Hata {code}: {err}")
            with stats_lock:
                stats['errors'] += 1

            if any(k in err.lower() for k in ["no such", "session", "disconnected", "crashed"]):
                try: driver.quit()
                except: pass
                time.sleep(3)
                try:
                    driver = create_driver(worker_id)
                except:
                    log.error(f"[W{worker_id}] Chrome yeniden başlatılamadı, çıkılıyor")
                    break

    if driver:
        try: driver.quit()
        except: pass
    log.info(f"[W{worker_id}] Bitti.")


def apply_aircraft_types():
    log.info(f"\n{'=' * 60}")
    log.info("UCAK TİPLERİNİ CSV'YE UYGULAMA")
    log.info(f"{'=' * 60}")

    types = load_existing_types()
    valid_types = {k: v for k, v in types.items() if v != 'UNKNOWN'}
    log.info(f"  Geçerli uçak tipi sayısı: {len(valid_types)}")

    if not valid_types:
        log.warning("Hiç geçerli uçak tipi bulunamadı.")
        return

    df = pd.read_csv(CSV_DOSYASI, low_memory=False)
    mask = df['ucak_tipi'].isna() | (df['ucak_tipi'].astype(str).str.strip() == '')
    before_empty = mask.sum()

    filled = 0
    for idx in df[mask].index:
        code = df.at[idx, 'ucus_kodu']
        if code in valid_types:
            df.at[idx, 'ucak_tipi'] = valid_types[code]
            filled += 1

    after_empty = (df['ucak_tipi'].isna() | (df['ucak_tipi'].astype(str).str.strip() == '')).sum()
    log.info(f"  Önceki eksik: {before_empty}")
    log.info(f"  Doldurulan: {filled}")
    log.info(f"  Sonraki eksik: {after_empty}")

    output = "turkiye_ucuslari_filled.csv"
    df.to_csv(output, index=False, encoding='utf-8-sig')
    log.info(f"  Kaydedildi: {output}")


def main():
    log.info("=" * 60)
    log.info(f"UCAK TİPLERİ SCRAPER (PARALEL - {NUM_WORKERS} WORKER)")
    log.info("=" * 60)

    df = pd.read_csv(CSV_DOSYASI, low_memory=False)
    mask = df['ucak_tipi'].isna() | (df['ucak_tipi'].astype(str).str.strip() == '')
    missing_codes = sorted(df[mask]['ucus_kodu'].unique())
    log.info(f"  Eksik ucak_tipi olan eşsiz uçuş kodu: {len(missing_codes)}")

    existing = load_existing_types()
    log.info(f"  Daha önce bulunan: {len(existing)}")

    priority, other = [], []
    for c in missing_codes:
        if c in existing:
            continue
        prefix = extract_iata_prefix(c)
        if prefix in PRIORITY_PREFIXES:
            priority.append(c)
        elif prefix in AIRLINE_IATA_TO_ICAO:
            other.append(c)

    remaining = priority + other
    log.info(f"  Aranacak: {len(remaining)} (öncelikli: {len(priority)}, diğer: {len(other)})")

    if not remaining:
        log.info("Aranacak kod kalmadı!")
        apply_aircraft_types()
        return

    stats = {
        'found': 0, 'not_found': 0, 'errors': 0,
        'total': len(remaining)
    }

    worker_tasks = [[] for _ in range(NUM_WORKERS)]
    for i, code in enumerate(remaining):
        worker_tasks[i % NUM_WORKERS].append(code)

    for i, wt in enumerate(worker_tasks):
        log.info(f"  Worker {i}: {len(wt)} görev")

    log.info(f"\n{NUM_WORKERS} Chrome başlatılıyor...\n")
    threads = []

    try:
        for wid in range(NUM_WORKERS):
            t = threading.Thread(target=worker_fn, args=(wid, worker_tasks[wid], stats), daemon=True)
            t.start()
            threads.append(t)
            time.sleep(3) 

        for t in threads:
            t.join()

    except KeyboardInterrupt:
        log.info("\n\nDurduruldu! Bulunan tipler kaydedildi.")

    log.info(f"\n{'=' * 60}")
    log.info(f"SONUÇ: {stats['found']} bulundu, {stats['not_found']} bulunamadı, {stats['errors']} hata")
    log.info(f"{'=' * 60}")

    apply_aircraft_types()


if __name__ == "__main__":
    main()

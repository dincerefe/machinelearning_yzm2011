import undetected_chromedriver as uc
from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
import os
import re
import json
import random
from datetime import datetime, timedelta
import logging
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

# UTF-8 encoding
sys.stdout.reconfigure(encoding='utf-8')

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
    encoding='utf-8'
)
log = logging.getLogger(__name__)

# Gereksiz loglari kapat
logging.getLogger('undetected_chromedriver').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('selenium').setLevel(logging.WARNING)

# --- AYARLAR ---
CSV_DOSYASI = "turkiye_ucuslari.csv"
WEATHER_CACHE_FILE = "weather_cache.json"
BASE_URL = "https://www.flightera.net/en"
PAGE_WAIT = (1.5, 2.5)
BETWEEN_PAGES = (0.5, 1.5)
BETWEEN_AIRPORTS = (0.5, 1.0)

# ★★★ PARALEL TARAYICI SAYISI ★★★
# RAM durumuna göre ayarla: her Chrome ~300-500MB RAM yer
# 8GB RAM -> 4-5 tarayıcı, 16GB RAM -> 8-10 tarayıcı
NUM_WORKERS = 8

# --- TÜRKİYE HAVALİMANLARI ---
AIRPORTS = [
    ("ESB", "LTAC", "Ankara", 40.1281, 32.9951, "Ankara"),
    ("AYT", "LTAI", "Antalya", 36.8987, 30.8005, "Antalya"),
    ("GZT", "LTAJ", "Gaziantep", 36.9472, 37.4787, "Gaziantep"),
    ("KFS", "LTAL", "Kastamonu", 41.3142, 33.7958, "Kastamonu"),
    ("KYA", "LTAN", "Konya", 37.9790, 32.5619, "Konya"),
    ("MZH", "LTAP", "Amasya", 40.8294, 35.5220, "Amasya"),
    ("VAS", "LTAR", "Sivas", 39.8138, 36.9035, "Sivas"),
    ("ONQ", "LTAS", "Zonguldak", 41.5064, 32.0886, "Zonguldak"),
    ("MLX", "LTAT", "Malatya", 38.4353, 38.0910, "Malatya"),
    ("ASR", "LTAU", "Kayseri", 38.7704, 35.4954, "Kayseri"),
    ("TJK", "LTAW", "Tokat", 40.3247, 36.3906, "Tokat"),
    ("DNZ", "LTAY", "Denizli", 37.7856, 29.7013, "Denizli"),
    ("NAV", "LTAZ", "Nevşehir", 38.7719, 34.5345, "Nevşehir"),
    ("BZI", "LTBF", "Balıkesir", 39.6193, 27.9260, "Balıkesir"),
    ("CKZ", "LTBH", "Çanakkale", 40.1377, 26.4268, "Çanakkale"),
    ("ADB", "LTBJ", "Gaziemir", 38.2924, 27.1570, "Gaziemir"),
    ("KCO", "LTBQ", "Kartepe", 40.7350, 30.0833, "Kartepe"),
    ("YEI", "LTBR", "Yenişehir", 40.2552, 29.5626, "Yenişehir"),
    ("DLM", "LTBS", "Dalaman", 36.7131, 28.7925, "Dalaman"),
    ("TEQ", "LTBU", "Çorlu", 41.1382, 27.9191, "Çorlu"),
    ("AOE", "LTBY", "Eskişehir", 39.8116, 30.5193, "Eskişehir"),
    ("KZR", "LTBZ", "Altıntaş", 39.1111, 30.1304, "Altıntaş"),
    ("EZS", "LTCA", "Elazığ", 38.5980, 39.2835, "Elazığ"),
    ("OGU", "LTCB", "Ordu", 40.9669, 38.0860, "Ordu"),
    ("DIY", "LTCC", "Diyarbakır", 37.8939, 40.2010, "Diyarbakır"),
    ("ERC", "LTCD", "Erzincan", 39.7102, 39.5270, "Erzincan"),
    ("ERZ", "LTCE", "Erzurum", 39.9565, 41.1702, "Erzurum"),
    ("KSY", "LTCF", "Kars", 40.5622, 43.1150, "Kars"),
    ("TZX", "LTCG", "Trabzon", 40.9951, 39.7897, "Trabzon"),
    ("VAN", "LTCI", "Van", 38.4682, 43.3323, "Van"),
    ("BAL", "LTCJ", "Batman", 37.9290, 41.1166, "Batman"),
    ("MSR", "LTCK", "Muş", 38.7478, 41.6612, "Muş"),
    ("SXZ", "LTCL", "Siirt", 37.9789, 41.8404, "Siirt"),
    ("NOP", "LTCM", "Sinop", 42.0183, 35.0718, "Sinop"),
    ("KCM", "LTCN", "Kahramanmaraş", 37.5388, 36.9535, "Kahramanmaraş"),
    ("AJI", "LTCO", "Ağrı", 39.6556, 43.0257, "Ağrı"),
    ("ADF", "LTCP", "Adıyaman", 37.7314, 38.4689, "Adıyaman"),
    ("MQM", "LTCR", "Mardin", 37.2233, 40.6317, "Mardin"),
    ("GNY", "LTCS", "Şanlıurfa", 37.4457, 38.8956, "Şanlıurfa"),
    ("IGD", "LTCT", "Iğdır", 39.9766, 43.8766, "Iğdır"),
    ("BGG", "LTCU", "Bingöl", 38.8601, 40.5945, "Bingöl"),
    ("NKT", "LTCV", "Şırnak", 37.3647, 42.0582, "Şırnak"),
    ("YKO", "LTCW", "Hakkari", 37.5497, 44.2381, "Hakkari"),
    ("HTY", "LTDA", "Antakya", 36.3608, 36.2856, "Antakya"),
    ("ISE", "LTFC", "Isparta", 37.8554, 30.3684, "Isparta"),
    ("EDO", "LTFD", "Edremit", 39.5525, 27.0102, "Edremit"),
    ("BJV", "LTFE", "Bodrum", 37.2493, 27.6640, "Bodrum"),
    ("GZP", "LTFG", "Gazipaşa", 36.2988, 32.2970, "Gazipaşa"),
    ("SZF", "LTFH", "Samsun", 41.2540, 36.5675, "Samsun"),
    ("SAW", "LTFJ", "Pendik, Istanbul", 40.8986, 29.3092, "Istanbul"),
    ("IST", "LTFM", "Istanbul", 41.2749, 28.7321, "Istanbul"),
]

AIRPORT_MAP = {a[0]: a for a in AIRPORTS}

# Thread-safe CSV yazma lock'u
csv_lock = threading.Lock()
weather_lock = threading.Lock()
done_tasks_lock = threading.Lock()
def mark_task_as_done(iata, date_str):
    with done_tasks_lock:
        with open("done_tasks_log.txt", "a", encoding="utf-8") as f:
            f.write(f"{date_str},{iata}\n")


# ==================== HAVA DURUMU (OPEN-METEO) ====================

weather_cache = {}

def load_weather_cache():
    global weather_cache
    if os.path.exists(WEATHER_CACHE_FILE):
        with open(WEATHER_CACHE_FILE, 'r', encoding='utf-8') as f:
            weather_cache = json.load(f)
        log.info(f"Weather cache: {len(weather_cache)} kayit")

def save_weather_cache():
    with weather_lock:
        try:
            with open(WEATHER_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(weather_cache, f)
        except Exception as e:
            log.error(f"Weather cache kaydedilemedi: {e}")

GLOBAL_AIRPORTS = {}
def load_global_airports():
    global GLOBAL_AIRPORTS
    if os.path.exists("global_airports.json"):
        try:
            with open("global_airports.json", "r", encoding="utf-8") as f:
                GLOBAL_AIRPORTS = json.load(f)
            log.info(f"Global havalimanlari yuklendi: {len(GLOBAL_AIRPORTS)} kayit")
        except: pass

# --- PROXY ---
USE_PROXIES = True
PROXY_LIST = []
proxy_lock = threading.Lock()

def fetch_free_proxies():
    global PROXY_LIST
    with proxy_lock:
        if len(PROXY_LIST) > 100: 
            return
            
        log.info("Genis ucretsiz proxy havuzu cekiliyor (Coklu Kaynak)...")
        combined_proxies = set(PROXY_LIST)
        
        urls = [
            "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=all&ssl=all&anonymity=elite",
            "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
            "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
            "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/proxy.txt"
        ]
        
        for url in urls:
            try:
                resp = requests.get(url, timeout=10)
                if resp.status_code == 200:
                    lines = resp.text.split('\n')
                    for p in lines:
                        p = p.strip()
                        if p and ":" in p:
                            combined_proxies.add(p)
            except Exception:
                pass
                
        # Internetten cekilenleri karistir
        fetched_list = list(combined_proxies)
        random.shuffle(fetched_list)
        
        PROXY_LIST = fetched_list
        
        if PROXY_LIST:
            log.info(f"Farkli kaynaklardan toplam {len(PROXY_LIST)} adet benzersiz proxy birestirildi!")
        else:
            log.warning("Hicbir kaynaktan proxy cekilemedi!")


def get_weather(lat, lon, date_str):
    """Open-Meteo ile gunluk hava durumu (tamamen ucretsiz, API key yok)"""
    return None  # KULLANICI ISTEGI UZERINE HAVA DURUMU SORGULARI GECICI OLARAK KAPATILDI
    
    key = f"{lat:.2f}_{lon:.2f}_{date_str}"
    with weather_lock:
        if key in weather_cache:
            return weather_cache[key]

    params = {
        "latitude": lat, "longitude": lon,
        "start_date": date_str, "end_date": date_str,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_gusts_10m,visibility,cloud_cover,weather_code",
        "timezone": "Europe/Istanbul"
    }

    # API cagrisini proxy ile veya proxy'siz dene
    for attempt in range(4):
        try:
            proxies = None
            if attempt > 0 and PROXY_LIST:
                p = random.choice(PROXY_LIST)
                proxies = {"http": f"http://{p}", "https": f"http://{p}"}

            resp = requests.get("https://archive-api.open-meteo.com/v1/archive", 
                                params=params, timeout=5, proxies=proxies)
            
            if resp.status_code == 200:
                data = resp.json()
                if "hourly" in data:
                    h = data["hourly"]
                    with weather_lock:
                        weather_cache[key] = h
                        if len(weather_cache) % 250 == 0:
                            save_weather_cache() # Sifirdan basladigimiz icin cache erimesin. 250 islemde bir dosyaya kaydet.
                    return h
            elif resp.status_code == 429:
                time.sleep(1)
        except Exception:
            pass

    # Hicbir proxy veya lokal baglanti API'dan cevap alamadiysa, NONE olarak cachele.
    # Boylece ayni IST veya SAW gunesinde yuzlerce ayni sehre giden ucus ayni hatati yiyerek 5 saat askida tutmaz.
    with weather_lock:
        weather_cache[key] = None
    return None


# ==================== FLIGHTERA SCRAPER ====================

def check_proxy_fast(proxy):
    try:
        px = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
        r = requests.get("https://www.flightera.net/robots.txt", proxies=px, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.5)
        if r.status_code in [200, 403, 404]:
            return proxy
    except: pass
    return None

def create_driver(worker_id=0, use_proxy=False):
    """CloudFlare bypass icin undetected-chromedriver (headed mode)"""
    options = uc.ChromeOptions()
    options.page_load_strategy = 'eager'  # Sayfanin tamamen yuklenmesini (reklamlar vs) bekleme, DOM ulasinca dur
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    # Hizlandirma icin resimleri kapatiyoruz
    options.add_argument("--blink-settings=imagesEnabled=false")
    # Her worker icin farkli port
    options.add_argument(f"--remote-debugging-port={9222 + worker_id}")

    proxy = None
    if use_proxy:
        proxy_found = False
        
        # Devasa gucu ac: 40 Proxy'i AYNI ANDA test et (Multithreading)
        for attempt_batch in range(3): # Maksimum 3 defa 40'lik paket dene
            batch = []
            with proxy_lock:
                take_count = min(40, len(PROXY_LIST))
                if take_count > 0:
                    batch = PROXY_LIST[:take_count]
                    del PROXY_LIST[:take_count]
            
            if not batch:
                break
                
            log.info(f"[W{worker_id}] Kalan havuzdan alinmis {len(batch)} adet proxy ASIRI HIZLI (Paralel) saldiriyla taranıyor...")
            
            with ThreadPoolExecutor(max_workers=40) as executor:
                futures = {executor.submit(check_proxy_fast, p): p for p in batch}
                for future in as_completed(futures):
                    p = futures[future]
                    res = future.result()
                    if res:
                        proxy = res
                        proxy_found = True
                        break
                        
            if proxy_found:
                break
            
        if proxy_found and proxy:
            options.add_argument(f'--proxy-server=http://{proxy}')
        else:
            raise Exception("Paralel dev hizli tarama basarisiz! Yeni IP taranacak...")

    for attempt in range(3):
        try:
            driver = uc.Chrome(options=options, version_main=146)
            driver.set_page_load_timeout(10)  #  saniyede acilmazsa TimeOutException atar
            return driver, proxy
        except Exception as e:
            if attempt < 2:
                time.sleep(5 + worker_id * 2)  # Stagger retries
            else:
                raise


def parse_time_from_text(text):
    if not text:
        return None
    m = re.search(r'(\d{1,2}:\d{2})', text)
    return m.group(1) if m else None


def parse_iata_from_dest(text):
    if not text:
        return None
    m = re.search(r'\((\w{3})\s*/', text)
    return m.group(1) if m else None


def parse_flights_from_html(html, target_date_str):
    """HTML'den ucus verilerini parse et"""
    soup = BeautifulSoup(html, 'html.parser')
    flights = []

    tbody = soup.find('tbody')
    if not tbody:
        return flights, None

    rows = tbody.find_all('tr')

    for row in rows:
        tds = row.find_all('td')
        if len(tds) < 7:
            continue

        flight = {}
        try:
            td0 = tds[0]
            date_link = td0.find('a', href=lambda h: h and '/flight_details/' in h)
            if date_link:
                href_detail = date_link.get('href', '')
                # Eger ucus, taradigimiz gune ait degilse atla (sayfa sonlarinda sonraki gune tasan ucuslari filtrele)
                if target_date_str not in href_detail:
                    continue
                    
                flight['detay_url'] = href_detail
                flight['tarih_text'] = date_link.get_text(strip=True)

            status_span = td0.find('span', class_=re.compile(r'bg-(green|yellow|red)'))
            if status_span:
                flight['durum'] = status_span.get_text(strip=True)
                cls = ' '.join(status_span.get('class', []))
                if 'green' in cls: flight['durum_kat'] = 'On_Time'
                elif 'yellow' in cls: flight['durum_kat'] = 'Delayed'
                elif 'red' in cls: flight['durum_kat'] = 'Cancelled'
                else: flight['durum_kat'] = 'Other'

            mobile_time = td0.find('span', class_=lambda c: c and 'inline' in c and 'lg:hidden' in c if c else False)
            if mobile_time:
                time_span = mobile_time.find('span', class_='whitespace-nowrap')
                if time_span:
                    flight['plan_kalkis'] = parse_time_from_text(time_span.get_text(strip=True))

            td1 = tds[1]
            fl_link = td1.find('a', href=lambda h: h and '/flight/' in h)
            if fl_link:
                flight['ucus_kodu'] = fl_link.get_text(strip=True)
            al_link = td1.find('a', href=lambda h: h and '/airline/' in h)
            if al_link:
                flight['havayolu'] = al_link.get_text(strip=True)

            td2 = tds[2]
            dest_link = td2.find('a', href=lambda h: h and '/airport/' in h)
            if dest_link:
                dest_full = dest_link.get_text(strip=True)
                flight['varis_bilgi'] = dest_full
                flight['varis_iata'] = parse_iata_from_dest(dest_full)

            td3 = tds[3]
            sched_span = td3.find('span', class_='whitespace-nowrap')
            if sched_span:
                sched_time = parse_time_from_text(sched_span.get_text(strip=True))
                if sched_time:
                    flight['plan_kalkis'] = sched_time

            td4 = tds[4]
            dep_span = td4.find('span', class_='whitespace-nowrap')
            if dep_span:
                flight['gercek_kalkis'] = parse_time_from_text(dep_span.get_text(strip=True))

            delay_badge = td4.find('span', class_=re.compile(r'rounded-full|bg-(red|yellow)-100'))
            if delay_badge:
                m = re.search(r'(\d+)\s*min', delay_badge.get_text(strip=True))
                if m: flight['delay_dk'] = int(m.group(1))

            td5 = tds[5]
            arr_span = td5.find('span', class_='whitespace-nowrap')
            if arr_span:
                flight['varis_zamani'] = parse_time_from_text(arr_span.get_text(strip=True))

            td6 = tds[6]
            dur_span = td6.find('span', class_='whitespace-nowrap')
            if dur_span:
                flight['sure'] = dur_span.get_text(strip=True)

            if 'ucus_kodu' in flight:
                flights.append(flight)
        except Exception:
            continue

    next_url = None
    later_link = soup.find('a', string=re.compile(r'Later Flights'))
    if later_link:
        href = later_link.get('href', '')
        date_compact = target_date_str.replace('-', '')
        if date_compact in href or target_date_str in href:
            next_url = href if href.startswith('http') else f"https://www.flightera.net{href}"

    return flights, next_url


def scrape_airport_day(driver, iata, date_str, worker_id=0, start_url_override=None):
    """Bir havalimani icin bir gunluk tum ucuslari topla"""
    ap = AIRPORT_MAP[iata]
    icao, url_city = ap[1], ap[5]
    date_formatted = f"{date_str}%2000_00"

    start_url = start_url_override if start_url_override else f"{BASE_URL}/airport/{url_city}/{icao}/departure/{date_formatted}"
    all_flights = []
    current_url = start_url
    page = 0
    max_pages = 50
    blocked = False

    while page < max_pages:
        try:
            driver.get(current_url)
            time.sleep(random.uniform(*PAGE_WAIT))

            title = driver.title.lower() if driver.title else ""
            source = driver.page_source.lower() if driver.page_source else ""
            
            cf_keywords = [
                "just a moment", "too many requests", 
                "performing security verification", "security service to protect", 
                "verify you are human", "this site can't be reached",
                "this site can’t be reached", "err_timed_out", "err_tunnel", 
                "err_proxy", "err_connection"
            ]
            
            is_blocked = ("error" in title) or any(k in title for k in cf_keywords) or any(k in source for k in cf_keywords)

            if is_blocked:
                time.sleep(10)
                title = driver.title.lower() if driver.title else ""
                source = driver.page_source.lower() if driver.page_source else ""
                is_blocked = ("error" in title) or any(k in title for k in cf_keywords) or any(k in source for k in cf_keywords)
                
                if is_blocked:
                    log.warning(f"  [W{worker_id}] CloudFlare/RateLimit block: {iata}")
                    blocked = True
                    break

            flights, next_url = parse_flights_from_html(driver.page_source, date_str)
            all_flights.extend(flights)
            page += 1

            if not flights or not next_url or next_url == current_url:
                current_url = None # Tamamen bitti anlaminda
                break

            current_url = next_url
            time.sleep(random.uniform(*BETWEEN_PAGES))

        except Exception as e:
            err_str = str(e)
            first_line = err_str.splitlines()[0][:150] if err_str else "Unknown Error"
            log.error(f"  [W{worker_id}] Hata ({iata}): {first_line}")
            
            err_lower = err_str.lower()
            if any(k in err_lower for k in ["err_tunnel", "err_proxy", "err_timed", "timeout", "timed out", "err_connection"]):
                blocked = True
            break

    return all_flights, blocked, current_url


def calc_delay(sched, actual):
    if not sched or not actual:
        return None
    try:
        sh, sm = map(int, sched.split(':'))
        ah, am = map(int, actual.split(':'))
        diff = (ah * 60 + am) - (sh * 60 + sm)
        if diff < -720: diff += 1440
        return diff
    except:
        return None


def process_flights(raw, iata, date_str):
    ap = AIRPORT_MAP[iata]
    lat, lon, city = ap[3], ap[4], ap[2]
    wx = get_weather(lat, lon, date_str)

    rows = []
    for f in raw:
        row = {
            'tarih': date_str, 'kalkis_havalimani': iata, 'kalkis_sehir': city,
            'varis_havalimani': f.get('varis_iata', ''), 'varis_bilgi': f.get('varis_bilgi', ''),
            'havayolu': f.get('havayolu', ''), 'ucus_kodu': f.get('ucus_kodu', ''),
            'plan_kalkis': f.get('plan_kalkis', ''), 'gercek_kalkis': f.get('gercek_kalkis', ''),
            'varis_zamani': f.get('varis_zamani', ''), 'ucus_suresi': f.get('sure', ''),
            'durum': f.get('durum', ''), 'durum_kategori': f.get('durum_kat', ''),
        }
        
        if 'delay_dk' in f:
            row['delay_dk'] = f['delay_dk']
        else:
            row['delay_dk'] = calc_delay(f.get('plan_kalkis'), f.get('gercek_kalkis'))
            
        row['ic_hat'] = 1 if f.get('varis_iata', '') in AIRPORT_MAP else 0

        # Ucak tipi bilgisi sayfada olmadigi icin bos birakiliyor
        row['ucak_tipi'] = ''

        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            row['gun_adi'] = dt.strftime('%A')
            row['ay'] = dt.month
            row['haftanin_gunu'] = dt.weekday()
        except: pass

        if f.get('plan_kalkis'):
            try:
                hour = int(f['plan_kalkis'].split(':')[0])
                row['kalkis_saati'] = hour
                if 6 <= hour < 12: row['saat_dilimi'] = 'Sabah'
                elif 12 <= hour < 17: row['saat_dilimi'] = 'Ogle'
                elif 17 <= hour < 22: row['saat_dilimi'] = 'Aksam'
                else: row['saat_dilimi'] = 'Gece'
            except: pass

        if wx:
            hr = int(row.get('kalkis_saati', 12))  # Default öğle vakti
            if 0 <= hr < 24:
                try:
                    row['hava_temp'] = wx['hourly']['temperature_2m'][hr]
                    row['hava_humidity'] = wx['hourly']['relative_humidity_2m'][hr]
                    row['hava_precip_mm'] = wx['hourly']['precipitation'][hr]
                    row['hava_wind'] = wx['hourly']['wind_speed_10m'][hr]
                    row['hava_wind_gust'] = wx['hourly']['wind_gusts_10m'][hr]
                    row['hava_vis_min_m'] = wx['hourly'].get('visibility', [None]*24)[hr]
                    row['hava_cloud'] = wx['hourly']['cloud_cover'][hr]
                except (KeyError, IndexError, TypeError):
                    row['hava_temp'] = row['hava_humidity'] = row['hava_precip_mm'] = None
                    row['hava_wind'] = row['hava_wind_gust'] = row['hava_vis_min_m'] = row['hava_cloud'] = None
        else:
            row['hava_temp'] = row['hava_humidity'] = row['hava_precip_mm'] = None
            row['hava_wind'] = row['hava_wind_gust'] = row['hava_vis_min_m'] = row['hava_cloud'] = None

        # Varis Havalimani Hava Durumu
        varis_iata = f.get('varis_iata', '')
        v_lat, v_lon = None, None
        
        if varis_iata in AIRPORT_MAP:
            v_ap = AIRPORT_MAP[varis_iata]
            v_lat, v_lon = v_ap[3], v_ap[4]
        elif varis_iata in GLOBAL_AIRPORTS:
            v_lat = GLOBAL_AIRPORTS[varis_iata].get('lat')
            v_lon = GLOBAL_AIRPORTS[varis_iata].get('lon')

        if v_lat is not None and v_lon is not None:
            wx_v = get_weather(v_lat, v_lon, date_str)
            if wx_v:
                # Varis saatini tahmin et veya bul
                hr_v = int(row.get('kalkis_saati', 12)) + 1 
                if hr_v >= 24: hr_v = 23
                if f.get('varis_zamani'):
                    try: hr_v = int(f['varis_zamani'].split(':')[0])
                    except: pass
                
                try:
                    row['varis_temp'] = wx_v['hourly']['temperature_2m'][hr_v]
                    row['varis_humidity'] = wx_v['hourly']['relative_humidity_2m'][hr_v]
                    row['varis_precip_mm'] = wx_v['hourly']['precipitation'][hr_v]
                    row['varis_wind'] = wx_v['hourly']['wind_speed_10m'][hr_v]
                    row['varis_wind_gust'] = wx_v['hourly']['wind_gusts_10m'][hr_v]
                    row['varis_vis_min_m'] = wx_v['hourly'].get('visibility', [None]*24)[hr_v]
                    row['varis_cloud'] = wx_v['hourly']['cloud_cover'][hr_v]
                except (KeyError, IndexError, TypeError):
                    row['varis_temp'] = row['varis_humidity'] = row['varis_precip_mm'] = None
                    row['varis_wind'] = row['varis_wind_gust'] = row['varis_vis_min_m'] = row['varis_cloud'] = None
            else:
                row['varis_temp'] = row['varis_humidity'] = row['varis_precip_mm'] = None
                row['varis_wind'] = row['varis_wind_gust'] = row['varis_vis_min_m'] = row['varis_cloud'] = None
        else:
            row['varis_temp'] = row['varis_humidity'] = row['varis_precip_mm'] = None
            row['varis_wind'] = row['varis_wind_gust'] = row['varis_vis_min_m'] = row['varis_cloud'] = None

        rows.append(row)
    return rows


def save_rows_to_csv(rows):
    """Thread-safe CSV kaydetme"""
    if not rows:
        return
    with csv_lock:
        file_exists = os.path.exists(CSV_DOSYASI) and os.path.getsize(CSV_DOSYASI) > 0
        df = pd.DataFrame(rows)
        df.to_csv(CSV_DOSYASI, mode='a', header=not file_exists, index=False, encoding='utf-8-sig')


# ==================== WORKER FONKSIYONU ====================

def worker_fn(worker_id, tasks, stats):
    """Her worker kendi Chrome tarayicisiyla calisir"""
    log.info(f"[W{worker_id}] Gorev sayisi: {len(tasks)}")
    
    driver = None
    current_proxy = None
    use_proxy = False
    
    i = 0
    resume_url = None
    
    while i < len(tasks):
        iata, date_str = tasks[i]
        
        if driver is None:
            try:
                driver, current_proxy = create_driver(worker_id, use_proxy=use_proxy)
            except Exception as e:
                log.warning(f"[W{worker_id}] {e}")
                if use_proxy:
                    if len(PROXY_LIST) < 15:
                        log.info(f"[W{worker_id}] Havuz tukenmek uzere, yeni liste talep ediliyor...")
                        fetch_free_proxies()
                    
                    # 10 saniyede bir %25 ihitmalle "Acaba ban kalkmis midir?" diyerek ev internetimizi geri dener.
                    if random.random() < 0.25:
                        log.info(f"[W{worker_id}] Uzun sure saglam proxy bulunamadi. Ev internetinizin bani (Kalkti mi?) test ediliyor...")
                        use_proxy = False
                continue
                
        try:
            flights, blocked, next_resume_url = scrape_airport_day(driver, iata, date_str, worker_id, start_url_override=resume_url)
            
            if flights:
                log.info(f"[W{worker_id}] Sayfalar kazindi, {len(flights)} ucus tabloya ekleniyor...")
                rows = process_flights(flights, iata, date_str)
                save_rows_to_csv(rows)
                with stats['lock']:
                    stats['total'] += len(rows)
                    total, done, all_tasks = stats['total'], stats['done'], stats['all_tasks']
                
                msg = "ARA KAYIT (Proxy coktu)" if blocked else "BU GUN KISMI BİTTİ"
                log.info(f"[W{worker_id}] {iata} {date_str}: +{len(rows)} {msg} | Toplam: {total}")
            
            if blocked:
                log.warning(f"[W{worker_id}] IP engellendi veya Proxy cokuk, yenisi deneniyor...")
                try: driver.quit()
                except: pass
                driver = None
                
                if USE_PROXIES:
                    use_proxy = True
                    if current_proxy in PROXY_LIST:
                        PROXY_LIST.remove(current_proxy)
                        log.warning(f"[W{worker_id}] Bozuk proxy atildi: {current_proxy}. Kalan: {len(PROXY_LIST)}")
                        
                    if not PROXY_LIST:
                        fetch_free_proxies()
                
                # Cokuk olan proxy isine yaradigi kadariyla ucuslari yukarida kaydetti. Kalan sayfalardan devam edecegiz:
                resume_url = next_resume_url
                # i artmaz, ayni (iata, date) tekrar denenir ama kaldigi URL'den baslar
                continue
                
            # BURAYA GELDIYSE BLOCKED DEGILDIR, GOREV TAMAMLANMISTIR
            
            if not flights and not resume_url:
                with stats['lock']: stats['done'] += 1
                log.info(f"[W{worker_id}] {iata} {date_str}: BOS (0 ucus) kaydedildi.")
            else:
                with stats['lock']: stats['done'] += 1
                done_ct, all_tasks_ct = stats['done'], stats['all_tasks']
                log.info(f"[W{worker_id}] {iata} {date_str}: TUMU TAMAMLANDI | {done_ct}/{all_tasks_ct}")
            
            # Ne olursa olsun basariyla taranan gunu logla
            mark_task_as_done(iata, date_str)
            
            time.sleep(random.uniform(*BETWEEN_AIRPORTS))
            i += 1
            resume_url = None
            
        except Exception as e:
            log.error(f"[W{worker_id}] Hata {iata} {date_str}: {e}")
            with stats['lock']: stats['done'] += 1
            i += 1
            resume_url = None

    if driver:
        try: driver.quit()
        except: pass
    log.info(f"[W{worker_id}] Tamamlandi.")


# ==================== ANA FONKSIYON ====================

def main():
    # ========== KONFIGURASYON ==========
    TARGET_AIRPORTS = [a[0] for a in AIRPORTS]

    START_DATE = datetime(2024, 1, 1)    
    END_DATE = datetime(2025, 12, 31)
    # ====================================

    total_days = (END_DATE - START_DATE).days + 1
    log.info("=" * 60)
    log.info("TURKIYE UCUS VERISI TOPLAMA (PARALEL)")
    log.info(f"Kaynak: flightera.net + Open-Meteo")
    log.info(f"Havalimanlari: {len(TARGET_AIRPORTS)}")
    log.info(f"Tarih: {START_DATE.date()} -> {END_DATE.date()} ({total_days} gun)")
    log.info(f"Paralel tarayici: {NUM_WORKERS}")
    log.info("=" * 60)

    load_weather_cache()
    load_global_airports()

    # Mevcut veriyi kontrol et
    existing = set()
    total_existing = 0

    if os.path.exists("done_tasks_log.txt"):
        try:
            with open("done_tasks_log.txt", "r", encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) == 2:
                        existing.add((parts[0], parts[1]))
        except: pass

    if os.path.exists(CSV_DOSYASI) and os.path.getsize(CSV_DOSYASI) > 0:
        try:
            df = pd.read_csv(CSV_DOSYASI, usecols=['tarih', 'kalkis_havalimani'])
            total_existing = len(df)
            for _, r in df.iterrows():
                existing.add((str(r['tarih']), str(r['kalkis_havalimani'])))
        except:
            pass

    log.info(f"Hafizada tamamlanmis sayilan gorev (gun+havalimani): {len(existing)}")

    # Gorev listesi olustur: (iata, tarih) ciftleri
    all_tasks = []
    current = START_DATE
    while current <= END_DATE:
        ds = current.strftime('%Y-%m-%d')
        for iata in TARGET_AIRPORTS:
            if (ds, iata) not in existing:
                all_tasks.append((iata, ds))
        current += timedelta(days=1)

    log.info(f"Toplam gorev: {len(all_tasks)} (havalimani x gun)")
    
    if not all_tasks:
        log.info("Tum veriler zaten toplanmis!")
        return

    # Tahmin
    est_per_task = 3.5  # Hizlandirilmis script icin ortalama saniye/gorev
    est_total_min = (len(all_tasks) * est_per_task) / NUM_WORKERS / 60
    log.info(f"Tahmini sure: ~{est_total_min:.0f} dakika ({est_total_min/60:.1f} saat) "
            f"[{NUM_WORKERS} paralel tarayici ile]")

    # Gorevleri worker'lara dagit (round-robin)
    worker_tasks = [[] for _ in range(NUM_WORKERS)]
    for i, task in enumerate(all_tasks):
        worker_tasks[i % NUM_WORKERS].append(task)

    for i, wt in enumerate(worker_tasks):
        log.info(f"  Worker {i}: {len(wt)} gorev")

    # Paylasilan istatistikler
    stats = {
        'total': total_existing,
        'done': 0,
        'all_tasks': len(all_tasks),
        'lock': threading.Lock()
    }

    # Chrome tarayicilari kademeli baslat (hepsini ayni anda baslatmamak icin)
    log.info(f"\n{NUM_WORKERS} Chrome tarayicisi baslatiliyor (kademeli)...")

    threads = []
    try:
        for wid in range(NUM_WORKERS):
            t = threading.Thread(target=worker_fn, args=(wid, worker_tasks[wid], stats), daemon=True)
            t.start()
            threads.append(t)
            time.sleep(3)  # Her Chrome arasinda 3sn bekle (stagger)

        # Tum thread'lerin bitmesini bekle
        for t in threads:
            t.join()

    except KeyboardInterrupt:
        log.info("\n\nDurduruldu! Veriler kaydedildi.")
    finally:
        save_weather_cache()

    log.info(f"\nTOPLAM: {stats['total']} ucus -> {CSV_DOSYASI}")


if __name__ == "__main__":
    main()

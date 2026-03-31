# -*- coding: utf-8 -*-
"""
Eksik Uçuş Verilerini Doldurma Scripti (ULTRA HIZLI & ATOMİK CACHE)
========================================================================
1) Eksik/Hatalı uçak tiplerini -> aircraft_types.txt ile EZİP DÜZELTİR.
2) Kalan boşlukları uçuş kodu bazında mode ile doldurur.
3) Eksik hava durumu verilerini Connection Pooling ile hızlıca çeker.
4) Pandas Vectorization (Merge/Update) ile 500k satırı milisaniyede doldurur.
"""

import pandas as pd
import numpy as np
import requests
import json
import os
import time
import sys
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

sys.stdout.reconfigure(encoding='utf-8')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
    encoding='utf-8'
)
log = logging.getLogger(__name__)

# --- AYARLAR ---
CSV_DOSYASI = "turkiye_ucuslari.csv"
OUTPUT_CSV = "turkiye_ucuslari_filled.csv"
GLOBAL_AIRPORTS_FILE = "global_airports.json"
AIRCRAFT_TYPES_FILE = "aircraft_types.txt"
WEATHER_CACHE_FILE = "weather_offline_cache.json"

MAX_WORKERS = 15  # Session pooling ile 8 stabil ve çok hızlı çalışır.

cache_lock = threading.Lock()

# Thread'ler arası bağlantıları tekrar kullanmak için Session
thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

# Türkiye havalimanları koordinatları
AIRPORTS = {
    "ESB": (40.1281, 32.9951), "AYT": (36.8987, 30.8005), "GZT": (36.9472, 37.4787),
    "KFS": (41.3142, 33.7958), "KYA": (37.9790, 32.5619), "MZH": (40.8294, 35.5220),
    "VAS": (39.8138, 36.9035), "ONQ": (41.5064, 32.0886), "MLX": (38.4353, 38.0910),
    "ASR": (38.7704, 35.4954), "TJK": (40.3247, 36.3906), "DNZ": (37.7856, 29.7013),
    "NAV": (38.7719, 34.5345), "BZI": (39.6193, 27.9260), "CKZ": (40.1377, 26.4268),
    "ADB": (38.2924, 27.1570), "KCO": (40.7350, 30.0833), "YEI": (40.2552, 29.5626),
    "DLM": (36.7131, 28.7925), "TEQ": (41.1382, 27.9191), "AOE": (39.8116, 30.5193),
    "KZR": (39.1111, 30.1304), "EZS": (38.5980, 39.2835), "OGU": (40.9669, 38.0860),
    "DIY": (37.8939, 40.2010), "ERC": (39.7102, 39.5270), "ERZ": (39.9565, 41.1702),
    "KSY": (40.5622, 43.1150), "TZX": (40.9951, 39.7897), "VAN": (38.4682, 43.3323),
    "BAL": (37.9290, 41.1166), "MSR": (38.7478, 41.6612), "SXZ": (37.9789, 41.8404),
    "NOP": (42.0183, 35.0718), "KCM": (37.5388, 36.9535), "AJI": (39.6556, 43.0257),
    "ADF": (37.7314, 38.4689), "MQM": (37.2233, 40.6317), "GNY": (37.4457, 38.8956),
    "IGD": (39.9766, 43.8766), "BGG": (38.8601, 40.5945), "NKT": (37.3647, 42.0582),
    "YKO": (37.5497, 44.2381), "HTY": (36.3608, 36.2856), "ISE": (37.8554, 30.3684),
    "EDO": (39.5525, 27.0102), "BJV": (37.2493, 27.6640), "GZP": (36.2988, 32.2970),
    "SZF": (41.2540, 36.5675), "SAW": (40.8986, 29.3092), "IST": (41.2749, 28.7321),
}

WEATHER_FIELDS_API = "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_gusts_10m,visibility,cloud_cover"

KALKIS_WEATHER_MAP = {
    "temperature_2m": "hava_temp", "relative_humidity_2m": "hava_humidity", "precipitation": "hava_precip_mm",
    "wind_speed_10m": "hava_wind", "wind_gusts_10m": "hava_wind_gust", "visibility": "hava_vis_min_m", "cloud_cover": "hava_cloud",
}

VARIS_WEATHER_MAP = {
    "temperature_2m": "varis_temp", "relative_humidity_2m": "varis_humidity", "precipitation": "varis_precip_mm",
    "wind_speed_10m": "varis_wind", "wind_gusts_10m": "varis_wind_gust", "visibility": "varis_vis_min_m", "cloud_cover": "varis_cloud",
}

# ==================== ATOMİK CACHE SİSTEMİ ====================
def load_weather_cache():
    if os.path.exists(WEATHER_CACHE_FILE):
        try:
            with open(WEATHER_CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            log.warning(f"Cache dosyası okunurken hata: {e}")
            return {}
    return {}

def save_weather_cache(cache):
    with cache_lock:
        temp_file = WEATHER_CACHE_FILE + ".tmp"
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(cache, f)
            os.replace(temp_file, WEATHER_CACHE_FILE)
        except Exception as e:
            log.error(f"Cache kaydedilirken hata oluştu: {e}")

# ==================== UÇAK TİPLERİNİ DÜZELTME ====================
def fill_aircraft_types(df):
    log.info("=" * 60)
    log.info("ADIM 1: Uçak tiplerini düzeltme")
    log.info("=" * 60)

    filled_df = df.copy()

    if os.path.exists(AIRCRAFT_TYPES_FILE):
        extra_types = {}
        try:
            with open(AIRCRAFT_TYPES_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    if '=' in line:
                        code, atype = line.strip().split('=', 1)
                        if atype and atype != 'UNKNOWN':
                            extra_types[code.replace(" ", "").strip()] = atype.strip()
            
            # Pandas map ile anında eziyoruz (Milisaniyelik işlem)
            filled_df['ucus_kodu_clean'] = filled_df['ucus_kodu'].astype(str).str.replace(" ", "").str.strip()
            mask = filled_df['ucus_kodu_clean'].isin(extra_types.keys())
            filled_df.loc[mask, 'ucak_tipi'] = filled_df.loc[mask, 'ucus_kodu_clean'].map(extra_types)
            log.info(f"  TXT tabanlı zorunlu güncellemeyle {mask.sum()} uçuşun tipi DÜZELTİLDİ.")
            filled_df.drop(columns=['ucus_kodu_clean'], inplace=True)

        except Exception as e:
            log.error(f"  TXT okunurken hata: {e}")

    # Mode ile kalanları doldur
    filled_df['ucak_tipi'] = filled_df['ucak_tipi'].replace(r'^\s*$', np.nan, regex=True)
    
    if filled_df['ucak_tipi'].isna().sum() == 0:
        return filled_df

    # Saniyelik mode doldurma
    mode_map = filled_df.dropna(subset=['ucak_tipi']).groupby('ucus_kodu')['ucak_tipi'].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan)
    filled_df['ucak_tipi'] = filled_df['ucak_tipi'].fillna(filled_df['ucus_kodu'].map(mode_map))

    filled_df['_airline_prefix'] = filled_df['ucus_kodu'].astype(str).str.extract(r'^([A-Z]{2})', expand=False)
    airline_mode = filled_df.dropna(subset=['ucak_tipi']).groupby('_airline_prefix')['ucak_tipi'].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan)
    
    filled_df['ucak_tipi'] = filled_df['ucak_tipi'].fillna(filled_df['_airline_prefix'].map(airline_mode))
    filled_df.drop(columns=['_airline_prefix'], inplace=True)
    
    return filled_df

# ==================== HAVA DURUMU HIZLI ÇEKİM ====================
def get_airport_coords(iata, global_airports):
    if iata in AIRPORTS: return AIRPORTS[iata]
    if iata in global_airports: return (global_airports[iata]['lat'], global_airports[iata]['lon'])
    return None

def fetch_weather_bulk(lat, lon, start_date, end_date):
    session = get_session() # TCP bağlantısını yeniden kullan (Connection Pooling)
    params = {
        "latitude": round(lat, 4), "longitude": round(lon, 4),
        "start_date": start_date, "end_date": end_date,
        "hourly": WEATHER_FIELDS_API, "timezone": "Europe/Istanbul"
    }

    for attempt in range(4):
        try:
            resp = session.get("https://archive-api.open-meteo.com/v1/archive", params=params, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if "hourly" not in data: return None
                
                hourly = data["hourly"]
                times = hourly["time"]
                result = {}
                
                for i, t in enumerate(times):
                    date_str = t[:10]
                    hour = int(t[11:13])
                    if date_str not in result: result[date_str] = {}

                    hour_data = {}
                    for field in KALKIS_WEATHER_MAP.keys():
                        val = hourly.get(field, [None] * len(times))
                        hour_data[field] = val[i] if i < len(val) else None
                    result[date_str][hour] = hour_data
                return result

            elif resp.status_code == 429:
                wait_time = 5 * (attempt + 1)
                time.sleep(wait_time)
            else:
                time.sleep(1)
        except Exception:
            time.sleep(2)
            
    return None

def process_airport_weather(iata, coords, ap_min, ap_max, weather_cache):
    current_start = datetime.strptime(ap_min, '%Y-%m-%d')
    final_end = datetime.strptime(ap_max, '%Y-%m-%d')

    airport_weather = {}
    success = True
    
    while current_start <= final_end:
        chunk_end = min(current_start + timedelta(days=364), final_end)
        weather_data = fetch_weather_bulk(
            coords[0], coords[1],
            current_start.strftime('%Y-%m-%d'),
            chunk_end.strftime('%Y-%m-%d')
        )
        
        if weather_data:
            airport_weather.update(weather_data)
        else:
            success = False
            break 
            
        current_start = chunk_end + timedelta(days=1)
        time.sleep(0.2) # API nefes alsın

    if success and airport_weather:
        with cache_lock:
            weather_cache[iata] = airport_weather
        save_weather_cache(weather_cache) 
        log.info(f"    -> {iata} başarıyla çekildi ve KAYDEDİLDİ.")
    else:
        log.error(f"    -> {iata} çekilemedi! (Limit veya bağlantı sorunu)")


def fill_weather_data(df, global_airports):
    log.info("=" * 60)
    log.info(f"ADIM 2: Hava durumu verilerini doldurma (Kontrollü Paralel & Pandas Vektörizasyon)")
    log.info("=" * 60)

    weather_cache = load_weather_cache()
    log.info(f"  Lokal cache dosyasında {len(weather_cache)} havalimanı verisi BAŞARIYLA OKUNDU.")

    kalkis_cols = list(KALKIS_WEATHER_MAP.values())
    varis_cols = list(VARIS_WEATHER_MAP.values())
    
    filled_df = df.copy()

    all_needed_airports = set()
    if filled_df[kalkis_cols[0]].isna().sum() > 0:
        all_needed_airports.update(filled_df.loc[filled_df[kalkis_cols[0]].isna(), 'kalkis_havalimani'].dropna().unique())
    if filled_df[varis_cols[0]].isna().sum() > 0:
        varis_ap = filled_df.loc[filled_df[varis_cols[0]].isna(), 'varis_havalimani'].dropna().unique()
        all_needed_airports.update([a for a in varis_ap if isinstance(a, str) and len(a) == 3])

    airports_to_fetch = [iata for iata in all_needed_airports if iata not in weather_cache]
    log.info(f"  Toplam benzersiz havalimanı: {len(all_needed_airports)} | Çekilecek kalan havalimanı: {len(airports_to_fetch)}")

    # API'DEN EKSİKLERİ ÇEK (Eğer varsa)
    if airports_to_fetch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for idx, iata in enumerate(airports_to_fetch):
                coords = get_airport_coords(iata, global_airports)
                if not coords: continue

                dates = filled_df[(filled_df['kalkis_havalimani'] == iata) | (filled_df['varis_havalimani'] == iata)]['tarih']
                if dates.empty: continue
                
                ap_min, ap_max = dates.min(), dates.max()
                futures.append(executor.submit(
                    process_airport_weather, iata, coords, ap_min, ap_max, weather_cache
                ))
            for future in as_completed(futures):
                pass 

    log.info("\n  Tüm veriler hazır, tablo PANDAS ile ŞİMŞEK HIZIYLA dolduruluyor...")

    # =========================================================================
    # PANDAS VECTORIZATION İLE TABLOYU MERGE ETME (For döngüsü yok, anında biter)
    # =========================================================================
    
    # 1. Cache sözlüğünü Pandas Tablosuna Çeviriyoruz
    weather_records = []
    for iata, date_dict in weather_cache.items():
        for date_str, hour_dict in date_dict.items():
            for hr_str, w_data in hour_dict.items():
                row = w_data.copy()
                row['havalimani'] = iata
                row['tarih'] = date_str
                row['saat'] = int(hr_str)
                weather_records.append(row)

    if not weather_records:
        return filled_df

    wdf = pd.DataFrame(weather_records)
    
    # 2. Kalkış Hava Durumunu Doldur
    # Indexleri ayarlayarak saniyesinde dolduruyoruz
    wdf_kalkis = wdf.rename(columns=KALKIS_WEATHER_MAP)
    wdf_kalkis.set_index(['havalimani', 'tarih', 'saat'], inplace=True)
    
    filled_df['kalkis_saat_temp'] = pd.to_numeric(filled_df['kalkis_saati'], errors='coerce').fillna(-1).astype(int)
    filled_df.set_index(['kalkis_havalimani', 'tarih', 'kalkis_saat_temp'], drop=False, inplace=True)
    
    filled_df.update(wdf_kalkis)
    filled_df.reset_index(drop=True, inplace=True)

    # 3. Varış Hava Durumunu Doldur
    # Varış saatini hesapla
    filled_df['varis_saat_temp'] = pd.to_numeric(filled_df['varis_zamani'].str.split(':').str[0], errors='coerce')
    
    # Varış saati boşsa kalkış + 1 saat yap
    mask_calc = filled_df['varis_saat_temp'].isna() & filled_df['kalkis_saati'].notna()
    filled_df.loc[mask_calc, 'varis_saat_temp'] = pd.to_numeric(filled_df.loc[mask_calc, 'kalkis_saati']) + 1
    filled_df['varis_saat_temp'] = filled_df['varis_saat_temp'].fillna(-1).astype(int)
    filled_df['varis_saat_temp'] = filled_df['varis_saat_temp'].clip(0, 23)

    # Tarih atlaması olan gece uçuşlarını düzelt (örneğin 23'te kalkıp 01'de varanlar)
    filled_df['kalkis_saat_temp'] = pd.to_numeric(filled_df['kalkis_saati'], errors='coerce').fillna(-1).astype(int)
    mask_next_day = (filled_df['kalkis_saat_temp'] > -1) & (filled_df['varis_saat_temp'] < filled_df['kalkis_saat_temp'])
    
    filled_df['varis_tarih_temp'] = filled_df['tarih']
    filled_df.loc[mask_next_day, 'varis_tarih_temp'] = pd.to_datetime(filled_df.loc[mask_next_day, 'tarih']) + pd.Timedelta(days=1)
    filled_df['varis_tarih_temp'] = filled_df['varis_tarih_temp'].astype(str).str[:10]

    # Varış için merge/update
    wdf_varis = wdf.rename(columns=VARIS_WEATHER_MAP)
    wdf_varis.set_index(['havalimani', 'tarih', 'saat'], inplace=True)

    filled_df.set_index(['varis_havalimani', 'varis_tarih_temp', 'varis_saat_temp'], drop=False, inplace=True)
    filled_df.update(wdf_varis)
    filled_df.reset_index(drop=True, inplace=True)

    # Geçici sütunları temizle
    filled_df.drop(columns=['kalkis_saat_temp', 'varis_saat_temp', 'varis_tarih_temp'], inplace=True, errors='ignore')

    log.info(f"  Kalan Boş Kalkış H. Durumu: {filled_df[kalkis_cols[0]].isna().sum()}")
    log.info(f"  Kalan Boş Varış H. Durumu: {filled_df[varis_cols[0]].isna().sum()}")

    return filled_df


# ==================== ANA FONKSİYON ====================
def main():
    log.info("=" * 60)
    log.info("EKSİK UÇUŞ VERİLERİNİ DOLDURMA (Vektör Modu)")
    log.info("=" * 60)

    df = pd.read_csv(CSV_DOSYASI, dtype={'kalkis_saati': 'float64'})
    df['tarih'] = df['tarih'].astype(str)

    global_airports = {}
    if os.path.exists(GLOBAL_AIRPORTS_FILE):
        with open(GLOBAL_AIRPORTS_FILE, 'r', encoding='utf-8') as f:
            global_airports = json.load(f)

    start_time = time.time()
    
    df = fill_aircraft_types(df)
    df = fill_weather_data(df, global_airports)

    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    
    elapsed = time.time() - start_time
    log.info(f"Sonuç kaydedildi: {OUTPUT_CSV}")
    log.info(f"BİTTİ! Toplam geçen süre: {elapsed:.2f} saniye.")

if __name__ == "__main__":
    main()
# -*- coding: utf-8 -*-
"""
Eksik Uçuş Verilerini Doldurma Scripti — ULTRA HIZLI v2
=========================================================
Eski sürümden farklar:
  - Polars      : Pandas yerine, 10-100x hızlı veri işleme
  - Meteostat   : Open-Meteo yerine; CDN'den direkt CSV çeker, rate-limit yok
  - asyncio     : Meteostat zaten hızlı olduğundan ThreadPool yeterli; ama
                  fallback olarak aiohttp ile Open-Meteo async da destekleniyor
  - orjson      : stdlib json yerine 10x hızlı cache okuma/yazma
  - Parquet     : JSON cache yerine; büyük veri setlerinde çok daha hızlı
  - Akıllı önbellekleme: her havalimanı ayrı parquet dosyası → kısmi güncellemeler ucuz

Gerekli kütüphaneler:
  pip install polars meteostat orjson pyarrow
  (isteğe bağlı fallback: pip install aiohttp)
"""

import sys
import os
import time
import logging
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import polars as pl

# Hız için orjson; yoksa stdlib json
try:
    import orjson as json_lib
    _ORJSON = True
except ImportError:
    import json as json_lib
    _ORJSON = False

# Meteostat: ana hava durumu kaynağı
try:
    from meteostat import Hourly, Stations
    _METEOSTAT = True
except ImportError:
    _METEOSTAT = False
    print("[UYARI] meteostat kurulu değil. 'pip install meteostat' çalıştırın.")
    print("         Fallback olarak Open-Meteo (aiohttp) kullanılacak.")

sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    encoding="utf-8",
)
log = logging.getLogger(__name__)

# ─── AYARLAR ──────────────────────────────────────────────────────────────────
CSV_DOSYASI        = "turkiye_ucuslari.csv"
OUTPUT_CSV         = "turkiye_ucuslari_filled.csv"
GLOBAL_AIRPORTS    = "global_airports.json"
AIRCRAFT_TXT       = "aircraft_types.txt"
CACHE_DIR          = "weather_cache_parquet"   # her AP için ayrı .parquet
MAX_WORKERS        = 8                          # Meteostat thread sayısı

os.makedirs(CACHE_DIR, exist_ok=True)

# ─── TÜRKİYE HAVALİMANI KOORDİNATLARI ───────────────────────────────────────
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

# Meteostat sütun adı → CSV sütun adı eşlemesi
METEOSTAT_KALKIS = {
    "temp":  "hava_temp",
    "rhum":  "hava_humidity",
    "prcp":  "hava_precip_mm",
    "wspd":  "hava_wind",
    "wpgt":  "hava_wind_gust",
    "tsun":  None,                # kullanılmıyor
    "pres":  None,
    "dwpt":  None,
    "wdir":  None,
    # Meteostat'ta visibility yoktur; sonradan 0 ile doldurulacak
}
METEOSTAT_VARIS = {
    "temp":  "varis_temp",
    "rhum":  "varis_humidity",
    "prcp":  "varis_precip_mm",
    "wspd":  "varis_wind",
    "wpgt":  "varis_wind_gust",
}

# Kullanılacak sütunlar
KALKIS_COLS = ["hava_temp","hava_humidity","hava_precip_mm","hava_wind","hava_wind_gust"]
VARIS_COLS  = ["varis_temp","varis_humidity","varis_precip_mm","varis_wind","varis_wind_gust"]

# ─── CACHE YARDIMCILARı ──────────────────────────────────────────────────────
def _cache_path(iata: str) -> str:
    return os.path.join(CACHE_DIR, f"{iata}.parquet")

def cache_load(iata: str) -> pl.DataFrame | None:
    p = _cache_path(iata)
    if os.path.exists(p):
        try:
            return pl.read_parquet(p)
        except Exception:
            return None
    return None

def cache_save(iata: str, df: pl.DataFrame):
    try:
        df.write_parquet(_cache_path(iata))
    except Exception as e:
        log.warning(f"Cache kaydedilemedi ({iata}): {e}")


# ─── UÇAK TİPLERİNİ DÜZELTME (Polars) ───────────────────────────────────────
def fill_aircraft_types(df: pl.DataFrame) -> pl.DataFrame:
    log.info("=" * 60)
    log.info("ADIM 1: Uçak tiplerini düzeltme")
    log.info("=" * 60)

    # 1. aircraft_types.txt'den zorunlu güncelleme
    if os.path.exists(AIRCRAFT_TXT):
        extra: dict[str, str] = {}
        with open(AIRCRAFT_TXT, "r", encoding="utf-8") as f:
            for line in f:
                if "=" in line:
                    code, atype = line.strip().split("=", 1)
                    code = code.replace(" ", "").strip()
                    atype = atype.strip()
                    if atype and atype != "UNKNOWN":
                        extra[code] = atype

        if extra:
            mapping_df = pl.DataFrame(
                {"ucus_kodu_clean": list(extra.keys()), "_forced_type": list(extra.values())}
            )
            df = (
                df
                .with_columns(
                    pl.col("ucus_kodu").str.replace_all(" ", "").alias("ucus_kodu_clean")
                )
                .join(mapping_df, on="ucus_kodu_clean", how="left")
                .with_columns(
                    pl.when(pl.col("_forced_type").is_not_null())
                    .then(pl.col("_forced_type"))
                    .otherwise(pl.col("ucak_tipi"))
                    .alias("ucak_tipi")
                )
                .drop(["ucus_kodu_clean", "_forced_type"])
            )
            log.info(f"  TXT ile zorunlu güncelleme tamamlandı.")

    # 2. Boş stringleri null'a çevir
    df = df.with_columns(
        pl.when(pl.col("ucak_tipi").str.strip_chars() == "")
        .then(None)
        .otherwise(pl.col("ucak_tipi"))
        .alias("ucak_tipi")
    )

    null_cnt = df["ucak_tipi"].null_count()
    if null_cnt == 0:
        log.info("  Tüm uçak tipleri dolu, atlıyoruz.")
        return df

    log.info(f"  Mode doldurması için {null_cnt} boş kayıt mevcut.")

    # 3. ucus_kodu bazında mode doldurma
    mode_by_flight = (
        df.filter(pl.col("ucak_tipi").is_not_null())
        .group_by("ucus_kodu")
        .agg(pl.col("ucak_tipi").mode().first().alias("_mode_type"))
    )
    df = (
        df.join(mode_by_flight, on="ucus_kodu", how="left")
        .with_columns(
            pl.when(pl.col("ucak_tipi").is_null())
            .then(pl.col("_mode_type"))
            .otherwise(pl.col("ucak_tipi"))
            .alias("ucak_tipi")
        )
        .drop("_mode_type")
    )

    # 4. Havayolu prefix bazında mode doldurma
    df = df.with_columns(
        pl.col("ucus_kodu").str.extract(r"^([A-Z]{2})", 0).alias("_airline_prefix")
    )
    mode_by_airline = (
        df.filter(pl.col("ucak_tipi").is_not_null())
        .group_by("_airline_prefix")
        .agg(pl.col("ucak_tipi").mode().first().alias("_mode_airline"))
    )
    df = (
        df.join(mode_by_airline, on="_airline_prefix", how="left")
        .with_columns(
            pl.when(pl.col("ucak_tipi").is_null())
            .then(pl.col("_mode_airline"))
            .otherwise(pl.col("ucak_tipi"))
            .alias("ucak_tipi")
        )
        .drop(["_airline_prefix", "_mode_airline"])
    )

    log.info(f"  Doldurma sonrası boş kalan: {df['ucak_tipi'].null_count()}")
    return df


# ─── METEOSTATı İLE HAVA DURUMU ÇEKİMİ ──────────────────────────────────────
def fetch_meteostat(iata: str, lat: float, lon: float,
                    start: datetime, end: datetime) -> pl.DataFrame | None:
    """
    Meteostat kütüphanesi ile saatlik veri çeker.
    Dahili olarak CDN'den CSV indirir — rate-limit yok, çok hızlı.
    Dönüş: columns = [tarih, saat, temp, rhum, prcp, wspd, wpgt]
    """
    try:
        # Meteostat'ın yerleşik istasyon arama motoru
        stations = Stations()
        stations = stations.nearby(lat, lon)
        station = stations.fetch(1)
        if station.empty:
            log.warning(f"  {iata}: Yakın istasyon bulunamadı.")
            return None

        data = Hourly(station, start, end)
        data = data.fetch()
        if data is None or data.empty:
            return None

        # pandas → polars (hızlı yol)
        pf = pl.from_pandas(data.reset_index())
        # 'time' sütunundan tarih ve saat ayır
        pf = pf.with_columns([
            pl.col("time").dt.strftime("%Y-%m-%d").alias("tarih"),
            pl.col("time").dt.hour().alias("saat"),
        ]).drop("time")

        # Sadece ihtiyaç duyulan sütunları seç
        keep = ["tarih", "saat"] + [c for c in ["temp","rhum","prcp","wspd","wpgt"] if c in pf.columns]
        pf = pf.select(keep)
        pf = pf.with_columns(pl.lit(iata).alias("havalimani"))
        return pf

    except Exception as e:
        log.error(f"  {iata} Meteostat hatası: {e}")
        return None


# ─── OPEN-METEO ASYNC FALLBACK ────────────────────────────────────────────────
def fetch_openmeteo_sync(iata: str, lat: float, lon: float,
                         start: str, end: str) -> pl.DataFrame | None:
    """
    Meteostat yoksa Open-Meteo'dan senkron (requests) çeker.
    İstersen aiohttp ile async versiyona geçebilirsin.
    """
    import requests

    FIELDS = "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_gusts_10m"
    params = {
        "latitude": round(lat, 4), "longitude": round(lon, 4),
        "start_date": start, "end_date": end,
        "hourly": FIELDS, "timezone": "Europe/Istanbul",
    }
    for attempt in range(4):
        try:
            r = requests.get(
                "https://archive-api.open-meteo.com/v1/archive",
                params=params, timeout=20
            )
            if r.status_code == 200:
                d = r.json()
                if "hourly" not in d:
                    return None
                h = d["hourly"]
                times = h["time"]
                pf = pl.DataFrame({
                    "tarih": [t[:10] for t in times],
                    "saat":  [int(t[11:13]) for t in times],
                    "temp":  h.get("temperature_2m",   [None]*len(times)),
                    "rhum":  h.get("relative_humidity_2m", [None]*len(times)),
                    "prcp":  h.get("precipitation",    [None]*len(times)),
                    "wspd":  h.get("wind_speed_10m",   [None]*len(times)),
                    "wpgt":  h.get("wind_gusts_10m",   [None]*len(times)),
                })
                pf = pf.with_columns(pl.lit(iata).alias("havalimani"))
                return pf
            elif r.status_code == 429:
                time.sleep(5 * (attempt + 1))
            else:
                time.sleep(1)
        except Exception:
            time.sleep(2)
    return None


def fetch_airport_weather(iata: str, lat: float, lon: float,
                          date_min: str, date_max: str) -> pl.DataFrame | None:
    """
    Önce cache'e bak. Yoksa Meteostat (veya fallback Open-Meteo) ile çek.
    iata her zaman büyük harfle saklanır.
    """
    iata = iata.upper()
    cached = cache_load(iata)
    if cached is not None:
        return cached

    start = datetime.strptime(date_min, "%Y-%m-%d")
    end   = datetime.strptime(date_max, "%Y-%m-%d")

    if _METEOSTAT:
        df = fetch_meteostat(iata, lat, lon, start, end)
    else:
        df = fetch_openmeteo_sync(iata, lat, lon, date_min, date_max)

    if df is not None:
        cache_save(iata, df)
        log.info(f"    ✓ {iata} çekildi ve cache'e kaydedildi.")
    else:
        log.error(f"    ✗ {iata} çekilemedi.")

    return df


# ─── HAVA DURUMU DOLDURMA ─────────────────────────────────────────────────────
def _build_weather_frame(all_frames: list[pl.DataFrame]) -> pl.DataFrame:
    """Tüm havalimanı frame'lerini birleştir, (havalimani, tarih, saat) indexle."""
    if not all_frames:
        return pl.DataFrame()
    return pl.concat(all_frames, how="diagonal")


def _merge_weather(
    df: pl.DataFrame,
    weather: pl.DataFrame,
    ap_col: str,       # "kalkis_havalimani" veya "varis_havalimani"
    time_col: str,     # "kalkis_saati" veya varis saat sütunu
    col_map: dict,     # meteostat sütun → hedef sütun
    suffix: str = "",
) -> pl.DataFrame:
    """
    df ile weather tablosunu (havalimani, tarih, saat) üzerinden join'le,
    sadece null olan hedef sütunları doldur.
    """
    if weather.is_empty():
        return df

    # Yeniden adlandır
    rename = {"havalimani": "_ap", "tarih": "_tarih", "saat": "_join_saat"}
    for src, dst in col_map.items():
        if src in weather.columns and dst:
            rename[src] = dst + suffix
    wf = weather.rename(rename).select(list(rename.values()))

    # Saat sütununu int yap
    df = df.with_columns(
        pl.col(time_col).cast(pl.Float64, strict=False)
                        .fill_null(-1)
                        .cast(pl.Int32)
                        .alias("_join_saat")
    )

    df = (
        df
        .with_columns([
            pl.col(ap_col).alias("_ap"),
            pl.col("tarih").alias("_tarih"),
        ])
        .join(wf, on=["_ap", "_tarih", "_join_saat"], how="left", suffix="__w")
    )

    # Sadece null olanları doldur (mevcut değerlere dokunma)
    for src, dst in col_map.items():
        if not dst:
            continue
        w_col = dst + suffix + "__w" if (dst + suffix + "__w") in df.columns else dst + suffix
        if w_col not in df.columns:
            continue
        if dst + suffix not in df.columns:
            df = df.rename({w_col: dst + suffix})
        else:
            df = df.with_columns(
                pl.when(pl.col(dst + suffix).is_null())
                .then(pl.col(w_col))
                .otherwise(pl.col(dst + suffix))
                .alias(dst + suffix)
            ).drop(w_col)

    return df.drop(["_ap", "_tarih", "_join_saat"], strict=False)


def fill_weather_data(df: pl.DataFrame, global_ap_coords: dict) -> pl.DataFrame:
    log.info("=" * 60)
    log.info("ADIM 2: Hava durumu doldurma (Meteostat + Polars)")
    log.info("=" * 60)

    # Havalimanı kodlarını büyük harfe normalize et (bjl → BJL gibi)
    for col in ("kalkis_havalimani", "varis_havalimani"):
        if col in df.columns:
            df = df.with_columns(pl.col(col).str.to_uppercase().alias(col))

    # Hangi AP'lara ihtiyaç var?
    needed_kalkis = df.filter(pl.col(KALKIS_COLS[0]).is_null())["kalkis_havalimani"].drop_nulls().unique()
    needed_varis  = df.filter(pl.col(VARIS_COLS[0]).is_null())["varis_havalimani"].drop_nulls().unique()
    all_needed    = set(needed_kalkis.to_list()) | set(needed_varis.to_list())

    log.info(f"  Hava durumu gereken benzersiz havalimanı sayısı: {len(all_needed)}")

    # Tarih aralıklarını hesapla
    date_ranges: dict[str, tuple[str, str]] = {}
    for iata in all_needed:
        dates = df.filter(
            (pl.col("kalkis_havalimani") == iata) | (pl.col("varis_havalimani") == iata)
        )["tarih"]
        if not dates.is_empty():
            date_ranges[iata] = (dates.min(), dates.max())

    # Koordinat yardımcısı — büyük/küçük harf fark etmez
    def get_coords(iata: str):
        iata_up = iata.upper()
        if iata_up in AIRPORTS:
            return AIRPORTS[iata_up]
        # global_airports.json genellikle büyük harfli key içerir ama ikisini de dene
        for key in (iata_up, iata):
            if key in global_ap_coords:
                g = global_ap_coords[key]
                return (g["lat"], g["lon"])
        return None

    # Paralel çekiş (Meteostat CDN'den indirdiği için IO-bound, thread pool ideal)
    all_frames: list[pl.DataFrame] = []

    def worker(iata: str) -> pl.DataFrame | None:
        coords = get_coords(iata)
        if not coords:
            log.warning(f"  {iata}: koordinat bulunamadı, atlanıyor.")
            return None
        dr = date_ranges.get(iata)
        if not dr:
            return None
        return fetch_airport_weather(iata, coords[0], coords[1], dr[0], dr[1])

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(worker, iata): iata for iata in all_needed}
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                all_frames.append(result)

    if not all_frames:
        log.warning("  Hiç hava durumu verisi alınamadı!")
        return df

    weather = _build_weather_frame(all_frames)
    log.info(f"  Toplam hava durumu satırı: {len(weather):,}")

    # Kalkış hava durumunu doldur
    log.info("  Kalkış hava durumu merge ediliyor...")
    df = _merge_weather(
        df, weather,
        ap_col="kalkis_havalimani",
        time_col="kalkis_saati",
        col_map=METEOSTAT_KALKIS,
    )

    # Varış saatini hesapla (geçici sütun)
    df = df.with_columns(
        pl.col("varis_zamani").str.split(":").list.first()
        .cast(pl.Float64, strict=False)
        .alias("_varis_saat_raw")
    )
    # Gece uçuşu tarih düzeltmesi
    df = df.with_columns(
        pl.col("kalkis_saati").cast(pl.Float64, strict=False).alias("_kalkis_saat_f")
    )
    df = df.with_columns(
        pl.when(
            pl.col("_varis_saat_raw").is_not_null() &
            pl.col("_kalkis_saat_f").is_not_null() &
            (pl.col("_varis_saat_raw") < pl.col("_kalkis_saat_f"))
        )
        .then(
            (pl.col("tarih").str.to_datetime("%Y-%m-%d") + pl.duration(days=1))
            .dt.strftime("%Y-%m-%d")
        )
        .otherwise(pl.col("tarih"))
        .alias("_varis_tarih")
    )
    # Varış saatini (saat olarak düzelt, yoksa kalkış+1)
    df = df.with_columns(
        pl.when(pl.col("_varis_saat_raw").is_null())
        .then(
            (pl.col("_kalkis_saat_f").fill_null(0) + 1).clip(0, 23)
        )
        .otherwise(pl.col("_varis_saat_raw").clip(0, 23))
        .cast(pl.Int32)
        .alias("_varis_saat_int")
    )

    # Varış hava durumunu doldur — önce geçici tarih/saat col adlarını ayarla
    log.info("  Varış hava durumu merge ediliyor...")

    wf_varis = weather.rename(
        {c: v for c, v in METEOSTAT_VARIS.items() if c in weather.columns and v}
        | {"havalimani": "_ap", "tarih": "_tarih", "saat": "_join_saat"}
    )

    df = (
        df
        .rename({"_varis_tarih": "_tarih", "_varis_saat_int": "_join_saat",
                 "varis_havalimani": "_ap"})
        .join(
            wf_varis.select(
                ["_ap","_tarih","_join_saat"] +
                [v for v in METEOSTAT_VARIS.values() if v]
            ),
            on=["_ap","_tarih","_join_saat"], how="left", suffix="__w"
        )
    )

    for _, dst in METEOSTAT_VARIS.items():
        if not dst:
            continue
        w_col = dst + "__w"
        if w_col not in df.columns:
            continue
        if dst not in df.columns:
            df = df.rename({w_col: dst})
        else:
            df = df.with_columns(
                pl.when(pl.col(dst).is_null())
                .then(pl.col(w_col))
                .otherwise(pl.col(dst))
                .alias(dst)
            ).drop(w_col)

    df = df.rename({"_ap": "varis_havalimani", "_tarih": "_varis_tarih_drop",
                    "_join_saat": "_varis_saat_drop"})
    df = df.drop(["_varis_tarih_drop", "_varis_saat_drop",
                  "_varis_saat_raw", "_kalkis_saat_f"], strict=False)

    log.info(f"  Kalan boş kalkış hava durumu: {df[KALKIS_COLS[0]].null_count():,}")
    log.info(f"  Kalan boş varış hava durumu : {df[VARIS_COLS[0]].null_count():,}")
    return df


# ─── ANA FONKSİYON ───────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("EKSİK UÇUŞ VERİLERİNİ DOLDURMA — ULTRA HIZLI v2")
    log.info(f"  Polars    : {pl.__version__}")
    log.info(f"  Meteostat : {'AKTIF' if _METEOSTAT else 'PASIF (Open-Meteo fallback)'}")
    log.info(f"  orjson    : {'AKTIF' if _ORJSON else 'PASIF (stdlib json)'}")
    log.info("=" * 60)

    # ── Veri Yükleme (Polars lazy scan → çok hızlı) ──
    log.info(f"CSV okunuyor: {CSV_DOSYASI}")
    df = pl.read_csv(CSV_DOSYASI, infer_schema_length=10_000)

    # tarih sütununu string yap
    df = df.with_columns(pl.col("tarih").cast(pl.Utf8))

    # Eksik hava sütunlarını oluştur (yoksa)
    for col in KALKIS_COLS + VARIS_COLS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))

    # Global havalimanı koordinatları (varsa)
    global_ap = {}
    if os.path.exists(GLOBAL_AIRPORTS):
        with open(GLOBAL_AIRPORTS, "rb" if _ORJSON else "r",
                  **({"encoding": "utf-8"} if not _ORJSON else {})) as f:
            global_ap = json_lib.loads(f.read()) if _ORJSON else json_lib.load(f)

    start = time.perf_counter()

    df = fill_aircraft_types(df)
    df = fill_weather_data(df, global_ap)

    # ── Kaydet ──
    df.write_csv(OUTPUT_CSV)
    elapsed = time.perf_counter() - start

    log.info("=" * 60)
    log.info(f"Sonuç kaydedildi : {OUTPUT_CSV}  ({len(df):,} satır)")
    log.info(f"BİTTİ! Toplam süre: {elapsed:.2f} saniye.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()

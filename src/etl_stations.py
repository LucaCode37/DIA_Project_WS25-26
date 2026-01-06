import json
import argparse
import string
import unicodedata
from typing import Optional, Tuple, Dict

import psycopg2

def normalize_name(s:string) -> str:
    if s is None:
        return ""
    s = s.strip().lower()
    
    if s.startswith("berlin "):
        s = s[len("berlin "):]
        
    s = s.replace("ß", "ss")
    s = s.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
    
    for x in ["-", "_", ".", ",", ";", ":", "'", "\"", "´", "`"]:
        s = s.replace(x, " ")
        
    s = " ".join(s.split())
    
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s

def pick_main_eva_and_coords(station_obj) -> Tuple[Optional[int], Optional[float], Optional[float]]:
    eva = None
    lat = None
    lon = None
    
    eva_numbers = station_obj.get("eva_numbers") or []
    main = None
    for e in eva_numbers:
        if e.get("isMain"):
            main = e
            break
    if main is None and eva_numbers:
        main = eva_numbers[0]
        
    if main:
        eva = main.get("number")
        coords = (main.get("geographicCoordinates") or {}).get("coordinates")
        if isinstance(coords, list) and len(coords) == 2:
            lon, lat = float(coords[0]), float(coords[1])
            
    return eva, lat, lon

def main():
    
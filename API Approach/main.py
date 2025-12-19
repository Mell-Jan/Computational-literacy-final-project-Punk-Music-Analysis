import time
import requests
import csv
import logging
import json
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Don't need information from those libraries just the Errors if it crashes.
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("requests").setLevel(logging.CRITICAL)

#QUERY = 'tag:"punk" AND firstreleasedate:1981 AND recording:*'
LIMIT = 5   # number of recordings to process
# MusicBrainz asks you to leave a user agent with mail
USER_AGENT = "PunkPopularityScript_Digital_Humanities_University_Helsinki ( Mail )"
BASE_SLEEP  = 1.5
WE_HAVE_ARTIST = []
REQUESTED_ARTISTS = []

ARTIST_CACHE_PATH = r""
ARTIST_CACHE = {}

FIELDNAMES = [
    "title",
    "artist",
    "mbid",
    "votes",
    "rating",
    "area",
    "raw_request_record",
    "raw_request_rating",
    "raw_request_artist",
]


session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})

retry = Retry(
    total=6,
    backoff_factor=1.0,
    status_forcelist=[502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=5, pool_maxsize=5)
session.mount("https://", adapter)

_last_call_ts = 0.0

def setup_logging(log_path="logs/debug.log"):
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(path, encoding="utf-8"),
            logging.StreamHandler()  # keep console output
        ]
    )

    logging.info("Logging initialized")

def append_result_csv(path, result):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    file_exists = p.exists()

    row = result_to_csv_row(result)

    with p.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)
        f.flush()

def append_artist_cache_csv(artist_id, area_name, path=ARTIST_CACHE_PATH):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    file_exists = p.exists()

    with p.open("a", newline="", encoding="utf-8") as f:
        fieldnames = ["artist_mbid", "area_name"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            "artist_mbid": artist_id,
            "area_name": area_name if area_name else "none"
        })
        f.flush()

def load_artist_cache_csv(path=ARTIST_CACHE_PATH):
    p = Path(path)
    if not p.exists():
        logging.info(f"No artist cache found at {p} (will create on first write).")
        return

    with p.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            artist_id = (row.get("artist_mbid") or "").strip()
            area_name = (row.get("area_name") or "").strip()
            if artist_id:
                ARTIST_CACHE[artist_id] = area_name if area_name else "none"

    logging.info(f"Loaded {len(ARTIST_CACHE)} artists from cache: {p}")


def result_to_csv_row(result):
    return {
        "title": result.get("title"),
        "artist": result.get("artist"),
        "mbid": result.get("mbid"),
        "votes": result.get("votes"),
        "rating": result.get("rating"),
        "area": (
            result.get("area")
        ),
        "raw_request_record": json.dumps(
            result.get("raw_request_record"),
            ensure_ascii=False
        ),
        "raw_request_rating": json.dumps(
            result.get("raw_request_rating"),
            ensure_ascii=False
        ),
        "raw_request_artist": json.dumps(
            result.get("raw_request_artist"),
            ensure_ascii=False
        ),
    }

#
def load_processed_recording_ids_csv(path):
    p = Path(path)
    if not p.exists():
        return set()

    processed = set()
    with p.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mbid = row.get("recording_mbid")
            if mbid:
                processed.add(mbid)

    return processed

# To ensure that MusicBrainz does not decline my requests I needed to make sure that they are just every second. I got kicked out 2 times so I opted to this approach
def throttled_get(url, params=None):
    global _last_call_ts

    elapsed = time.time() - _last_call_ts
    if elapsed < BASE_SLEEP:
        time.sleep(BASE_SLEEP - elapsed)

    for attempt in range(1, 6):
        try:
            resp = session.get(url, params=params, timeout=20)
            _last_call_ts = time.time()

            if resp.status_code == 503:
                time.sleep(5)

            resp.raise_for_status()
            return resp

        except requests.exceptions.ConnectionError:
            wait = min(2 ** attempt, 30)
            time.sleep(wait)
            if attempt == 5:
                raise


# Search for all recording for one year
def search_recordings(year):
    query = f'tag:"punk" AND firstreleasedate:{year} AND recording:*'
    url = "https://musicbrainz.org/ws/2/recording/"

    limit = 100
    offset = 0
    all_recordings = []
    i = 0
    while True:
    #while i < 3:
        params = {
            "query": query,
            "limit": limit,
            "offset": offset,
            "fmt": "json"
        }

        data = throttled_get(url, params=params).json()
        recordings = data.get("recordings", [])
        count = data.get("count", 0)

        all_recordings.extend(recordings)

        print(f"Fetched {len(all_recordings)} / {count}")

        if len(all_recordings) >= count:
            break

        offset += limit
        #i = i + 1

    return all_recordings


# Get the rating for the record with MusicBrainz ID
def fetch_rating(recording_mbid):
    url = f"https://musicbrainz.org/ws/2/recording/{recording_mbid}"
    params = {"fmt": "json", "inc": "ratings"}

    logging.debug(f"Requesting Rating {recording_mbid}")
    rating = throttled_get(url, params=params).json().get("rating", {})
    return rating


# Go to the artists page and see where the artist is from, this also includes a file that works as a cache in case I already requested that artist, this saves time when doing a lot of requests
def get_country_of_origin(artist_id):

    if artist_id in ARTIST_CACHE:
        cached = ARTIST_CACHE[artist_id]
        logging.debug(f"Artist area served from cache {cached}")
        return {"name": None if cached == "none" else cached}

    url = f"https://musicbrainz.org/ws/2/artist/{artist_id}"
    params = {"fmt": "json"}
    logging.debug(f"Requesting Area with url {url}")

    artist_json = throttled_get(url, params=params).json()

    area_obj = artist_json.get("area")  # may be missing
    area_name = area_obj.get("name") if isinstance(area_obj, dict) else None

    ARTIST_CACHE[artist_id] = area_name if area_name else "none"
    append_artist_cache_csv(artist_id, area_name)
    print(f"Area debug {area_name}")
    return {"name": area_name}

# Saves results to as CSV
def save_results_to_csv(results, filename):
    if not results:
        logging.debug("No data to save.")
        return

    fieldnames = results[0].keys()

    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    logging.debug(f"Saved {len(results)} records to {filename}")


# Here is the main logic For each year -> get tracks -> search for rating and artist. This also checks if the track ID exists already
def main():
    my_years = [1985,1986,1987,1988,1989,1990,1991,1992,1993,1994,1995,1996,1997,1998,1999]
    for my_year in my_years:
        output_path = rf"punk_{my_year}_results.csv"

        load_artist_cache_csv()
        processed = load_processed_recording_ids_csv(output_path)
        logging.info(f"Resume enabled: {len(processed)} recordings already saved in CSV.")

        recordings = search_recordings(my_year)
        logging.debug(f"Found {len(recordings)} recordings. Fetching ratings…")

        results = []

        for idx, rec in enumerate(recordings, start=1):
            rec_id = rec["id"]

            if rec_id in processed:
                logging.debug(f"[{idx}/{len(recordings)}] Skipping already saved recording {rec_id}")
                continue

            try:
                artist_id = rec["artist-credit"][0]["artist"]["id"]
                rec_title = rec.get("title", "Unknown Title")
                rec_artist = rec["artist-credit"][0]["name"]

                rating = fetch_rating(rec_id)
                vote_count = rating.get("votes-count", 0)
                average = rating.get("value", 0)

                raw_area = get_country_of_origin(artist_id)
                area = raw_area.get("name","Unknown Area")

                logging.debug(f"Saving Song Entry {rec_title}")

                result = {
                    "title": rec_title,
                    "artist": rec_artist,
                    "mbid": rec_id,
                    "votes": vote_count,
                    "rating": average,
                    "area": area,
                    "raw_request_record": rec,
                    "raw_request_rating": rating,
                    "raw_request_artist": raw_area,
                }

                results.append(result)

                append_result_csv(output_path, result)
                processed.add(rec_id)

                logging.info(f"[{idx}/{len(recordings)}] Saved: {rec_title} — {rec_artist} (votes: {vote_count})")

            except Exception:
                logging.exception(f"[{idx}/{len(recordings)}] Failed processing recording {rec_id}")
                continue




if __name__ == "__main__":
    setup_logging()
    main()
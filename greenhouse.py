import asyncio
import hashlib
import json
import os
import random
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional, Tuple, List
from urllib.parse import urlparse
from pathlib import Path
import re

import aiohttp
from dotenv import load_dotenv

# Load local env file
load_dotenv(".env.local")

# ----------------------------
# Configuration
# ----------------------------
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
JITTER_SECONDS = int(os.getenv("JITTER_SECONDS", "10"))
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "20"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "20"))

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_GREENHOUSE", "").strip()
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

#Directory 
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
COMPANIES_DIR = DATA_DIR / "companies"
WATCH_DIR = DATA_DIR / "watch"

COMPANIES_DIR.mkdir(parents=True, exist_ok=True)
WATCH_DIR.mkdir(parents=True, exist_ok=True)

COMPANIES_FILE = COMPANIES_DIR / "greenhouse_companies.txt"

DB_PATH = WATCH_DIR / os.getenv("GREENHOUSE_DB", "greenhouse_watch.db")
COMPANIES_FILE = os.getenv("COMPANIES_FILE", "greenhouse_companies.txt")


# ----------------------------
# Helpers
# ----------------------------
def job_absolute_url(slug: str, job_id: int) -> str:
    return f"https://boards.greenhouse.io/{slug}/jobs/{job_id}"


def slug_from_board_url(url: str) -> str:
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    if not parts:
        raise ValueError(f"Could not parse company slug from {url}")
    return parts[0]


def greenhouse_jobs_api(slug: str) -> str:
    return f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"


def normalize_board_url(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        raise ValueError("Empty line")

    # If just a slug, make it a URL
    if "://" not in s and "boards.greenhouse.io" not in s:
        return f"https://boards.greenhouse.io/{s.strip('/')}"

    # If missing scheme
    if "://" not in s and "boards.greenhouse.io" in s:
        s = "https://" + s

    return s.rstrip("/")


def load_board_links_from_file(path: str) -> List[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Companies file not found: {path}. Create it with one board URL per line."
        )

    links: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            raw = raw.split("#", 1)[0].strip()
            if not raw:
                continue

            try:
                url = normalize_board_url(raw)
                slug = slug_from_board_url(url)
                links.append(f"https://boards.greenhouse.io/{slug}")
            except Exception as e:
                print(f"[warn] Skipping invalid line {line_no} in {path}: {line!r} ({e})")

    # Dedupe preserve order
    seen = set()
    out: List[str] = []
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def stable_fingerprint(jobs_json: dict) -> str:
    jobs = jobs_json.get("jobs", [])
    compact = [{"id": j.get("id"), "updated_at": j.get("updated_at")} for j in jobs]
    blob = json.dumps(compact, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def format_new_jobs_message(slug: str, new_jobs: list, limit: int = 15) -> str:
    board_url = f"https://boards.greenhouse.io/{slug}"
    lines = [f"Board: {board_url}"]

    for j in sorted(new_jobs, key=lambda x: x.get("id") or 0)[:limit]:
        jid = j.get("id")
        title = (j.get("title") or "").strip()
        if jid and title:
            lines.append(f"{title} | {job_absolute_url(slug, jid)}")

    return "\n".join(lines)


# ----------------------------
# Title matching
# ----------------------------
KEYWORDS = ("software engineer", "software developer", "ai engineer", "full stack", "frontend engineer")

TITLE_NOISE_PATTERNS = [
    r"\bsenior\b",
    r"\bsr\.?\b",
    r"\bprincipal\b",
    r"\bstaff\b",
    r"\blead\b",
    r"\bjunior\b",
    r"\bmid\b",
    r"\bintern\b",
    r"\bii\b",
    r"\biii\b",
    r"\biv\b",
    r"\b1\b",
    r"\b2\b",
    r"\b3\b",
    r"\b4\b",
]

EXCLUDE_TITLE_PATTERNS = [
    r"\bsenior\b",
    r"\bsr\.?\b",
    r"\bstaff\b",
    r"\blead\b",
    r"\bprincipal\b",
    r"\bmanager\b",
    r"\bdirector\b",
    r"\bmachine\s*learning\b",
    r"\bml\b",
    r"\bdata engineer\b",
    r"\bfield engineer\b",
    r"\bembedded\b",
    r"\breliability engineer\b",
    r"\bnetwork engineer\b",
    r"\bsoftware engineer\s*ii\b",
    r"\bsoftware engineer\s*iii\b",
]


def normalize_title(title: str) -> str:
    t = (title or "").lower().strip()
    t = re.sub(r"[^\w\s]", " ", t)
    for pat in TITLE_NOISE_PATTERNS:
        t = re.sub(pat, " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def title_matches(title: str) -> bool:
    raw = (title or "").lower()
    if any(re.search(p, raw) for p in EXCLUDE_TITLE_PATTERNS):
        return False

    t = normalize_title(title)
    return any(k in t for k in KEYWORDS)


# ----------------------------
# US location matching
# ----------------------------
US_STATES = {
    "alabama","alaska","arizona","arkansas","california","colorado","connecticut","delaware",
    "florida","georgia","hawaii","idaho","illinois","indiana","iowa","kansas","kentucky","louisiana",
    "maine","maryland","massachusetts","michigan","minnesota","mississippi","missouri","montana",
    "nebraska","nevada","new hampshire","new jersey","new mexico","new york","north carolina",
    "north dakota","ohio","oklahoma","oregon","pennsylvania","rhode island","south carolina",
    "south dakota","tennessee","texas","utah","vermont","virginia","washington","west virginia",
    "wisconsin","wyoming",
    "district of columbia","washington dc","d c","d.c."
}

US_STATE_ABBR = {
    "al","ak","az","ar","ca","co","ct","de","fl","ga","hi","id","il","in","ia","ks","ky","la",
    "me","md","ma","mi","mn","ms","mo","mt","ne","nv","nh","nj","nm","ny","nc","nd","oh","ok",
    "or","pa","ri","sc","sd","tn","tx","ut","vt","va","wa","wv","wi","wy","dc"
}

US_COUNTRY_PATTERNS = [
    r"\busa\b",
    r"\bu\.s\.a\.?\b",
    r"\bu\.s\.?\b",
    r"\bunited states\b",
    r"\bunited states of america\b",
]

REMOTE_US_PATTERNS = [
    r"\bremote\b.*\b(us|usa|u\.s\.?|united states)\b",
    r"\b(us|usa|u\.s\.?|united states)\b.*\bremote\b",
]


def extract_location_texts(job: dict) -> List[str]:
    texts: List[str] = []

    loc = job.get("location")
    if isinstance(loc, dict):
        name = loc.get("name")
        if name:
            texts.append(str(name))

    for key in ("locations", "additional_locations"):
        arr = job.get(key)
        if isinstance(arr, list):
            for item in arr:
                if isinstance(item, dict) and item.get("name"):
                    texts.append(str(item["name"]))
                elif isinstance(item, str):
                    texts.append(item)

    return [t.strip() for t in texts if t and str(t).strip()]


def is_us_location_text(text: str) -> bool:
    t = (text or "").lower()

    if any(re.search(p, t) for p in US_COUNTRY_PATTERNS):
        return True

    if any(re.search(p, t) for p in REMOTE_US_PATTERNS):
        return True

    for state in US_STATES:
        if re.search(rf"\b{re.escape(state)}\b", t):
            return True

    # Abbr check: require word boundaries, avoid matching inside other words
    for abbr in US_STATE_ABBR:
        if re.search(rf"(?<![a-z]){abbr}(?![a-z])", t):
            return True

    return False


def job_is_us(job: dict) -> bool:
    return any(is_us_location_text(t) for t in extract_location_texts(job))


# ----------------------------
# Storage (sqlite)
# ----------------------------
@dataclass
class StoredState:
    etag: Optional[str]
    last_modified: Optional[str]
    fingerprint: Optional[str]
    last_seen_ts: Optional[int]
    notified_job_ids_json: Optional[str]


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS company_state (
            slug TEXT PRIMARY KEY,
            etag TEXT,
            last_modified TEXT,
            fingerprint TEXT,
            last_seen_ts INTEGER,
            notified_job_ids_json TEXT
        )
        """
    )

    desired = {
        "etag": "TEXT",
        "last_modified": "TEXT",
        "fingerprint": "TEXT",
        "last_seen_ts": "INTEGER",
        "notified_job_ids_json": "TEXT",
    }
    cols = {r[1] for r in conn.execute("PRAGMA table_info(company_state)").fetchall()}
    for col, col_type in desired.items():
        if col not in cols:
            conn.execute(f"ALTER TABLE company_state ADD COLUMN {col} {col_type}")

    conn.commit()
    return conn


def load_state(conn: sqlite3.Connection, slug: str) -> StoredState:
    row = conn.execute(
        "SELECT etag, last_modified, fingerprint, last_seen_ts, notified_job_ids_json FROM company_state WHERE slug = ?",
        (slug,),
    ).fetchone()
    if not row:
        return StoredState(None, None, None, None, None)
    return StoredState(row[0], row[1], row[2], row[3], row[4])


def save_state(
    conn: sqlite3.Connection,
    slug: str,
    etag: Optional[str],
    last_modified: Optional[str],
    fingerprint: Optional[str],
    last_seen_ts: int,
    notified_job_ids_json: Optional[str],
) -> None:
    conn.execute(
        """
        INSERT INTO company_state (slug, etag, last_modified, fingerprint, last_seen_ts, notified_job_ids_json)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(slug) DO UPDATE SET
            etag=excluded.etag,
            last_modified=excluded.last_modified,
            fingerprint=excluded.fingerprint,
            last_seen_ts=excluded.last_seen_ts,
            notified_job_ids_json=excluded.notified_job_ids_json
        """,
        (slug, etag, last_modified, fingerprint, last_seen_ts, notified_job_ids_json),
    )
    conn.commit()


# ----------------------------
# Notifications
# ----------------------------
async def post_webhook(session: aiohttp.ClientSession, url: str, text: str) -> None:
    if not url:
        return
    try:
        # Discord supports {"content": "..."}
        await session.post(url, json={"content": text}, timeout=TIMEOUT_SECONDS)
    except Exception as e:
        print(f"[warn] webhook post failed: {e}")


async def post_discord_long(session: aiohttp.ClientSession, text: str, max_len: int = 1900) -> None:
    if not DISCORD_WEBHOOK_URL:
        return

    lines = text.split("\n")
    chunk = ""

    for line in lines:
        if len(chunk) + len(line) + 1 > max_len:
            await post_webhook(session, DISCORD_WEBHOOK_URL, chunk)
            chunk = line
        else:
            chunk = f"{chunk}\n{line}" if chunk else line

    if chunk:
        await post_webhook(session, DISCORD_WEBHOOK_URL, chunk)


async def notify(session: aiohttp.ClientSession, message: str) -> None:
    print(message)

    # Slack expects {"text": "..."}
    if SLACK_WEBHOOK_URL:
        try:
            await session.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=TIMEOUT_SECONDS)
        except Exception as e:
            print(f"[warn] Slack notify failed: {e}")

    if DISCORD_WEBHOOK_URL:
        await post_discord_long(session, message)


# ----------------------------
# Polling
# ----------------------------
async def fetch_company(
    session: aiohttp.ClientSession, conn: sqlite3.Connection, slug: str
) -> Tuple[str, str]:
    api_url = greenhouse_jobs_api(slug)
    prior = load_state(conn, slug)

    try:
        notified_ids = set(json.loads(prior.notified_job_ids_json)) if prior.notified_job_ids_json else set()
    except Exception:
        notified_ids = set()

    headers = {}
    if prior.etag:
        headers["If-None-Match"] = prior.etag
    if prior.last_modified:
        headers["If-Modified-Since"] = prior.last_modified

    now_ts = int(time.time())

    try:
        async with session.get(api_url, headers=headers, timeout=TIMEOUT_SECONDS) as resp:
            if resp.status == 304:
                save_state(conn, slug, prior.etag, prior.last_modified, prior.fingerprint, now_ts, prior.notified_job_ids_json)
                return slug, "unchanged (304)"

            if resp.status != 200:
                save_state(conn, slug, prior.etag, prior.last_modified, prior.fingerprint, now_ts, prior.notified_job_ids_json)
                return slug, f"error HTTP {resp.status}"

            etag = resp.headers.get("ETag")
            last_modified = resp.headers.get("Last-Modified")

            data = await resp.json(content_type=None)
            fp = stable_fingerprint(data)

            jobs = data.get("jobs", [])
            by_id = {j.get("id"): j for j in jobs if j.get("id")}

            current_ids = set(by_id.keys())
            new_ids = [jid for jid in current_ids if jid not in notified_ids]

            new_matching: List[dict] = []
            for jid in new_ids:
                j = by_id.get(jid)
                if not j:
                    continue
                if title_matches(j.get("title", "")) and job_is_us(j):
                    new_matching.append(j)

            # Only send anything if there is a new match
            if new_matching:
                for j in new_matching:
                    notified_ids.add(j["id"])

                msg = format_new_jobs_message(slug, new_matching, limit=15)
                await notify(session, msg)

            save_state(
                conn,
                slug,
                etag,
                last_modified,
                fp,
                now_ts,
                json.dumps(sorted(list(notified_ids))),
            )

            return slug, "new match" if new_matching else "ok"

    except asyncio.TimeoutError:
        save_state(conn, slug, prior.etag, prior.last_modified, prior.fingerprint, now_ts, prior.notified_job_ids_json)
        return slug, "timeout"
    except Exception as e:
        save_state(conn, slug, prior.etag, prior.last_modified, prior.fingerprint, now_ts, prior.notified_job_ids_json)
        return slug, f"exception: {e}"


async def run_forever():
    board_links = load_board_links_from_file(COMPANIES_FILE)
    slugs = sorted({slug_from_board_url(u) for u in board_links})

    if not slugs:
        raise RuntimeError(f"No valid company boards found in {COMPANIES_FILE}. Add one per line.")

    print(f"Watching {len(slugs)} company boards (from {COMPANIES_FILE})")

    conn = init_db()
    timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        sem = asyncio.Semaphore(CONCURRENCY)

        async def bounded_fetch(s: str):
            async with sem:
                return await fetch_company(session, conn, s)

        while True:
            start = time.time()
            tasks = [asyncio.create_task(bounded_fetch(slug)) for slug in slugs]
            results = await asyncio.gather(*tasks)

            counts = {}
            for _, status in results:
                counts[status] = counts.get(status, 0) + 1
            summary = ", ".join(f"{k}: {v}" for k, v in sorted(counts.items()))
            print(f"Cycle done in {time.time() - start:.1f}s. {summary}")

            sleep_for = POLL_INTERVAL_SECONDS + random.randint(0, JITTER_SECONDS)
            await asyncio.sleep(sleep_for)


if __name__ == "__main__":
    try:
        asyncio.run(run_forever())
    except KeyboardInterrupt:
        print("Stopped.")

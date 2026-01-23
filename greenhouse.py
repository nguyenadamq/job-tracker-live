import asyncio
import hashlib
import json
import os
import random
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional, Tuple
from urllib.parse import urlparse
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

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

DB_PATH = os.getenv("DB_PATH", "greenhouse_watch.db")


# ----------------------------
# Combined company board links (previous + new)
# ----------------------------
BOARD_LINKS = [
    # --- previous list ---
    "https://boards.greenhouse.io/2k",
    "https://boards.greenhouse.io/8451",
    "https://boards.greenhouse.io/8451university",
    "https://boards.greenhouse.io/addepar1",
    "https://boards.greenhouse.io/affirm",
    "https://boards.greenhouse.io/afresh",
    "https://boards.greenhouse.io/alarmcom",
    "https://boards.greenhouse.io/alpaca",
    "https://boards.greenhouse.io/ambiententerprises",
    "https://boards.greenhouse.io/appliedintuition",
    "https://boards.greenhouse.io/applytopopulate",
    "https://boards.greenhouse.io/aurorainnovation",
    "https://boards.greenhouse.io/axios",
    "https://boards.greenhouse.io/axon",
    "https://boards.greenhouse.io/babelstreet",
    "https://boards.greenhouse.io/babylist",
    "https://boards.greenhouse.io/bitgo",
    "https://boards.greenhouse.io/bitpanda",
    "https://boards.greenhouse.io/blinkhealth",
    "https://boards.greenhouse.io/bloombergorg",
    "https://boards.greenhouse.io/bolt42",
    "https://boards.greenhouse.io/bwaltpostings",
    "https://boards.greenhouse.io/canonicaljobs",
    "https://boards.greenhouse.io/carmichaellynchrelate",
    "https://boards.greenhouse.io/chalkinc",
    "https://boards.greenhouse.io/check",
    "https://boards.greenhouse.io/checkr",
    "https://boards.greenhouse.io/clariticloudinc",
    "https://boards.greenhouse.io/coherehealth",
    "https://boards.greenhouse.io/codazen",
    "https://boards.greenhouse.io/correlationone",
    "https://boards.greenhouse.io/cortex",
    "https://boards.greenhouse.io/crunchyroll",
    "https://boards.greenhouse.io/curaleaf",
    "https://boards.greenhouse.io/dagsterlabs",
    "https://boards.greenhouse.io/day1academies",
    "https://boards.greenhouse.io/dfinity",
    "https://boards.greenhouse.io/doordashusa",
    "https://boards.greenhouse.io/ebanx",
    "https://boards.greenhouse.io/energage",
    "https://boards.greenhouse.io/evolvevacationrental",
    "https://boards.greenhouse.io/exodus54",
    "https://boards.greenhouse.io/faire",
    "https://boards.greenhouse.io/flexport",
    "https://boards.greenhouse.io/forteracorporation",
    "https://boards.greenhouse.io/found",
    "https://boards.greenhouse.io/gleanwork",
    "https://boards.greenhouse.io/hightouch",
    "https://boards.greenhouse.io/inkhousehq",
    "https://boards.greenhouse.io/kcare",
    "https://boards.greenhouse.io/lightspeedsystems",
    "https://boards.greenhouse.io/lucidsoftware",
    "https://boards.greenhouse.io/maddoxindustrialtransformer",
    "https://boards.greenhouse.io/mediaalpha",
    "https://boards.greenhouse.io/mercury",
    "https://boards.greenhouse.io/minware",
    "https://boards.greenhouse.io/momence",
    "https://boards.greenhouse.io/nearsure",
    "https://boards.greenhouse.io/ocrolusinc",
    "https://boards.greenhouse.io/oddball",
    "https://boards.greenhouse.io/okx",
    "https://boards.greenhouse.io/outschool",
    "https://boards.greenhouse.io/pacaso",
    "https://boards.greenhouse.io/perpay",
    "https://boards.greenhouse.io/prophecysimpledatalabs",
    "https://boards.greenhouse.io/quorum",
    "https://boards.greenhouse.io/regscale",
    "https://boards.greenhouse.io/rti",
    "https://boards.greenhouse.io/runwayml",
    "https://boards.greenhouse.io/sentryuniversityrecruiting",
    "https://boards.greenhouse.io/shakepay",
    "https://boards.greenhouse.io/singlestore",
    "https://boards.greenhouse.io/simpplr",
    "https://boards.greenhouse.io/snorkelai",
    "https://boards.greenhouse.io/stablekernel",
    "https://boards.greenhouse.io/stockx",
    "https://boards.greenhouse.io/tastylive",
    "https://boards.greenhouse.io/tenableinc",
    "https://boards.greenhouse.io/thetradedesk",
    "https://boards.greenhouse.io/trueanomalyinc",
    "https://boards.greenhouse.io/veeamsoftware",
    "https://boards.greenhouse.io/via",
    "https://boards.greenhouse.io/webflow",
    "https://boards.greenhouse.io/wisetack",
    "https://boards.greenhouse.io/wurljobs",
    "https://boards.greenhouse.io/zscaler",

    # --- new list ---
    "https://boards.greenhouse.io/6sense",
    "https://boards.greenhouse.io/airbnb",
    "https://boards.greenhouse.io/airtable",
    "https://boards.greenhouse.io/alma",
    "https://boards.greenhouse.io/andurilindustries",
    "https://boards.greenhouse.io/astranis",
    "https://boards.greenhouse.io/biograph",
    "https://boards.greenhouse.io/careaccess",
    "https://boards.greenhouse.io/chainguard",
    "https://boards.greenhouse.io/charterup",
    "https://boards.greenhouse.io/clockworksystems",
    "https://boards.greenhouse.io/cloudflare",
    "https://boards.greenhouse.io/contentful",
    "https://boards.greenhouse.io/databricks",
    "https://boards.greenhouse.io/dorsia",
    "https://boards.greenhouse.io/elementcritical",
    "https://boards.greenhouse.io/esri",
    "https://boards.greenhouse.io/flagshippioneeringinc",
    "https://boards.greenhouse.io/fluxx",
    "https://boards.greenhouse.io/gallup",
    "https://boards.greenhouse.io/grahamcapitalmanagement",
    "https://boards.greenhouse.io/hoyoverse",
    "https://boards.greenhouse.io/inchargeenergy",
    "https://boards.greenhouse.io/internshiplist2000",
    "https://boards.greenhouse.io/kardfinancialinc",
    "https://boards.greenhouse.io/linkedinlimitedpostings",
    "https://boards.greenhouse.io/locusrobotics",
    "https://boards.greenhouse.io/lotlinx",
    "https://boards.greenhouse.io/lush",
    "https://boards.greenhouse.io/mara",
    "https://boards.greenhouse.io/masterclass",
    "https://boards.greenhouse.io/metronome",
    "https://boards.greenhouse.io/morty",
    "https://boards.greenhouse.io/netdocuments",
    "https://boards.greenhouse.io/nex",
    "https://boards.greenhouse.io/okta",
    "https://boards.greenhouse.io/ophelia",
    "https://boards.greenhouse.io/opploans",
    "https://boards.greenhouse.io/outfit7",
    "https://boards.greenhouse.io/pallet",
    "https://boards.greenhouse.io/robinhood",
    "https://boards.greenhouse.io/rocketems",
    "https://boards.greenhouse.io/samsara",
    "https://boards.greenhouse.io/sensiblecare",
    "https://boards.greenhouse.io/sharebite",
    "https://boards.greenhouse.io/simplifynext",
    "https://boards.greenhouse.io/spacex",
    "https://boards.greenhouse.io/thepacgroup",
    "https://boards.greenhouse.io/traderepublicbank",
    "https://boards.greenhouse.io/twilio",
    "https://boards.greenhouse.io/usaforunhcr",
    "https://boards.greenhouse.io/vast",
    "https://boards.greenhouse.io/vonage",
    "https://boards.greenhouse.io/wikimedia",
]

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


# Only notify for jobs whose TITLE contains one of these (case-insensitive).
KEYWORDS = ("engineer", "developer", "software", "full stack")

# Words/phrases to ignore in titles (levels + seniority)
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

def normalize_title(title: str) -> str:
    """
    Normalize job titles so level variations don't create separate matches.
    Example:
      "Senior Software Engineer II" -> "software engineer"
      "Sr. Software Engineer" -> "software engineer"
      "Principal Software Engineer III" -> "software engineer"
    """
    t = (title or "").lower().strip()

    # Remove punctuation that breaks matching
    t = re.sub(r"[^\w\s]", " ", t)

    # Remove noise words (seniority/levels)
    for pat in TITLE_NOISE_PATTERNS:
        t = re.sub(pat, " ", t)

    # Collapse whitespace
    t = re.sub(r"\s+", " ", t).strip()
    return t

def title_matches(title: str) -> bool:
    """
    Keyword match runs on normalized title.
    """
    t = normalize_title(title)
    return any(k in t for k in KEYWORDS)



def stable_fingerprint(jobs_json: dict) -> str:
    """
    Fingerprint based on job ids + updated_at so we detect:
      - new jobs
      - removed jobs
      - edits (updated_at changes)
    """
    jobs = jobs_json.get("jobs", [])
    compact = [{"id": j.get("id"), "updated_at": j.get("updated_at")} for j in jobs]
    blob = json.dumps(compact, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


@dataclass
class StoredState:
    etag: Optional[str]
    last_modified: Optional[str]
    fingerprint: Optional[str]
    last_seen_ts: Optional[int]
    notified_job_ids_json: Optional[str]


# ----------------------------
# Storage (sqlite)
# ----------------------------
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

    # Backward-compatible migration (if old DB exists)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(company_state)").fetchall()]
    if "notified_job_ids_json" not in cols:
        conn.execute("ALTER TABLE company_state ADD COLUMN notified_job_ids_json TEXT")

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
        await session.post(url, json={"content": text}, timeout=TIMEOUT_SECONDS)
    except Exception as e:
        print(f"[warn] webhook post failed: {e}")


async def notify(session: aiohttp.ClientSession, message: str) -> None:
    print(message)
    if SLACK_WEBHOOK_URL:
        try:
            await session.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=TIMEOUT_SECONDS)
        except Exception as e:
            print(f"[warn] Slack notify failed: {e}")
    if DISCORD_WEBHOOK_URL:
        await post_webhook(session, DISCORD_WEBHOOK_URL, message)


# ----------------------------
# Polling
# ----------------------------
async def fetch_company(session: aiohttp.ClientSession, conn: sqlite3.Connection, slug: str) -> Tuple[str, str]:
    """
    Notifies ONLY when it sees a new job id for that company AND the job title matches KEYWORDS.
    """
    api_url = greenhouse_jobs_api(slug)
    prior = load_state(conn, slug)

    # Load previously-notified job ids
    try:
        notified_ids = set(json.loads(prior.notified_job_ids_json)) if prior.notified_job_ids_json else set()
    except Exception:
        notified_ids = set()

    headers = {}
    if prior.etag:
        headers["If-None-Match"] = prior.etag
    if prior.last_modified:
        headers["If-Modified-Since"] = prior.last_modified

    try:
        async with session.get(api_url, headers=headers, timeout=TIMEOUT_SECONDS) as resp:
            now_ts = int(time.time())

            if resp.status == 304:
                save_state(conn, slug, prior.etag, prior.last_modified, prior.fingerprint, now_ts, prior.notified_job_ids_json)
                return slug, "unchanged (304)"

            if resp.status != 200:
                # Preserve prior state on transient errors
                save_state(conn, slug, prior.etag, prior.last_modified, prior.fingerprint, now_ts, prior.notified_job_ids_json)
                return slug, f"error HTTP {resp.status}"

            etag = resp.headers.get("ETag")
            last_modified = resp.headers.get("Last-Modified")

            data = await resp.json(content_type=None)
            fp = stable_fingerprint(data)

            first_seen = (prior.fingerprint is None)
            changed = (not first_seen and fp != prior.fingerprint)

            jobs = data.get("jobs", [])
            by_id = {j.get("id"): j for j in jobs if j.get("id")}

            current_ids = set(by_id.keys())
            new_ids = [jid for jid in current_ids if jid not in notified_ids]

            # Only notify for new jobs whose title matches keywords
            new_matching = []
            for jid in new_ids:
                j = by_id.get(jid)
                if not j:
                    continue
                if title_matches(j.get("title", "")):
                    new_matching.append(j)

            if new_matching:
                # mark notified
                for j in new_matching:
                    notified_ids.add(j["id"])

                board_url = f"https://boards.greenhouse.io/{slug}"
                lines = [
                    f"[new matching job] {slug}",
                    f"Board: {board_url}",
                    "Jobs:",
                ]

                # Keep message compact (Discord limit safety)
                for j in sorted(new_matching, key=lambda x: x.get("id"))[:15]:
                    jid = j.get("id")
                    title = (j.get("title") or "").strip()
                    url = job_absolute_url(slug, jid)
                    lines.append(f"{title} | {url}")

                await notify(session, "\n".join(lines))

            save_state(
                conn,
                slug,
                etag,
                last_modified,
                fp,
                now_ts,
                json.dumps(sorted(list(notified_ids))),
            )

            if first_seen:
                return slug, "initialized"
            return slug, "changed" if changed else "unchanged (200)"

    except asyncio.TimeoutError:
        now_ts = int(time.time())
        save_state(conn, slug, prior.etag, prior.last_modified, prior.fingerprint, now_ts, prior.notified_job_ids_json)
        return slug, "timeout"
    except Exception as e:
        now_ts = int(time.time())
        save_state(conn, slug, prior.etag, prior.last_modified, prior.fingerprint, now_ts, prior.notified_job_ids_json)
        return slug, f"exception: {e}"


async def run_forever():
    slugs = sorted({slug_from_board_url(u) for u in BOARD_LINKS})
    print(f"Watching {len(slugs)} company boards")

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

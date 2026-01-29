import asyncio
import json
import os
import random
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, unquote

import aiohttp
from dotenv import load_dotenv

load_dotenv(".env.local")

# ----------------------------
# Config
# ----------------------------

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
COMPANIES_DIR = DATA_DIR / "companies"
WATCH_DIR = DATA_DIR / "watch"

COMPANIES_DIR.mkdir(parents=True, exist_ok=True)
WATCH_DIR.mkdir(parents=True, exist_ok=True)

COMPANIES_FILE = COMPANIES_DIR / "ashbyhq_companies.txt"

# Use the env var name
DB_PATH = WATCH_DIR / os.getenv("ASHBY_DB", "ashbyhq_watch.db")

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
JITTER_SECONDS = int(os.getenv("JITTER_SECONDS", "10"))
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "20"))

# 0 = all
COMPANY_LIMIT = int(os.getenv("COMPANY_LIMIT", "0"))

# companies/sec (start rate)
COMPANIES_PER_SECOND = float(os.getenv("COMPANIES_PER_SECOND", "3.0"))

# in-flight requests cap (keep modest)
CONCURRENCY = int(os.getenv("CONCURRENCY", "3"))

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_ASHBYHQ", "").strip()
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()


ASHBY_GQL_ENDPOINT = "https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams"

RE_SPACES = re.compile(r"\s+")
RE_PUNCT_TO_SPACE = re.compile(r"[^\w\s-]", re.UNICODE)

# ----------------------------
# Title matching (match Greenhouse behavior)
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


def normalize_title_gh(title: str) -> str:
    t = (title or "").lower().strip()
    t = re.sub(r"[^\w\s]", " ", t)
    for pat in TITLE_NOISE_PATTERNS:
        t = re.sub(pat, " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def title_allowed(title: str) -> bool:
    raw = (title or "").lower()
    if any(re.search(p, raw) for p in EXCLUDE_TITLE_PATTERNS):
        return False
    t = normalize_title_gh(title)
    return any(k in t for k in KEYWORDS)


# ----------------------------
# US location matching (match Greenhouse behavior)
# Works on Ashby's locationName string
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


def is_us_location(location_text: str) -> bool:
    t = (location_text or "").lower()

    if any(re.search(p, t) for p in US_COUNTRY_PATTERNS):
        return True

    if any(re.search(p, t) for p in REMOTE_US_PATTERNS):
        return True

    for state in US_STATES:
        if re.search(rf"\b{re.escape(state)}\b", t):
            return True

    for abbr in US_STATE_ABBR:
        if re.search(rf"(?<![a-z]){abbr}(?![a-z])", t):
            return True

    return False


def infer_work_type_from_text(s: str) -> str:
    t = (s or "").lower()
    if "remote" in t:
        return "remote"
    if "hybrid" in t:
        return "hybrid"
    return "on-site"


def normalize_ashby_workplace(workplace: str, location_text: str) -> str:
    w = (workplace or "").lower().strip()
    if w in ("remote", "hybrid", "onsite", "on-site"):
        return "on-site" if w in ("onsite", "on-site") else w
    return infer_work_type_from_text(location_text)


# ----------------------------
# Company list parsing
# ----------------------------
def slugify_ashby_hosted_page_name(s: str) -> str:
    raw = (s or "").strip()
    if not raw:
        return ""

    if "://" in raw:
        parsed = urlparse(raw)
        parts = [p for p in parsed.path.split("/") if p]
        raw = parts[0] if parts else raw

    raw = unquote(raw).strip()
    raw = raw.replace("Careers Page", "").replace("careers page", "").strip()
    raw = raw.replace("&", "and")
    raw = raw.replace("_", "-")
    raw = re.sub(r"\s+", "-", raw)
    raw = re.sub(r"[^a-zA-Z0-9-]", "", raw)
    raw = raw.strip("-").lower()
    raw = re.sub(r"-{2,}", "-", raw)
    return raw


def load_company_slugs_from_file(path: str) -> List[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Companies file not found: {path}")

    slugs: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            raw = raw.split("#", 1)[0].strip()
            if not raw:
                continue

            slug = slugify_ashby_hosted_page_name(raw)
            if not slug:
                print(f"[warn] skipping invalid line {line_no}: {line!r}")
                continue
            slugs.append(slug)

    seen = set()
    out: List[str] = []
    for s in slugs:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


# ----------------------------
# Rate limiter (start N companies/sec)
# ----------------------------
class StartRateLimiter:
    def __init__(self, companies_per_second: float) -> None:
        self.interval = 1.0 / max(companies_per_second, 0.0001)
        self._lock = asyncio.Lock()
        self._next_allowed = time.monotonic()

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if now < self._next_allowed:
                await asyncio.sleep(self._next_allowed - now)
            self._next_allowed = time.monotonic() + self.interval + random.uniform(0.0, self.interval * 0.25)


# ----------------------------
# Ashby GraphQL call
# ----------------------------
ASHBY_QUERY_JOBBOARD_WITH_TEAMS = """
query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) {
  jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) {
    jobPostings {
      id
      title
      locationName
      workplaceType
      employmentType
    }
  }
}
"""


async def fetch_postings(session: aiohttp.ClientSession, slug: str) -> tuple[str, List[Dict[str, Any]]]:
    payload = {
        "operationName": "ApiJobBoardWithTeams",
        "query": ASHBY_QUERY_JOBBOARD_WITH_TEAMS,
        "variables": {"organizationHostedJobsPageName": slug},
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Referer": f"https://jobs.ashbyhq.com/{slug}",
        "Origin": "https://jobs.ashbyhq.com",
    }

    async with session.post(ASHBY_GQL_ENDPOINT, json=payload, headers=headers, timeout=TIMEOUT_SECONDS) as resp:
        if resp.status != 200:
            return f"http_{resp.status}", []

        data = await resp.json(content_type=None)
        if isinstance(data, dict) and data.get("errors"):
            msg = (data["errors"][0].get("message") or "graphql_error").strip()
            return f"gql_error:{msg}", []

        postings = (((data.get("data") or {}).get("jobBoardWithTeams") or {}).get("jobPostings") or [])
        return "ok", postings


def filter_matches(slug: str, postings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for p in postings:
        title = (p.get("title") or "").strip()
        if not title_allowed(title):
            continue

        opening_id = str(p.get("id") or "").strip()
        if not opening_id:
            continue

        location_name = str(p.get("locationName") or "").strip()
        if not is_us_location(location_name):
            continue

        work_type = normalize_ashby_workplace(str(p.get("workplaceType") or ""), location_name)

        out.append(
            {
                "company": slug,
                "title": title,
                "id": opening_id,
                "url": f"https://jobs.ashbyhq.com/{slug}/{opening_id}",
                "location": location_name,
                "work_type": work_type,
            }
        )
    return out


# ----------------------------
# DB (store notified ids per company)
# ----------------------------
@dataclass
class StoredState:
    last_seen_ts: Optional[int]
    notified_job_ids_json: Optional[str]


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS company_state (
            slug TEXT PRIMARY KEY,
            last_seen_ts INTEGER,
            notified_job_ids_json TEXT
        )
        """
    )
    conn.commit()
    return conn


def load_state(conn: sqlite3.Connection, slug: str) -> StoredState:
    row = conn.execute(
        "SELECT last_seen_ts, notified_job_ids_json FROM company_state WHERE slug = ?",
        (slug,),
    ).fetchone()
    if not row:
        return StoredState(None, None)
    return StoredState(row[0], row[1])


def save_state(conn: sqlite3.Connection, slug: str, last_seen_ts: int, notified_job_ids_json: Optional[str]) -> None:
    conn.execute(
        """
        INSERT INTO company_state (slug, last_seen_ts, notified_job_ids_json)
        VALUES (?, ?, ?)
        ON CONFLICT(slug) DO UPDATE SET
            last_seen_ts=excluded.last_seen_ts,
            notified_job_ids_json=excluded.notified_job_ids_json
        """,
        (slug, last_seen_ts, notified_job_ids_json),
    )
    conn.commit()


# ----------------------------
# Discord notify (one msg per company per cycle, only for NEW matches)
# ----------------------------
def format_discord_message(company: str, matches: List[Dict[str, Any]], max_jobs: int = 25) -> str:
    lines = [f"[{company}]"]
    for j in matches[:max_jobs]:
        title = (j.get("title") or "").strip()
        url = (j.get("url") or "").strip()
        if title and url:
            lines.append(f"{title} - {url}")
    return "\n".join(lines)


async def post_discord(session: aiohttp.ClientSession, text: str, max_len: int = 1900) -> None:
    if not DISCORD_WEBHOOK_URL:
        return

    # chunk if needed
    lines = text.split("\n")
    chunk = ""
    for line in lines:
        if len(chunk) + len(line) + 1 > max_len:
            await _post_discord_chunk(session, chunk)
            chunk = line
        else:
            chunk = f"{chunk}\n{line}" if chunk else line

    if chunk:
        await _post_discord_chunk(session, chunk)


async def _post_discord_chunk(session: aiohttp.ClientSession, text: str) -> None:
    try:
        await session.post(DISCORD_WEBHOOK_URL, json={"content": text}, timeout=TIMEOUT_SECONDS)
    except Exception as e:
        print(f"[warn] Discord webhook failed: {e}")


# ----------------------------
# Per-company fetch
# ----------------------------
async def fetch_one(
    session: aiohttp.ClientSession,
    conn: sqlite3.Connection,
    limiter: StartRateLimiter,
    sem: asyncio.Semaphore,
    slug: str,
    print_raw: bool = False,
) -> Tuple[str, str, int, int]:
    await limiter.wait()
    async with sem:
        prior = load_state(conn, slug)
        try:
            notified_ids = set(json.loads(prior.notified_job_ids_json)) if prior.notified_job_ids_json else set()
        except Exception:
            notified_ids = set()

        now_ts = int(time.time())

        try:
            status, postings = await fetch_postings(session, slug)
            if status != "ok":
                save_state(conn, slug, now_ts, prior.notified_job_ids_json)
                return slug, status, 0, 0

            if print_raw:
                print(f"\n[{slug}] total_postings={len(postings)}")
                for i, p in enumerate(postings, 1):
                    pid = str(p.get("id") or "").strip()
                    title = (p.get("title") or "").strip()
                    loc = (p.get("locationName") or "").strip()
                    workplace = (p.get("workplaceType") or "").strip()
                    emp = (p.get("employmentType") or "").strip()
                    url = f"https://jobs.ashbyhq.com/{slug}/{pid}" if pid else ""
                    print(f"{i:03d}. {title} | {loc} | workplace={workplace} | emp={emp} | {url}")

            matches = filter_matches(slug, postings)

            # Only notify on newly-seen matching jobs
            new_matches: List[Dict[str, Any]] = []
            for j in matches:
                jid = j.get("id")
                if jid and jid not in notified_ids:
                    new_matches.append(j)

            if new_matches:
                for j in new_matches:
                    notified_ids.add(j["id"])
                msg = format_discord_message(slug, new_matches)
                await post_discord(session, msg)

            save_state(conn, slug, now_ts, json.dumps(sorted(list(notified_ids))))
            return slug, "new match" if new_matches else "ok", len(postings), len(new_matches)

        except asyncio.TimeoutError:
            save_state(conn, slug, now_ts, prior.notified_job_ids_json)
            return slug, "timeout", 0, 0
        except Exception as e:
            save_state(conn, slug, now_ts, prior.notified_job_ids_json)
            return slug, f"exception:{e}", 0, 0


# ----------------------------
# Main loop (runs forever)
# ----------------------------
async def run_forever() -> None:
    slugs = load_company_slugs_from_file(COMPANIES_FILE)
    if COMPANY_LIMIT > 0:
        slugs = slugs[:COMPANY_LIMIT]

    if not slugs:
        raise RuntimeError(f"No valid companies found in {COMPANIES_FILE}")

    print(f"Watching {len(slugs)} Ashby companies (from {COMPANIES_FILE})")
    print(f"Rate: {COMPANIES_PER_SECOND} companies/sec, concurrency={CONCURRENCY}, timeout={TIMEOUT_SECONDS}s")
    print(f"Poll every {POLL_INTERVAL_SECONDS}s (+ up to {JITTER_SECONDS}s jitter)")
    if not DISCORD_WEBHOOK_URL:
        print("[warn] DISCORD_WEBHOOK_ASHBYHQ is not set, so nothing will be sent to Discord")
    print(f"DB: {DB_PATH}")

    conn = init_db()

    timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    sem = asyncio.Semaphore(CONCURRENCY)
    limiter = StartRateLimiter(COMPANIES_PER_SECOND)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        while True:
            start = time.time()

            tasks = [
                asyncio.create_task(fetch_one(session, conn, limiter, sem, slug, print_raw=False))
                for slug in slugs
            ]
            results = await asyncio.gather(*tasks)

            counts: Dict[str, int] = {}
            total_postings = 0
            total_new_matches = 0

            for _, status, postings_n, new_matches_n in results:
                counts[status] = counts.get(status, 0) + 1
                total_postings += postings_n
                total_new_matches += new_matches_n

            summary = ", ".join(f"{k}: {v}" for k, v in sorted(counts.items()))
            elapsed = time.time() - start
            print(
                f"Cycle done in {elapsed:.1f}s. total_postings={total_postings} "
                f"new_matches={total_new_matches}. {summary}"
            )

            sleep_for = POLL_INTERVAL_SECONDS + random.randint(0, max(JITTER_SECONDS, 0))
            await asyncio.sleep(sleep_for)


if __name__ == "__main__":
    try:
        asyncio.run(run_forever())
    except KeyboardInterrupt:
        print("Stopped.")

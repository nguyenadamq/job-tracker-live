import asyncio
import json
import os
import re
from dataclasses import dataclass
from typing import Iterable, Set, Tuple
from urllib.parse import urlparse

import aiohttp
from dotenv import load_dotenv

load_dotenv(".env.local")

CONCURRENCY = int(os.getenv("CONCURRENCY", "20"))
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "25"))
PER_SEED_BYTE_LIMIT = int(os.getenv("PER_SEED_BYTE_LIMIT", str(5_000_000)))  # 5 MB safety cap
VALIDATION_CONCURRENCY = int(os.getenv("VALIDATION_CONCURRENCY", "40"))

# 10 seed URLs (no Google Cloud needed)
SEED_URLS = [
    # SimplifyJobs New Grad
    "https://raw.githubusercontent.com/SimplifyJobs/New-Grad-Positions/main/README.md",
    "https://raw.githubusercontent.com/SimplifyJobs/New-Grad-Positions/main/listings.json",
    "https://raw.githubusercontent.com/SimplifyJobs/New-Grad-Positions/main/CONTRIBUTING.md",

    # SimplifyJobs Summer 2026 Internships
    "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/main/README.md",
    "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/main/listings.json",
    "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/main/CONTRIBUTING.md",

    # Extra high-signal pages with lots of outbound job links
    # InternshipList job board page (Greenhouse job-boards subdomain)
    "https://job-boards.greenhouse.io/internshiplist2000",

    # A couple GitHub issue pages often contain ATS links in templates/comments
    "https://github.com/SimplifyJobs/New-Grad-Positions/issues",
    "https://github.com/SimplifyJobs/Summer2026-Internships/issues",

    # A third-party page that frequently includes ATS links (can change over time)
    "https://careerpowerup.com/find-hidden-remote-job-openings/",

    "https://www.newgrad-jobs.com/",
    "https://www.firstwavejobs.com/"
]

URL_RE = re.compile(r"https?://[^\s)>\"]+")

# Match board slugs from URLs we find in the seed content
GH_HOSTS = {"boards.greenhouse.io", "job-boards.greenhouse.io"}
LEVER_HOST = "jobs.lever.co"
ASHBY_HOST = "jobs.ashbyhq.com"

def greenhouse_api(slug: str) -> str:
    # Greenhouse boards API uses the same slug even when the site is job-boards.greenhouse.io
    return f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"

def lever_api(slug: str) -> str:
    return f"https://api.lever.co/v0/postings/{slug}?mode=json"

def ashby_api(slug: str) -> str:
    return f"https://jobs.ashbyhq.com/api/non-auth/job-board/{slug}"

def normalize_slug(s: str) -> str:
    # Keep it simple and safe: strip trailing punctuation and whitespace
    return (s or "").strip().strip("/").strip(".,);]\"'")

def extract_slug_from_url(u: str) -> Tuple[str, str] | None:
    """
    Returns (platform, slug) where platform in {"greenhouse","lever","ashby"}.
    """
    try:
        p = urlparse(u)
        host = (p.netloc or "").lower()
        parts = [x for x in (p.path or "").split("/") if x]

        if host in GH_HOSTS:
            if not parts:
                return None
            return ("greenhouse", normalize_slug(parts[0]))

        if host == LEVER_HOST:
            if not parts:
                return None
            return ("lever", normalize_slug(parts[0]))

        if host == ASHBY_HOST:
            if not parts:
                return None
            # Handle /api/non-auth/job-board/{company}
            if len(parts) >= 4 and parts[0] == "api" and parts[1] == "non-auth" and parts[2] == "job-board":
                return ("ashby", normalize_slug(parts[3]))
            return ("ashby", normalize_slug(parts[0]))

        return None
    except Exception:
        return None

async def fetch_text(session: aiohttp.ClientSession, url: str) -> str:
    # Stream with a byte cap so you do not accidentally download huge pages forever
    async with session.get(url) as resp:
        resp.raise_for_status()
        chunks = []
        total = 0
        async for chunk in resp.content.iter_chunked(65536):
            total += len(chunk)
            if total > PER_SEED_BYTE_LIMIT:
                break
            chunks.append(chunk)
        return b"".join(chunks).decode("utf-8", errors="ignore")

async def validate_url(session: aiohttp.ClientSession, url: str) -> bool:
    try:
        async with session.get(url) as resp:
            # For these APIs, 200 means the slug is valid.
            return resp.status == 200
    except Exception:
        return False

async def validate_slugs(
    session: aiohttp.ClientSession,
    slugs: Iterable[str],
    api_fn,
    label: str,
) -> Set[str]:
    slugs_list = list(dict.fromkeys(slugs))  # stable dedupe
    sem = asyncio.Semaphore(VALIDATION_CONCURRENCY)

    async def one(slug: str) -> Tuple[str, bool]:
        async with sem:
            ok = await validate_url(session, api_fn(slug))
            return slug, ok

    tasks = [asyncio.create_task(one(s)) for s in slugs_list]
    results = await asyncio.gather(*tasks)

    valid = {slug for slug, ok in results if ok}
    print(f"Validated {label}: {len(valid)}/{len(slugs_list)}")
    return valid

def write_company_file(path: str, base_url: str, slugs: Set[str]) -> None:
    lines = [f"{base_url}/{s}" for s in sorted(slugs)]
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"Wrote {len(lines)} -> {path}")

async def main():
    timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    headers = {
        "User-Agent": "job-tracker-seed-crawler/1.0"
    }

    greenhouse_slugs: Set[str] = set()
    lever_slugs: Set[str] = set()
    ashby_slugs: Set[str] = set()

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
        sem = asyncio.Semaphore(CONCURRENCY)

        async def process_seed(url: str) -> None:
            async with sem:
                try:
                    text = await fetch_text(session, url)
                except Exception as e:
                    print(f"[seed failed] {url} -> {e}")
                    return

                urls = URL_RE.findall(text)
                for u in urls:
                    out = extract_slug_from_url(u)
                    if not out:
                        continue
                    platform, slug = out
                    if not slug:
                        continue
                    if platform == "greenhouse":
                        greenhouse_slugs.add(slug)
                    elif platform == "lever":
                        lever_slugs.add(slug)
                    elif platform == "ashby":
                        ashby_slugs.add(slug)

                print(
                    f"[seed ok] {url} "
                    f"GH={len(greenhouse_slugs)} Lever={len(lever_slugs)} Ashby={len(ashby_slugs)}"
                )

        await asyncio.gather(*(process_seed(u) for u in SEED_URLS))

        print("\nRaw extracted slugs:")
        print(f"  Greenhouse: {len(greenhouse_slugs)}")
        print(f"  Lever:      {len(lever_slugs)}")
        print(f"  Ashby:      {len(ashby_slugs)}\n")

        # Validate via official public endpoints
        gh_valid = await validate_slugs(session, greenhouse_slugs, greenhouse_api, "Greenhouse")
        lever_valid = await validate_slugs(session, lever_slugs, lever_api, "Lever")
        ashby_valid = await validate_slugs(session, ashby_slugs, ashby_api, "Ashby")

    # Write outputs
    write_company_file("greenhouse_companies.txt", "https://boards.greenhouse.io", gh_valid)
    write_company_file("lever_companies.txt", "https://jobs.lever.co", lever_valid)
    write_company_file("ashby_companies.txt", "https://jobs.ashbyhq.com", ashby_valid)

    print("\nDone.")
    print(f"Final valid: GH={len(gh_valid)} Lever={len(lever_valid)} Ashby={len(ashby_valid)}")

if __name__ == "__main__":
    asyncio.run(main())

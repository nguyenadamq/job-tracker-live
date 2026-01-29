# Job Board Tracker

A job monitoring system that discovers company-hosted job boards and continuously checks them for new postings. It filters for target roles and US locations, stores per-company state locally, and sends notifications to Discord.

## What’s Included

### 1) Company Discovery (Build company lists)
**Script:** `discover_companies.py`  
**Tech:** Python, Requests, SerpAPI (Google Search API)  
**Output files:**
- `ashbyhq_companies.txt`
- `greenhouse_companies.txt`
- `lever_companies.txt`

### 2) Job Board Monitors (Watch boards and notify)
Each monitor:
- Reads a company list file (one company per line)
- Polls the board API in a loop
- Stores state in SQLite to prevent duplicate alerts
- Sends notifications only for newly-seen matching jobs

**Monitors and tech**
- Ashby: Python, asyncio, aiohttp, SQLite, GraphQL
- Greenhouse: Python, asyncio, aiohttp, SQLite, REST
- Lever: Python, asyncio, aiohttp, SQLite, REST

---

## Requirements

- Python 3.10+ recommended
- Packages:
  - `python-dotenv`
  - `requests` (for discovery)
  - `aiohttp` (for monitors)

Install dependencies:
```bash
pip install -r requirements.txt

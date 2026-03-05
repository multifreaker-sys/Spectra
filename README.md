<p align="center">
  <img src="assets/icon.png" alt="Spectra" width="180" />
</p>

<h1 align="center">Spectra</h1>
<p align="center">
  <strong>Bank CSV/PDF → Categorization → Local Dashboard (optional Google Sheets)</strong><br>
  Your personal finance dashboard, fully automated — local-first, export-based.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.10+-blue" alt="Python" />
  <img src="https://img.shields.io/badge/license-AGPL--3.0-lightgrey" alt="License" />
  <img src="https://img.shields.io/badge/status-active-success" alt="Status" />
  <img src="https://img.shields.io/badge/categorization-openai%20%7C%20gemini%20%7C%20local-blueviolet" alt="Categorization Providers" />
  <img src="https://img.shields.io/github/stars/francescogabrieli/Prism?style=social" alt="GitHub stars" />
</p>

<p align="center">
  <img src="assets/Screenshot%202026-03-04%20alle%2016.22.41.png" alt="Spectra Dashboard Overview" width="1000" />
</p>

<p align="center">
  <img src="assets/Screenshot%202026-03-04%20alle%2016.23.24.png" alt="Spectra Transactions Page" width="495" />
  <img src="assets/Screenshot%202026-03-04%20alle%2016.23.42.png" alt="Spectra Budget Page" width="495" />
</p>

<p align="center">
  <img src="assets/Screenshot%202026-03-04%20alle%2016.24.01.png" alt="Spectra Trends Page" width="1000" />
</p>

---

## What is Spectra?

Spectra ingests bank exports (**CSV or PDF**), normalizes messy statement formats, categorizes transactions (via **OpenAI**, **Gemini**, or **fully offline**), and visualizes everything in a **self-hosted local web dashboard**. If you want, it can also sync data to **Google Sheets**.

- **Local-first**: no Open Banking, no bank logins, you stay in control of the export files
- **Web UI**: run it on your machine at `http://localhost:8080`
- **Optional Sheets**: keep everything local, or push to a spreadsheet dashboard if you prefer

---

## Why Spectra exists

Most personal finance tools either require direct access to your bank account or lock your data inside proprietary platforms.

Spectra takes a different approach: it works directly from standard exports (CSV/PDF), keeps the pipeline transparent, and gives you full control over storage and outputs.

---

## Features

### Self-Hosted Local Web Dashboard

A full-featured local dashboard at `http://localhost:8080`:

| Page | What it does |
|---|---|
| **Dashboard** | Income vs Expenses breakdown, monthly charts, category distribution |
| **Transactions** | Searchable/sortable ledger with inline category editing |
| **Upload** | Drag-and-drop CSV/PDF import with preview & confirmation |
| **Budget** | Per-category monthly limits with live 🟢/🟡/🔴 status |
| **Trends** | Month-over-month and year-over-year spending analysis |
| **Settings** | View active configuration, reset local database (Danger Zone) |

### Three categorization modes

Configure via `AI_PROVIDER` in your `.env`:

- **`openai`** — Categorization via OpenAI API. Requires `OPENAI_API_KEY`.
- **`gemini`** — Categorization via Google Gemini API. Requires `GEMINI_API_KEY`.
- **`local`** — 100% offline, no API keys needed. Uses a deterministic cascade:

  1. **User Overrides** — Previously corrected merchant/category mappings
  2. **Merchant Memory** — Exact match against merchants seen before (SQLite)
  3. **Fuzzy Match** — Approximate matching via `rapidfuzz`
  4. **Keyword Rules** — Built-in patterns covering common expense/income types
  5. **ML Classifier (optional)** — Trained on *your* history (enable by installing `scikit-learn`)
  6. **Fallback** — Marks as "Uncategorized" for manual correction (Spectra learns next time)

### Google Sheets sync (optional)

If enabled, Spectra can also push data to a Google Sheet:
- **Dashboard** — Income vs Expenses, category breakdowns, recurring cash flow, budget status
- **Budget** — Monthly limits with live status indicators
- **Transactions YYYY** — Color-coded ledger for each year
- **Trends** — YoY comparisons and multi-year charts

### Pipeline / reliability

- **Universal import** — Auto-detects delimiters, bank layouts, EU number formats (`1.234,56`), multi-line descriptions
- **Multi-currency FX** — Historical ECB rates via [Frankfurter API](https://www.frankfurter.app/) (no API key)
- **Recurring detection** — Pattern matching + historical spacing to flag subscriptions/salary
- **Idempotent** — Transaction hashes in SQLite prevent duplicate imports
- **Automation-ready** — Can run on a schedule (cron / GitHub Actions)

---

## Quick Start

### 1) Install

```bash
git clone https://github.com/francescogabrieli/Spectra.git
cd Spectra
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Configure `.env`

Create a `.env` file in the project root:

```env
BASE_CURRENCY=EUR

# Choose one provider:
AI_PROVIDER=local
# AI_PROVIDER=openai
# AI_PROVIDER=gemini

# Keys (depending on provider)
# OPENAI_API_KEY=...
# GEMINI_API_KEY=...

# Optional: Google Sheets sync
# SPREADSHEET_ID=...
# GOOGLE_SHEETS_CREDENTIALS_FILE=credentials.json
```

### 3) Run (web dashboard)

```bash
python -m spectra --serve
```

Open: **[http://localhost:8080](http://localhost:8080)**

---

## CLI usage (advanced)

```bash
# Process a folder of exports
python -m spectra --inbox inbox/

# Preview categorization without writing outputs
python -m spectra --inbox inbox/ --dry-run

# Process a single file
python -m spectra -f export.csv

# Custom port for the dashboard
python -m spectra --serve --port 3000
```

---

## Getting the keys (optional)

### Google Sheets API (Sheets sync)

To let Spectra write to your Google Sheet, you need a Google Cloud **Service Account**.

1. Go to Google Cloud Console
2. Create a project and enable **Google Sheets API** and **Google Drive API**
3. Create a **Service Account** and download the JSON key
4. Save it as `credentials.json` in the project root
5. Share your Google Sheet with the service account email (Editor)
6. Put the Sheet ID into `SPREADSHEET_ID` in your `.env`

### OpenAI / Gemini API keys

* **OpenAI**: create a key and set `OPENAI_API_KEY`, then `AI_PROVIDER=openai`
* **Gemini**: create a key and set `GEMINI_API_KEY`, then `AI_PROVIDER=gemini`

---

## GitHub Actions (optional)

Spectra includes a workflow for nightly automation.

You'll need these repo secrets:

* `OPENAI_API_KEY` or `GEMINI_API_KEY`
* `SPREADSHEET_ID`
* `GOOGLE_SHEETS_CREDENTIALS_B64` (base64 of `credentials.json`)

---

## Privacy & Security

* **No bank connections**: Spectra never logs into your bank — you export and upload files manually
* **Local-first pipeline**: parsing, normalization, deduplication, and (with `local`) categorization run locally
* **SQLite storage**: local database stored in `data/spectra.db` — you can reset it anytime from the Settings page
* **Cloud providers**: when using `openai`/`gemini`, Spectra sends a minimal payload (date + cleaned description + amount)

---

## License

**GNU Affero General Public License v3.0 (AGPL-3.0)**

If you modify Spectra and run it as a network service, you must make the source code available.

### Commercial licensing

If you need a closed-source or proprietary license, contact: [francesco.gabrieli.fg@gmail.com](mailto:francesco.gabrieli.fg@gmail.com)
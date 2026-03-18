# Agent Guide (Spectra)

## Purpose
This file helps coding agents work safely and quickly in this repository.

## Project Summary
- App: Spectra (local-first personal finance dashboard)
- Stack: Python, FastAPI, SQLite, Jinja templates
- Entry point: `python -m spectra`

## Quick Start
1. `python3 -m venv .venv`
2. `source .venv/bin/activate`
3. `pip install -e .`
4. `cp .env.example .env` (or use minimal local mode config)
5. `python -m spectra --serve`

Open: `http://127.0.0.1:8080`

## Useful Commands
- Run dashboard: `python -m spectra --serve --port 8080`
- Parse inbox: `python -m spectra --inbox inbox/`
- Parse one file: `python -m spectra -f <file.csv>`
- Smoke check import: `python -c "import spectra; print('ok')"`
- Syntax check key modules: `python -m py_compile src/spectra/*.py`

## Core Data Flow
1. Parse file (`csv_parser.py`, `pdf_parser.py`, `ofx_parser.py`)
2. Deduplicate via SQLite IDs
3. Categorize (rules, local memory/fuzzy, deterministic hints, ML)
4. Preview in web UI
5. Confirm and save to `tx_history`
6. Learn mappings for future runs

## Categorization Architecture
- Local categorization lives in `src/spectra/local_categorizer.py`.
- ML seed categories live in `src/spectra/ml_classifier.py`.
- NL-focused merchant seeds live in `src/spectra/merchant_seeds_nl.py`.
- Category rules are stored in DB table `category_rules`.
- Learned merchant mappings are stored in DB table `merchant_categories`.

### Important Rule
Keep backend category values canonical (English keys).
UI translation to Dutch is handled in templates via `window.SpectraCategory` in:
- `src/spectra/web/templates/base.html`

If you add a new canonical category, also add translation there.

## CSV Notes (ING/NL)
Current parser supports headers like:
- `Datum`
- `Naam / Omschrijving`
- `Af Bij`
- `Bedrag (EUR)`

Also supports duplicate disambiguation using row fingerprints to avoid dropping legit same-day same-amount rows.

## Frontend Notes
- Templates are in `src/spectra/web/templates/`.
- API routes are in `src/spectra/web/server.py`.
- Keep UI labels user-friendly Dutch where possible, but do not change canonical category keys in API payloads.

## Testing Expectations
Before pushing meaningful changes:
1. Run relevant targeted checks.
2. Validate `python -m spectra --help` still works.
3. If touching templates/server, open the UI and verify no obvious regressions.

## Privacy and Local Data
- Do not commit `.env`, DB files, or personal finance exports unless explicitly requested.
- This repo may contain local reference files for debugging; treat them as sensitive.

## Git Workflow
- Keep commits focused and descriptive.
- Avoid unrelated file churn.
- Push to the configured fork remote (`origin`) unless instructed otherwise.


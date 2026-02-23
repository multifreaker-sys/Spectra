<p align="center">
  <img src="assets/icon.png" alt="Prism" width="120" />
</p>

<h1 align="center">Prism</h1>
<p align="center">
  <strong>Bank CSV/PDF → AI categorization → Google Sheets</strong><br>
  Your personal finance dashboard, fully automated.
</p>

---

## What is Prism?

Prism takes your raw bank exports (CSV or PDF), sends them through an AI model to categorize every transaction, and writes the results to a formatted Google Sheets spreadsheet — complete with charts and a dashboard.

- **Works with any bank** — auto-detects column names, delimiters, date and number formats
- **AI-powered** — uses OpenAI or Gemini to clean merchant names and assign categories
- **Subscription detection** — flags recurring payments (Netflix, Spotify, etc.) with 🔄
- **Dashboard** — auto-generated charts: spending by category, monthly trends, income vs expenses
- **Idempotent** — never imports the same transaction twice (SHA1 dedup)
- **Scheduled** — GitHub Actions cron runs nightly; if there's nothing new, it exits silently

---

## Quick Start

```bash
git clone https://github.com/YOUR_USERNAME/Prism.git
cd Prism
python -m venv .venv && source .venv/bin/activate
pip install -e .
cp .env.example .env
```

Fill in your `.env`:

```env
AI_PROVIDER=openai
OPENAI_API_KEY=sk-...
SPREADSHEET_ID=1Do7APx...
GOOGLE_SHEETS_CREDENTIALS_FILE=credentials.json
```

Then run:

```bash
# Preview without writing anything
python -m prism --file your_bank_export.csv --dry-run

# Write to Google Sheets
python -m prism --file your_bank_export.csv
```

---

## Google Sheets Setup

1. Create a project on [Google Cloud Console](https://console.cloud.google.com/)
2. Enable **Google Sheets API** and **Google Drive API**
3. Create a **Service Account** → download the JSON key → rename to `credentials.json`
4. Create a Google Sheet → **Share** it with the service account email (as Editor)
5. Copy the spreadsheet ID from the URL → paste in `.env`

---

## AI Provider

Prism supports **OpenAI** and **Google Gemini**.

| Provider | Model | Config |
|----------|-------|--------|
| OpenAI | `gpt-5-nano-2025-08-07` (or any) | `AI_PROVIDER=openai` + `OPENAI_API_KEY` |
| Gemini | `gemma-3-27b-it` (free tier) | `AI_PROVIDER=gemini` + `GEMINI_API_KEY` |

Get a free Gemini key at [aistudio.google.com](https://aistudio.google.com/apikey).

---

## Usage

### Single file

```bash
python -m prism --file bank_export.csv
python -m prism --file statement.pdf
```

### Inbox mode (batch)

Drop one or more CSV/PDF files into `inbox/`, then:

```bash
python -m prism --inbox inbox/
```

Processed files are automatically moved to `processed/`.

### Options

| Flag | Description |
|------|-------------|
| `--file`, `-f` | Path to a single CSV or PDF |
| `--inbox` | Path to folder (processes all CSV/PDF files) |
| `--currency` | Currency code, default: `EUR` |
| `--dry-run` | Preview results in terminal, don't write to Sheets |

---

## GitHub Actions (Automation)

Prism includes a workflow that runs every night at 22:00 CET. If there are CSV/PDF files in `inbox/`, it processes them automatically.

### Setup

1. Push this repo to GitHub
2. Go to **Settings → Secrets and variables → Actions**
3. Add these secrets:

| Secret | Value |
|--------|-------|
| `OPENAI_API_KEY` | Your OpenAI key |
| `SPREADSHEET_ID` | Your Google Sheet ID |
| `GOOGLE_SHEETS_CREDENTIALS_B64` | Base64 of your `credentials.json` |

Generate the base64 secret:

```bash
base64 -i credentials.json | pbcopy   # macOS — copies to clipboard
```

The workflow also triggers on `push` when files land in `inbox/`, so you can just commit a CSV and it runs automatically.

---

## Supported Formats

### CSV

| Bank | Delimiter | Tested |
|------|-----------|--------|
| ISyBank / Intesa Sanpaolo | `;` | ✅ |
| UniCredit | `;` | ✅ |
| N26 | `,` | ✅ |
| Revolut | `,` | ✅ |
| Any other bank | Auto-detect | ✅ |

Prism auto-detects: delimiter, column names (Italian/English/German), date format, number format (Italian `1.234,56` or English `1,234.56`), and handles metadata rows before the actual header.

### PDF

Extracts tables from PDF bank statements using `pdfplumber`. Falls back to regex line-matching for text-based PDFs.

---

## Project Structure

```
Prism/
├── src/prism/
│   ├── __main__.py        # CLI entry point
│   ├── config.py          # Environment settings (Pydantic)
│   ├── csv_parser.py      # Universal CSV parser
│   ├── pdf_parser.py      # PDF statement parser
│   ├── ai.py              # LLM categorizer (OpenAI / Gemini)
│   ├── sheets.py          # Google Sheets writer + formatting
│   ├── dashboard.py       # Dashboard tab with charts
│   └── db.py              # SQLite dedup tracker
├── tests/                 # Unit + integration tests
├── inbox/                 # Drop CSV/PDF files here
├── processed/             # Files moved here after processing
├── .github/workflows/     # GitHub Actions cron job
├── .env.example           # Template for environment variables
└── pyproject.toml
```

---

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AI_PROVIDER` | No | `gemini` | `gemini` or `openai` |
| `OPENAI_API_KEY` | Yes* | — | OpenAI API key |
| `GEMINI_API_KEY` | Yes* | — | Gemini API key |
| `SPREADSHEET_ID` | Yes | — | Google Sheet ID |
| `GOOGLE_SHEETS_CREDENTIALS_FILE` | Yes† | `credentials.json` | Path to service account JSON |
| `GOOGLE_SHEETS_CREDENTIALS_B64` | Yes† | — | Base64-encoded JSON (for CI) |
| `DB_PATH` | No | `data/prism.db` | SQLite database path |
| `LOG_LEVEL` | No | `INFO` | Logging level |

\* One of the two AI keys is required, depending on `AI_PROVIDER`.
† One of the two credential methods is required.

---

## Development

```bash
pip install -e ".[dev]"
python -m pytest tests/ -v
```

---

## Privacy

- Your bank data **never leaves your machine** except for transaction descriptions sent to the AI for categorization
- No third-party banking APIs — you provide the export file
- The local SQLite database only stores SHA1 hashes of seen transactions
- Credentials stay in your `.env` (never committed to git)

---

## License

MIT
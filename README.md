<p align="center">
  <img src="assets/icon.png" alt="Prism" width="180" />
</p>

<h1 align="center">Prism</h1>
<p align="center">
  <strong>Bank CSV/PDF → AI categorization → Google Sheets</strong><br>
  Your personal finance dashboard, fully automated.
</p>

---

## What is Prism?

Prism takes your raw bank exports (CSV or PDF), sends them through an AI model (OpenAI or Gemini) to categorize every transaction, and builds a comprehensive, multi-tab financial dashboard on Google Sheets.

### Core Features

- **Universal Import** — Auto-detects custom delimiters, edge-case bank layouts, Italian/European number formats (`1.234,56`), and multi-line descriptions from CSVs and PDFs. Drop a file in the `inbox/` folder and Prism handles the rest.
- **AI Categorization** — Cleans obscure bank transfer descriptions into readable merchant names and accurately categorizes them using LLMs, distinguishing between Expenses (Shopping, Food, Transport) and Income (Salary, Transfers In).
- **Multi-Currency (FX Rates)** — If your bank export contains foreign currencies (USD, GBP, etc.), Prism automatically calls the free [Frankfurter API](https://www.frankfurter.app/) to fetch the exact historical ECB exchange rate for that day, converting everything to EUR to keep your budgets and trends perfectly aligned. (Requires **zero API keys**).
- **Deterministic Subscriptions** — Uses pattern-matching algorithms to reliably tag recurring subscriptions (Netflix, Amazon, Gym) and recurring income without relying on LLM guesses.
- **Idempotent** — Maintains a local SQLite database of transaction hashes. Prism never imports the same transaction twice, even if you re-run the same CSV.
- **Fully Automated (Cron)** — Run it nightly via GitHub Actions. If you add a CSV to the `inbox`, the bot picks it up, updates your Google Sheet, and moves the file to `processed/`.

### The Google Sheets Dashboard

Prism automatically creates and formats multiple tabs:
1. **Transactions YYYY**: A detailed, color-coded ledger for each year.
2. **Dashboard**: High-level view for the current year. Shows Income vs Expenses, Spending donuts by category, monthly breakdowns, and recurring cash flow.
3. **Budget**: A dedicated tab where you define your monthly limits. Prism checks these limits against your spending and displays a Live 🟢/🟡/🔴 Budget Status on the Dashboard.
4. **Trends**: A Year-over-Year (YoY) comparison tab. Tracks your Net Cash Flow, Savings Rate %, and generates comparative multi-year line charts to visualize your trajectory.

---

## 🚀 Quick Start (Local)

1. **Clone the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/Prism.git
   cd Prism
   ```

2. **Setup virtual environment**
   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -e .
   cp .env.example .env
   ```

3. **Configure the `.env` file** (See sections below for getting these keys)
   ```env
   AI_PROVIDER=openai
   OPENAI_API_KEY=sk-...
   SPREADSHEET_ID=1Do7APx...
   GOOGLE_SHEETS_CREDENTIALS_FILE=credentials.json
   ```

4. **Run Prism**
   Place your bank exported files `.csv` or `.pdf` in the `inbox/` directory.
   ```bash
   # Preview without writing to Google Sheets
   python -m prism --inbox inbox/ --dry-run
   
   # Run the full pipeline (Clean, Categorise, Write to Sheets, Update Dashboards)
   python -m prism --inbox inbox/
   ```

---

## 🔑 Getting the Keys

### 1. Google Sheets API (`SPREADSHEET_ID` & `credentials.json`)

To allow Prism to write to your Google Sheet, you need a Service Account.

1. Go to the [Google Cloud Console](https://console.cloud.google.com/).
2. Create a new Project (e.g., "Prism Finance").
3. Navigate to **APIs & Services > Library** and enable **Google Sheets API** and **Google Drive API**.
4. Navigate to **APIs & Services > Credentials**.
5. Click **Create Credentials > Service Account**. Name it (e.g., "prism-bot").
6. Click on the newly created Service Account, go to the **Keys** tab, click **Add Key > Create new key**, choose **JSON**, and download it.
7. Rename the downloaded file to `credentials.json` and place it in the root of the Prism folder.
8. **CRITICAL STEP**: Open the `credentials.json` file and copy the `client_email` address.
9. Create a new blank Google Sheet. Click the **Share** button in the top right, paste the `client_email`, and give it **Editor** permissions.
10. Copy the Spreadsheet ID from the URL of your new Google Sheet:  
    `https://docs.google.com/spreadsheets/d/`**`<SPREADSHEET_ID>`**`/edit`
11. Add this ID to the `SPREADSHEET_ID` variable in your `.env`.

### 2. AI Provider (`OPENAI_API_KEY` or `GEMINI_API_KEY`)

Prism supports **OpenAI** and **Google Gemini**.

| Provider | Config | Notes |
|----------|--------|-------|
| OpenAI | `AI_PROVIDER=openai` + `OPENAI_API_KEY` | Best categorization accuracy. Create an API key at [platform.openai.com](https://platform.openai.com). |
| Gemini | `AI_PROVIDER=gemini` + `GEMINI_API_KEY` | Free tier available. Get a key at [aistudio.google.com](https://aistudio.google.com/apikey). |

---

## 🤖 GitHub Actions (Cloud Automation)

Prism includes a workflow (`.github/workflows/prism.yml`) that runs every night at 22:00 CET or manually via dispatch. Drop CSVs into the `inbox/` folder, commit to GitHub, and let the Action process them while you sleep.

### Setup GitHub Secrets

Go to your GitHub Repository **Settings → Secrets and variables → Actions**. Add the following **Repository Secrets**:

1. **`OPENAI_API_KEY`**: Your OpenAI key.
2. **`SPREADSHEET_ID`**: Your Google Sheet ID.
3. **`GOOGLE_SHEETS_CREDENTIALS_B64`**: The base64-encoded version of your `credentials.json`.
   
   To generate this string:
   * **macOS/Linux**: `base64 -i credentials.json | pbcopy` (copies to clipboard)
   * **Windows (PowerShell)**: `[Convert]::ToBase64String([IO.File]::ReadAllBytes("credentials.json")) | clip`

When the action runs, it processes the `inbox/` folder. Processed files are moved to `processed/` and the changes are automatically committed back to the repository.

---

## 💻 CLI Usage

| Command | Description |
|---------|-------------|
| `python -m prism --inbox inbox/` | **Recommended**: Processes all `.csv` and `.pdf` files inside the `inbox/` directory. Automatically moves successfully imported files to `processed/`. |
| `python -m prism -f export.csv` | Process a single specific file. Does not move the file afterwards. |
| `--dry-run` | Appended to any command. Runs the parsing and AI categorization, prints the results to the terminal, but **skips** writing to Google Sheets and **skips** database saving. Great for testing. |
| `--currency USD` | Override the default `EUR` formatting. |

---

## 🔒 Privacy & Architecture

- **Local Processing First**: The heavy lifting (CSV parsing, description cleaning, deduplication) happens locally or in your private GitHub Action runner.
- **AI Payload**: Only the transaction date, cleaned description, and amount are sent to the AI Provider for categorization.
- **No Third-Party Plaid/OpenBanking**: Prism does not connect directly to your bank account. You remain in complete control of your data exports.
- **Database**: The local `data/prism.db` SQLite database stores **only cryptographic SHA1 hashes** of your transactions to prevent duplicates.

## 📄 License

This project is licensed under the **Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0)**. 
- You are free to use, share, and adapt the code.
- You **may not** use the material for commercial purposes or resale.
See the `LICENSE.md` file for details.
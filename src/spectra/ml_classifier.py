"""ML classifier for local mode — TF-IDF + Logistic Regression, bootstrapped with seed data.

The classifier is always active: it starts with built-in seed examples that encode
domain knowledge (common merchants and their categories), and improves as user
corrections and transaction history accumulate.  User data is weighted higher than
seed data so the model progressively personalises.
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any

import numpy as np

from spectra.merchant_seeds_nl import SEED_MERCHANTS_NL

logger = logging.getLogger("spectra.ml")

# ── Seed knowledge ──────────────────────────────────────────────
# Each tuple is (description_example, category).  These bootstrap the model
# so it works from day-0 without any user history.

_SEED_MERCHANTS_BASE: list[tuple[list[str], str]] = [
    # ── Digital Subscriptions ──────────────────────────────────
    (["Netflix", "NETFLIX.COM", "ADDEBITO SDD NETFLIX.COM", "Netflix subscription"], "Digital Subscriptions"),
    (["Spotify", "SPOTIFY AB", "ADDEBITO SDD SPOTIFY AB", "Spotify Premium"], "Digital Subscriptions"),
    (["Apple", "APPLE.COM/BILL", "Apple Music", "Apple One", "iTunes"], "Digital Subscriptions"),
    (["Disney+", "DISNEYPLUS", "Disney Plus"], "Digital Subscriptions"),
    (["Amazon Prime", "AMAZON PRIME", "AMZN PRIME"], "Digital Subscriptions"),
    (["YouTube Premium", "YOUTUBE PREMIUM"], "Digital Subscriptions"),
    (["ChatGPT", "OPENAI", "OpenAI subscription"], "Digital Subscriptions"),
    (["GitHub", "GITHUB.COM"], "Digital Subscriptions"),
    (["Dropbox", "DROPBOX.COM"], "Digital Subscriptions"),
    (["Google One", "GOOGLE STORAGE", "GOOGLE WORKSPACE", "GOOGLE CLOUD"], "Digital Subscriptions"),
    (["iCloud", "ICLOUD STORAGE"], "Digital Subscriptions"),
    (["Adobe", "ADOBE CREATIVE CLOUD", "ADOBE.COM"], "Digital Subscriptions"),
    (["Microsoft 365", "OFFICE 365"], "Digital Subscriptions"),
    (["Notion", "NOTION.SO"], "Digital Subscriptions"),
    (["Slack", "SLACK TECHNOLOGIES"], "Digital Subscriptions"),
    (["Zoom", "ZOOM.US"], "Digital Subscriptions"),
    (["LinkedIn Premium", "LINKEDIN PREMIUM"], "Digital Subscriptions"),
    (["DAZN", "DAZN SUBSCRIPTION"], "Digital Subscriptions"),
    (["Paramount+", "PARAMOUNT PLUS"], "Digital Subscriptions"),
    (["Sky", "SKY ITALIA", "SKY TV"], "Digital Subscriptions"),
    (["NordVPN", "NORDVPN.COM", "ExpressVPN", "ProtonVPN", "Surfshark"], "Digital Subscriptions"),
    (["AWS", "AMAZON WEB SERVICES", "Heroku", "DigitalOcean", "Vercel", "Netlify", "Cloudflare"], "Digital Subscriptions"),
    (["1Password", "Bitwarden", "LastPass"], "Digital Subscriptions"),
    (["Twitch", "TWITCH.TV"], "Digital Subscriptions"),
    (["Claude AI", "ANTHROPIC"], "Digital Subscriptions"),
    (["Midjourney", "MIDJOURNEY.COM"], "Digital Subscriptions"),
    (["Setapp", "SETAPP.COM"], "Digital Subscriptions"),
    (["Apple TV+", "TV.APPLE.COM"], "Digital Subscriptions"),
    (["Porkbun", "Namecheap", "GoDaddy", "Hover.com", "Gandi.net", "Registro.it"], "Digital Subscriptions"),

    # ── Transport ─────────────────────────────────────────────
    (["Uber", "UBER TRIP", "UBER BV", "HELP.UBER.COM"], "Transport"),
    (["Bolt", "BOLT.EU", "BOLT RIDE"], "Transport"),
    (["Lyft", "LYFT RIDE"], "Transport"),
    (["Trenitalia", "TRENITALIA SPA"], "Transport"),
    (["Italo Treno", "ITALO NTV"], "Transport"),
    (["FlixBus", "FLIXBUS.IT"], "Transport"),
    (["ATM Milano", "TPER", "GTT TORINO"], "Transport"),
    (["TfL Travel Charge", "TFL TRAVEL CHARGE LONDON", "MTA", "RATP", "SNCF", "RENFE", "SBB"], "Transport"),
    (["ENI STATION", "Q8", "AGIP", "IP STATION", "Shell", "BP", "TotalEnergies"], "Transport"),
    (["Autostrade", "TELEPASS", "VIACARD"], "Transport"),
    (["Taxi", "RADIOTAXI", "TAXIFY"], "Transport"),
    (["Lime scooter", "Bird scooter", "Tier scooter", "FreeNow"], "Transport"),

    # ── Travel ────────────────────────────────────────────────
    (["Ryanair", "RYANAIR LTD", "RYANAIR FR"], "Travel"),
    (["EasyJet", "EASYJET PLC"], "Travel"),
    (["Vueling", "WizzAir", "Lufthansa", "ITA Airways", "Alitalia"], "Travel"),
    (["Turkish Airlines", "KLM", "Air France", "British Airways", "Iberia", "TAP"], "Travel"),
    (["Booking.com", "BOOKING.COM AMSTERDAM", "BOOKING COM"], "Travel"),
    (["Airbnb", "AIRBNB.COM"], "Travel"),
    (["Expedia", "Hotels.com", "Trivago", "LastMinute"], "Travel"),
    (["Hotel", "Albergo", "B&B", "Bed and Breakfast", "Hostel"], "Travel"),
    (["Resort", "Motel", "Lodge", "Camping"], "Travel"),
    (["Hertz", "Avis", "Europcar", "Sixt", "Enterprise Rent", "Maggiore"], "Travel"),
    (["Aeroporto", "Airport"], "Travel"),
    (["Tirrenia", "Grimaldi Lines", "GNV", "Ferry"], "Travel"),
    (["Frecciarossa", "Frecciargento", "Frecciabianca"], "Travel"),

    # ── Entertainment ─────────────────────────────────────────
    (["Cinema", "UCI Cinema", "The Space Cinema"], "Entertainment"),
    (["Stadio", "Stadium", "Biglietti"], "Entertainment"),
    (["Concerto", "Concert", "Teatro", "Opera", "Museo"], "Entertainment"),
    (["TicketOne", "Ticketmaster", "Vivaticket", "Eventbrite"], "Entertainment"),
    (["Steam", "PlayStation", "Xbox", "Nintendo", "Epic Games", "PSN"], "Entertainment"),
    (["Gardaland", "Mirabilandia", "Disneyland", "Parco divertimenti"], "Entertainment"),

    # ── Groceries ─────────────────────────────────────────────
    (["Esselunga", "ESSELUNGA SPA", "POS ESSELUNGA"], "Groceries"),
    (["Coop", "COOP ITALIA"], "Groceries"),
    (["Conad", "CONAD SUPERMERCATO"], "Groceries"),
    (["Lidl", "LIDL ITALIA", "POS LIDL"], "Groceries"),
    (["Aldi", "ALDI SUD"], "Groceries"),
    (["Eurospin", "EUROSPIN SPA"], "Groceries"),
    (["PAM", "PAM SUPERMERCATO"], "Groceries"),
    (["Penny Market", "PENNY MARKET"], "Groceries"),
    (["Tesco", "Sainsbury", "ASDA", "Waitrose", "Morrisons", "Marks Spencer"], "Groceries"),
    (["Rewe", "Edeka", "Kaufland", "Netto", "Migros", "Denner"], "Groceries"),
    (["Auchan", "Leclerc", "Intermarche", "Monoprix", "Carrefour"], "Groceries"),
    (["Mercadona", "Pingo Doce", "Continente"], "Groceries"),
    (["Walmart", "Target", "Kroger", "Whole Foods", "Trader Joe", "7-Eleven"], "Groceries"),
    (["Supermercato", "Supermarket", "Grocery", "Alimentari"], "Groceries"),

    # ── Food & Dining ─────────────────────────────────────────
    (["Uber Eats", "UBER EATS DELIVERY", "UBEREATS"], "Food & Dining"),
    (["Deliveroo", "DELIVEROO.COM"], "Food & Dining"),
    (["Glovo", "GLOVO APP"], "Food & Dining"),
    (["Just Eat", "JUST EAT", "JUSTEAT"], "Food & Dining"),
    (["DoorDash", "Grubhub", "Postmates"], "Food & Dining"),
    (["Starbucks", "STARBUCKS COFFEE", "POS STARBUCKS"], "Food & Dining"),
    (["McDonalds", "MC DONALDS", "MCDONALDS"], "Food & Dining"),
    (["Burger King", "BURGER KING"], "Food & Dining"),
    (["KFC", "KENTUCKY FRIED CHICKEN"], "Food & Dining"),
    (["Subway", "SUBWAY SANDWICH"], "Food & Dining"),
    (["Ristorante", "Trattoria", "Pizzeria", "Osteria", "Enoteca", "Restaurant"], "Food & Dining"),
    (["Bar Caffè", "Caffè Roma", "Cafeteria", "Pasticceria", "Costa Coffee", "Dunkin"], "Food & Dining"),
    (["Sushi", "Sushiko", "Ramen", "Udon"], "Food & Dining"),
    (["Poke bowl", "Pokè house"], "Food & Dining"),
    (["Bakery", "Panetteria", "Forno", "Boulangerie", "Pret a Manger"], "Food & Dining"),
    (["Gelateria", "Gelato shop"], "Food & Dining"),
    (["Domino", "Papa Johns", "Pizza Hut"], "Food & Dining"),
    (["Autogrill", "AUTOGRILL SPA"], "Food & Dining"),
    (["Wolt", "WOLT.COM"], "Food & Dining"),

    # ── Shopping ──────────────────────────────────────────────
    (["Amazon", "AMAZON EU SARL", "AMZN MKTP", "AMAZON.IT", "AMAZON MARKETPLACE"], "Shopping"),
    (["AliExpress", "Temu", "Shein", "ASOS", "Zalando"], "Shopping"),
    (["IKEA", "IKEA ITALIA RETAIL", "POS IKEA"], "Shopping"),
    (["Zara", "H&M", "Uniqlo", "Decathlon", "Primark", "Muji"], "Shopping"),
    (["MediaWorld", "Unieuro", "Euronics", "Best Buy"], "Shopping"),
    (["Sephora", "Kiko", "MAC Cosmetics"], "Shopping"),
    (["Etsy", "ETSY.COM"], "Shopping"),
    (["Vinted", "VINTED.COM"], "Shopping"),
    (["eBay", "EBAY.COM"], "Shopping"),
    (["Leroy Merlin", "OBI", "Brico", "Home Depot"], "Shopping"),
    (["PayPal purchase", "PAYPAL PAYMENT"], "Shopping"),

    # ── Health ────────────────────────────────────────────────
    (["Farmacia", "Pharmacy", "Pharmacie", "Apotheke", "CVS", "Walgreens"], "Health"),
    (["Rossmann", "DM Drogerie"], "Health"),
    (["Dottore", "Medico", "Clinica", "Ospedale", "Hospital"], "Health"),
    (["Dentista", "Odontoiatra", "Dental clinic"], "Health"),
    (["Psicologo", "Psicologa", "Terapista", "Fisioterapista"], "Health"),
    (["Ottica", "Visita oculistica", "Optician"], "Health"),

    # ── Health & Fitness ──────────────────────────────────────
    (["Palestra", "Gym", "Fitness club", "Wellness center", "CrossFit", "Pilates", "Yoga"], "Health & Fitness"),

    # ── Insurance ─────────────────────────────────────────────
    (["Assicurazione", "Insurance", "AXA", "Allianz", "Generali", "Zurich", "UnipolSai"], "Insurance"),
    (["RC Auto", "Polizza auto", "Premio assicurativo", "Direct Line", "BUPA"], "Insurance"),

    # ── Utilities ─────────────────────────────────────────────
    (["Vodafone", "VODAFONE ITALIA"], "Utilities"),
    (["TIM", "TIM TELECOM"], "Utilities"),
    (["Wind Tre", "WINDTRE SPA"], "Utilities"),
    (["Iliad", "ILIAD ITALIA"], "Utilities"),
    (["Fastweb", "FASTWEB SPA"], "Utilities"),
    (["Enel", "ENEL ENERGIA"], "Utilities"),
    (["A2A", "IREN", "HERA", "ACEA"], "Utilities"),
    (["Bolletta", "Utenza", "Gas luce", "Electricity", "Water bill"], "Utilities"),
    (["Affitto", "Canone locazione", "Rent", "Noleggio appartamento"], "Utilities"),
    (["Condominio", "Spese condominiali"], "Utilities"),

    # ── Cash ──────────────────────────────────────────────────
    (["Versamento contanti", "Deposito contanti", "Cash deposit"], "Cash Deposit"),
    (["Prelievo", "Prelievo Bancomat", "ATM Cash", "Cash withdrawal", "Prelievo con carta"], "Cash Withdrawal"),

    # ── Taxes ─────────────────────────────────────────────────
    (["F24", "Agenzia Entrate", "Tasse", "Tributi", "IMU", "TARI", "Tax"], "Taxes"),
    (["Comune di", "Regione", "Provincia di", "ASL", "Council"], "Taxes"),
    (["Bollo auto", "PRA", "DVLA"], "Taxes"),

    # ── Education ─────────────────────────────────────────────
    (["Università", "Politecnico", "Accademia", "Corso di", "College", "School"], "Education"),
    (["Udemy", "Coursera", "Skillshare", "Duolingo", "Busuu"], "Education"),
    (["Libreria Feltrinelli", "Mondadori", "Libri", "Waterstones", "Barnes Noble"], "Education"),

    # ── Income & Transfers ────────────────────────────────────
    (["Stipendio", "STIPENDIO MESE", "Salary", "Payroll", "Retribuzione",
      "Gehalt", "Lohn", "Gehaltseingang",           # German
      "Salaire", "Virement salaire",                # French
      "Nómina", "Salario",                          # Spanish
      "Salário", "Ordenado"], "Salary"),             # Portuguese
    (["Pensione", "Pension", "Rente", "Retraite", "Jubilación", "Pensão"], "Pension"),
    (["Bonifico ricevuto", "Accredito bonifico", "Bonifico in entrata",
      "Incoming transfer", "Überweisung eingegangen",
      "Virement reçu", "Transferencia recibida"], "Transfer In"),
    (["Rimborso", "Refund", "Cashback",
      "Remboursement", "Reembolso", "Erstattung"], "Reimbursement"),
    (["Revolut top-up", "REVOLUT TOP UP"], "Transfer"),
]

# Global + locale-specific merchants
_SEED_MERCHANTS: list[tuple[list[str], str]] = _SEED_MERCHANTS_BASE + SEED_MERCHANTS_NL

# Banking prefixes used to augment seed data with realistic raw descriptions.
_BANKING_PREFIXES = [
    # Italian
    "",
    "POS ",
    "POS 1234 ",
    "ADDEBITO SDD ",
    "ADDEBITO DIRETTO ",
    "PAGAMENTO ",
    "PAGAMENTO SU POS ",
    "PAGAMENTO SU POS ESTERO ",
    # English / UK
    "CARD PAYMENT ",
    "CARD PAYMENT TO ",
    "DIRECT DEBIT ",
    "CONTACTLESS ",
    # German
    "Kartenzahlung ",
    "Lastschrift ",
    # French
    "Paiement CB ",
    "Prélèvement SEPA ",
    # Spanish
    "Pago con tarjeta ",
]


def _build_seed_data() -> list[tuple[str, str]]:
    """Expand _SEED_MERCHANTS into a flat list of (description, category) pairs."""
    data: list[tuple[str, str]] = []
    for examples, category in _SEED_MERCHANTS:
        for example in examples:
            data.append((example, category))
            # Add prefixed variants for the first/main example of each group
            if example == examples[0]:
                for prefix in _BANKING_PREFIXES:
                    if prefix:
                        data.append((f"{prefix}{example}", category))
                        data.append((f"{prefix}{example.upper()}", category))
    return data


# Pre-computed at import time
_SEED_DATA: list[tuple[str, str]] = _build_seed_data()


def default_categories() -> list[str]:
    """Return the default category catalog derived from seed merchants."""
    return sorted({category for _examples, category in _SEED_MERCHANTS})

# Weight multiplier for user data relative to seed data
_USER_WEIGHT = 10.0
_SEED_WEIGHT = 1.0


def train_classifier(
    training_data: list[tuple[str, str]] | None = None,
) -> Any | None:
    """Train a TF-IDF + LogisticRegression classifier.

    The model is always bootstrapped with built-in seed examples so it works
    from the very first run.  When *training_data* (from user history /
    corrections) is provided, those samples receive a higher weight so the
    model progressively personalises.

    Returns
    -------
    A fitted sklearn Pipeline, or None if scikit-learn is not installed.
    """
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import Pipeline
    except ImportError:
        logger.info("scikit-learn not installed — ML classifier disabled. Install with: pip install scikit-learn")
        return None

    # Combine seed + user data
    descriptions: list[str] = []
    categories: list[str] = []
    weights: list[float] = []

    for desc, cat in _SEED_DATA:
        descriptions.append(desc)
        categories.append(cat)
        weights.append(_SEED_WEIGHT)

    if training_data:
        for desc, cat in training_data:
            if desc and cat and cat != "Uncategorized":
                descriptions.append(desc)
                categories.append(cat)
                weights.append(_USER_WEIGHT)

    unique_cats = set(categories)
    if len(unique_cats) < 2:
        logger.info("Only 1 category in combined data — ML classifier not useful")
        return None

    pipeline = Pipeline([
        ("tfidf", TfidfVectorizer(
            max_features=5000,
            ngram_range=(1, 2),
            sublinear_tf=True,
            strip_accents="unicode",
            lowercase=True,
        )),
        ("clf", LogisticRegression(
            max_iter=1000,
            C=1.0,
            class_weight="balanced",
        )),
    ])

    pipeline.fit(descriptions, categories, clf__sample_weight=np.array(weights))

    n_user = sum(1 for w in weights if w == _USER_WEIGHT)
    n_seed = sum(1 for w in weights if w == _SEED_WEIGHT)
    logger.info(
        "ML classifier trained: %d seed + %d user samples, %d categories",
        n_seed, n_user, len(unique_cats),
    )
    return pipeline


def predict(classifier: Any, description: str) -> tuple[str, float]:
    """Predict category for a raw banking description.

    Returns
    -------
    (category, confidence) — confidence is the max class probability.
    """
    proba = classifier.predict_proba([description])[0]
    max_idx = proba.argmax()
    confidence = float(proba[max_idx])
    category = classifier.classes_[max_idx]
    return category, confidence
